package workerpool

import (
	"context"
	"errors"
	"sync"
)

var (
	// Errors.
	ErrAlreadyStarted    = errors.New("worker pool already started")
	ErrInvalidNumWorkers = errors.New("number of workers must be positive")
	ErrNilInputChannel   = errors.New("input channel must not be nil")
	ErrNilOutputChannel  = errors.New("output channel must not be nil")
	ErrNilWorkerFunc     = errors.New("worker function must not be nil")
	ErrNotStarted        = errors.New("worker pool not started")
)

type WorkerFunc func(interface{}) (interface{}, error)

type WorkerError struct {
	Item  interface{}
	Error error
}

type WorkerPool struct {
	workerFunc WorkerFunc

	numWorkers int

	wg sync.WaitGroup

	m             sync.Mutex
	inputChannel  <-chan interface{}
	outputChannel chan interface{}
	waitChannel   chan struct{}
	started       bool
}

func NewWorkerPool(workerFunc WorkerFunc, numWorkers int) (*WorkerPool, error) {
	if workerFunc == nil {
		return nil, ErrNilWorkerFunc
	}

	if numWorkers < 1 {
		return nil, ErrInvalidNumWorkers
	}

	return &WorkerPool{
		workerFunc,
		numWorkers,
		sync.WaitGroup{},
		sync.Mutex{},
		nil,
		nil,
		nil,
		false,
	}, nil
}

func (wp *WorkerPool) GetOutputChannel() <-chan interface{} {
	wp.m.Lock()
	defer wp.m.Unlock()

	if wp.outputChannel == nil {
		wp.outputChannel = make(chan interface{})
	}

	return wp.outputChannel
}

func (wp *WorkerPool) SetInputChannel(inputChannel <-chan interface{}) error {
	wp.m.Lock()
	defer wp.m.Unlock()

	if inputChannel == nil {
		return ErrNilInputChannel
	}

	wp.inputChannel = inputChannel

	return nil
}
func (wp *WorkerPool) Start(ctx context.Context) error {
	wp.m.Lock()
	defer wp.m.Unlock()

	if wp.started {
		return ErrAlreadyStarted
	}

	if wp.outputChannel == nil {
		return ErrNilOutputChannel
	}

	if wp.inputChannel == nil {
		return ErrNilInputChannel
	}

	wp.waitChannel = make(chan struct{})

	go wp.startWorkerLoops(ctx)

	wp.started = true

	return nil
}

func (wp *WorkerPool) Wait() error {
	wp.m.Lock()

	if !wp.started {
		wp.m.Unlock()

		return ErrNotStarted
	}

	wp.m.Unlock()

	<-wp.waitChannel

	return nil
}

func (wp *WorkerPool) startWorkerLoops(ctx context.Context) {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(ctx)
	}

	wp.wg.Wait()

	wp.m.Lock()

	close(wp.outputChannel)
	wp.outputChannel = nil

	close(wp.waitChannel)
	wp.waitChannel = nil

	wp.started = false

	wp.m.Unlock()
}

func (wp *WorkerPool) workerLoop(ctx context.Context) {
WORKERLOOP:
	for {
		select {
		case inputItem, ok := <-wp.inputChannel:
			if ok {
				outputItem, err := wp.workerFunc(inputItem)
				if err != nil {
					// WorkerFunc returned error. Send
					// WorkerError to outputChannel.
					wp.outputChannel <- WorkerError{
						inputItem,
						err,
					}

					continue
				}

				if outputItem != nil {
					wp.outputChannel <- outputItem
				}
			} else {
				// Input channel was closed. Exit.
				break WORKERLOOP
			}
		case <-ctx.Done():
			// Context is done. Exit.
			break WORKERLOOP
		}
	}

	wp.wg.Done()
}
