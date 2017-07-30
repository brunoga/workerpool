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

// WorkerFunc is the function type that is used by each worker to process items.
// It receives the item to be processed (as an interface{}) and the context the
// worke rpool was started with (to allow passing extra data to the worker
// function if needed. WorkerFunc implementations should return the result of
// processing te input item (also as an in terface{}) and a nil error on success
// and a nil result and non-nil error on failure.
type WorkerFunc func(interface{}, context.Context) (interface{}, error)

// WorkerError is the error sent through te output channel when there is an
// error processinmg an item. It includes the input Item that had an error
// during processing and the error returned from the WorkerFunc.
type WorkerError struct {
	Item  interface{}
	Error error
}

// WorkerPool implements a worker pool for executing the same task over many
// items in parallel.
type WorkerPool struct {
	workerFunc WorkerFunc

	numWorkers int

	wg sync.WaitGroup

	m             sync.Mutex
	inputChannel  <-chan interface{}
	outputChannel chan interface{}
	started       bool

	mw          sync.Mutex
	waitChannel chan struct{}
	waitError   error
}

// New returns a new WorkerPool instance taht will use the given workerFunc to
// process input items and will have numWorkers workers.
func New(workerFunc WorkerFunc, numWorkers int) (*WorkerPool, error) {
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
		false,
		sync.Mutex{},
		nil,
		nil,
	}, nil
}

// GetOutputChannel returns the channel where the result of processing input
// items will be sent to. This must be called at least once before Start().
func (wp *WorkerPool) GetOutputChannel() <-chan interface{} {
	wp.m.Lock()
	defer wp.m.Unlock()

	if wp.outputChannel == nil {
		wp.outputChannel = make(chan interface{})
	}

	return wp.outputChannel
}

// SetInputChannel sets the channel where workers will read items from. This
// must be called before Start().
func (wp *WorkerPool) SetInputChannel(inputChannel <-chan interface{}) error {
	wp.m.Lock()
	defer wp.m.Unlock()

	if inputChannel == nil {
		return ErrNilInputChannel
	}

	wp.inputChannel = inputChannel

	return nil
}

// Start starts workers in the worker pool with the given context. The context
// can be used to stop workers with an explicit cancelation or with a timeout.
// The context can also be used to pass required data to the workers.
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

	wp.mw.Lock()
	wp.waitChannel = make(chan struct{})
	wp.mw.Unlock()

	go wp.startWorkerLoops(ctx)

	wp.started = true

	return nil
}

// Wait blocks until all workers complete their jobs. It returns an error
// indicating the reason workers finished (clean termination, deadline exceeded
// or cancelation).
func (wp *WorkerPool) Wait() error {
	wp.m.Lock()

	if !wp.started {
		wp.m.Unlock()

		return ErrNotStarted
	}

	wp.m.Unlock()

	wp.mw.Lock()

	<-wp.waitChannel

	err := wp.waitError
	wp.waitError = nil

	wp.mw.Unlock()

	return err
}

func (wp *WorkerPool) startWorkerLoops(ctx context.Context) {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(ctx)
	}

	wp.wg.Wait()

	wp.m.Lock()

	close(wp.outputChannel)
	close(wp.waitChannel)

	wp.started = false
	wp.outputChannel = nil

	wp.m.Unlock()

	wp.mw.Lock()

	wp.waitChannel = nil

	wp.mw.Unlock()
}

func (wp *WorkerPool) workerLoop(ctx context.Context) {
WORKERLOOP:
	for {
		select {
		case inputItem, ok := <-wp.inputChannel:
			if ok {
				outputItem, err := wp.workerFunc(inputItem, ctx)
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
			wp.waitError = ctx.Err()
			break WORKERLOOP
		}
	}

	wp.wg.Done()
}
