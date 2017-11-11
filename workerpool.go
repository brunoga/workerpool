package workerpool

import (
	"errors"
	"sync"

	"github.com/brunoga/context"
	"github.com/brunoga/workerpool/worker"
)

var (
	// Errors.
	ErrInvalidNumWorkers = errors.New("number of workers must be positive")
)

// WorkerPool implements a worker pool for executing the same task over many
// items in parallel.
type WorkerPool struct {
	m             sync.Mutex
	workers       []*worker.Worker
	outputChannel chan interface{}
}

// New returns a new WorkerPool instance that will use the given workerFunc to
// process input items and will have numWorkers workers.
func New(workerFunc worker.WorkerFunc, numWorkers int) (*WorkerPool, error) {
	if numWorkers < 1 {
		return nil, ErrInvalidNumWorkers
	}

	workers := make([]*worker.Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		w, err := worker.New(workerFunc)
		if err != nil {
			return nil, err
		}
		workers[i] = w
	}

	return &WorkerPool{
		sync.Mutex{},
		workers,
		nil,
	}, nil
}

// SetInputChannel sets the channel where the Workers will read items from. This
// must be called before Start().
func (wp *WorkerPool) SetInputChannel(inputChannel chan interface{}) error {
	wp.m.Lock()
	defer wp.m.Unlock()

	for _, w := range wp.workers {
		err := w.SetInputChannel(inputChannel)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetOutputChannel returns the channel where the result of processing input
// items will be sent to. The channel will be internally allocated the first
// time this is called. This must be called at least once before Start().
func (wp *WorkerPool) GetOutputChannel() (<-chan interface{}, error) {
	wp.m.Lock()
	defer wp.m.Unlock()

	if wp.outputChannel == nil {
		wp.outputChannel = make(chan interface{})
		for _, w := range wp.workers {
			err := w.SetOutputChannel(wp.outputChannel)
			if err != nil {
				return nil, err
			}
		}
	}

	return wp.outputChannel, nil
}

// Start starts the WorkerPool with the given context. The context can be used
// to stop the WorkerPool with an explicit cancelation or with a deadline and
// it can also be used to pass required data to the workerFunc.
func (wp *WorkerPool) Start(ctx context.Context) error {
	wp.m.Lock()
	defer wp.m.Unlock()

	workerCtx, cancel := context.WithCancel(ctx)

	for _, w := range wp.workers {
		err := w.Start(context.EnableWait(workerCtx))
		if err != nil {
			return err
		}
	}

	go wp.waitAndDoCleanup(ctx, cancel)

	return nil
}

func (wp *WorkerPool) waitAndDoCleanup(ctx context.Context,
	cancel context.CancelFunc) {
	ctx.WaitForChildren()
	ctx.Finished()
	cancel()

	wp.m.Lock()
	close(wp.outputChannel)
	wp.outputChannel = nil
	wp.m.Unlock()
}
