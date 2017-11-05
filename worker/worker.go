package worker

import (
	"context"
	"errors"
	"sync"
)

var (
	// Errors.
	ErrAlreadyStarted   = errors.New("worker already started")
	ErrNilContext       = errors.New("context must not be nil")
	ErrNilInputChannel  = errors.New("input channel must not be nil")
	ErrNilOutputChannel = errors.New("output channel must not be nil")
	ErrNilWorkerFunc    = errors.New("worker function must not be nil")
	ErrNotStarted       = errors.New("worker not started")
)

// WorkerFunc is the function type that is used by the Worker to process items.
// It receives the item to be processed (as an interface{}) and the context the
// worker was started with (to allow passing extra data to the worker function
// if needed). WorkerFunc implementations should return the result of processing
// the input item (also as an in terface{}) and a nil error on success and a nil
// result and non-nil error on failure.
type WorkerFunc func(interface{}, context.Context) (interface{}, error)

// WorkerError is the error sent through the output channel when there is an
// error processing an item. It includes the input Item that had an error and
// the error returned by the WorkerFunc.
type WorkerError struct {
	Item  interface{}
	Error error
}

// Worker implements an asynchronous and interruptible Worker for processing
// items sent through its input channel and sending results to its output
// channel.
type Worker struct {
	workerFunc WorkerFunc
	cancelFunc context.CancelFunc

	m                     sync.Mutex
	inputChannel          chan interface{}
	outputChannel         chan interface{}
	externalOutputChannel bool
	internalInputChannel  bool
	started               bool

	waitWg    sync.WaitGroup
	waitError error
}

// New returns a new Worker instance that will use the given workerFunc to
// process input items.
func New(workerFunc WorkerFunc) (*Worker, error) {
	if workerFunc == nil {
		return nil, ErrNilWorkerFunc
	}

	return &Worker{
		workerFunc,
		nil,
		sync.Mutex{},
		nil,
		nil,
		false,
		false,
		false,
		sync.WaitGroup{},
		nil,
	}, nil
}

// GetInputChannel returns the channel where input items will be read from. The
// channel will be internally allocated the first time this is called. This must
// be called at least once before Start() (or SetInputChannel() can be used
// instead).
//
// This should be used whene there is the need to have multiple Workers
// processing items from the same source channel to parallelize work.
func (w *Worker) GetInputChannel() (chan<- interface{}, error) {
	w.m.Lock()
	defer w.m.Unlock()

	if w.started {
		return nil, ErrAlreadyStarted
	}

	if w.inputChannel == nil {
		w.inputChannel = make(chan interface{})
		w.internalInputChannel = true
	}

	return w.inputChannel, nil
}

// SetInputChannel sets the channel where the Worker will read items from. This
// must be called before Start() (or GetInputChannel() can be used instead).
func (w *Worker) SetInputChannel(inputChannel chan interface{}) error {
	if inputChannel == nil {
		return ErrNilInputChannel
	}

	w.m.Lock()
	defer w.m.Unlock()

	if w.started {
		return ErrAlreadyStarted
	}

	w.inputChannel = inputChannel

	return nil
}

// GetOutputChannel returns the channel where the result of processing input
// items will be sent to. The channel will be internally allocated the first
// time this is called. This must be called at least once before Start() (or
// SetOutputChannel() can be used instead).
func (w *Worker) GetOutputChannel() (<-chan interface{}, error) {
	w.m.Lock()
	defer w.m.Unlock()

	if w.started {
		return nil, ErrAlreadyStarted
	}

	if w.outputChannel == nil {
		w.outputChannel = make(chan interface{})
	}

	return w.outputChannel, nil
}

// SetOutputChannel sets the channel where the result of processing input
// items will be sent to. This must be called at least once before Start() (or
// GetOutputChannel() can be used instead).
//
// This should be used when there is the need to have multiple Workers
// sending processed items to the same destination channel, usually to
// aggregate data that was processed in parallel by multiple Workers.
func (w *Worker) SetOutputChannel(outputChannel chan interface{}) error {
	if outputChannel == nil {
		return ErrNilOutputChannel
	}

	w.m.Lock()
	defer w.m.Unlock()

	if w.started {
		return ErrAlreadyStarted
	}

	w.outputChannel = outputChannel
	w.externalOutputChannel = true

	return nil
}

// Start starts the Worker with the given context. The context can be used to
// stop Workers with an explicit cancelation or with a timeout and it can also
// be used to pass required data to the workerFunc.
func (w *Worker) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	w.m.Lock()
	defer w.m.Unlock()

	if w.started {
		return ErrAlreadyStarted
	}

	if w.outputChannel == nil {
		return ErrNilOutputChannel
	}

	if w.inputChannel == nil {
		return ErrNilInputChannel
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)

	w.cancelFunc = cancelFunc

	w.waitError = nil

	w.waitWg.Add(1)
	go w.workerLoop(cancelCtx)

	w.started = true

	return nil
}

// Stop stops the Worker.
func (w *Worker) Stop() error {
	w.m.Lock()
	defer w.m.Unlock()

	if !w.started {
		return ErrNotStarted
	}

	w.cancelFunc()

	return nil
}

// Wait blocks until the Worker completes its work.
func (w *Worker) Wait() error {
	w.m.Lock()
	if w.waitError != nil {
		// Worker finished and was not restarted. Simply return last
		// error.
		w.m.Unlock()
		return w.waitError
	}
	if !w.started {
		w.m.Unlock()
		return ErrNotStarted
	}
	w.m.Unlock()

	w.waitWg.Wait()

	return w.waitError
}

func (w *Worker) workerLoop(ctx context.Context) {
WORKERLOOP:
	for {
		select {
		case inputItem, ok := <-w.inputChannel:
			// Got something on input channel.
			if !ok {
				// Input channel was closed. Exit.
				break WORKERLOOP
			}

			// Got an item. Process it.
			outputItem, err := w.workerFunc(inputItem, ctx)
			if err != nil {
				// WorkerFunc returned error. Send WorkerError
				// to outputChannelsnd continue to next item.
				w.outputChannel <- WorkerError{
					inputItem,
					err,
				}
				continue
			}

			if outputItem == nil {
				// We filtered this item. Move to the next.
				continue
			}

			// Send processed item to outputChannel.
			w.outputChannel <- outputItem

		case <-ctx.Done():
			// Context was cancelled or timed out. Exit.
			break WORKERLOOP
		}
	}

	// No more work to do. Clean everything up.
	w.cleanup(ctx)
}

func (w *Worker) cleanup(ctx context.Context) {
	w.m.Lock()
	defer w.m.Unlock()

	if !w.externalOutputChannel {
		// We are not using an external output channel, so close it as
		// it was created internally by us.
		close(w.outputChannel)
	}

	if w.internalInputChannel {
		// We are using an internal input channel, so close it as it was
		// created internally by us.
		close(w.inputChannel)
	}

	w.cancelFunc = nil
	w.inputChannel = nil
	w.outputChannel = nil
	w.externalOutputChannel = false
	w.internalInputChannel = false
	w.started = false

	w.waitError = ctx.Err()
	w.waitWg.Done()
}
