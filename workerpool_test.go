package workerpool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestWorkerPool_NewWorkerPool_NilWorkerFunc(t *testing.T) {
	wp, err := NewWorkerPool(nil, 10)

	if wp != nil {
		t.Errorf("Expected nil WorkerPool.")
	}

	if err != ErrNilWorkerFunc {
		t.Errorf("Expected ErrNilWorkerFunc error. Got %q.", err)

	}
}

func TestWorkerPool_NewWorkerPool_InvalidNumWorkers(t *testing.T) {
	wp, err := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 0)

	if wp != nil {
		t.Errorf("Expected nil WorkerPool.")
	}

	if err != ErrInvalidNumWorkers {
		t.Errorf("Expected ErrInvalidNumWorkers error. Got %q.", err)

	}
}

func TestWorkerPool_NewWorkerPool_Success(t *testing.T) {
	wp, err := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp == nil {
		t.Errorf("Expected non-nil WorkerPool.")
	}

	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
}

func TestWorkerPool_GetOutputChannel(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp.outputChannel != nil {
		t.Errorf("Internal channel was not nil before " +
			"GetOutputChannel().")
	}

	c := wp.GetOutputChannel()

	if c != wp.outputChannel {
		t.Errorf("Internal and returned channels do not match.")
	}
}
func TestWorkerPool_SetInputChannel_NilInputChannel(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp.inputChannel != nil {
		t.Errorf("Internal input channel was not nil before " +
			"SetInputChannel().")
	}

	err := wp.SetInputChannel(nil)
	if err != ErrNilInputChannel {
		t.Errorf("Expected ErrNilInputChannel error. Got %q.", err)
	}
}

func TestWorkerPool_SetInputChannel(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp.inputChannel != nil {
		t.Errorf("Internal input channel was not nil before " +
			"SetInputChannel().")
	}

	c := make(chan interface{})

	err := wp.SetInputChannel(c)
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}

	if c != wp.inputChannel {
		t.Errorf("Internal and given channels do not match.")
	}
}

func TestWorkerPool_Start_NilOutputChannel(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))

	err := wp.Start(context.Background())
	if err != ErrNilOutputChannel {
		t.Errorf("Expected ErrNewOutputChannel error. Got %q.", err)
	}
}

func TestWorkerPool_Start_AlreadyStarted(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))
	_ = wp.GetOutputChannel()

	// Do not leak goroutines.
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	_ = wp.Start(ctx)

	err := wp.Start(ctx)
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}
}

func TestWorkerPool_Wait_NotStarted(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))
	_ = wp.GetOutputChannel()

	err := wp.Wait()
	if err != ErrNotStarted {
		t.Errorf("Expected ErrNotStarted error. Got %q.", err)
	}
}

func TestWorkerPool_Wait_Success(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))
	_ = wp.GetOutputChannel()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancelFunc()

	_ = wp.Start(ctx)

	err := wp.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
}

func TestWorkerPool_CleanupOnCancel(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))
	_ = wp.GetOutputChannel()

	ctx, cancelFunc := context.WithCancel(context.Background())

	_ = wp.Start(ctx)

	cancelFunc()

	_ = wp.Wait()

	if wp.started {
		t.Errorf("Expected worker pool to be stopped.")
	}

	if wp.outputChannel != nil {
		t.Errorf("Expected outputChannel to be nil.")
	}
}

func TestWorkerPool_WorkerFuncError(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return nil, fmt.Errorf("error test")
		}, 1)

	ic := make(chan interface{})
	_ = wp.SetInputChannel(ic)

	oc := wp.GetOutputChannel()

	_ = wp.Start(context.Background())

	go func() {
		result := <-oc

		we, ok := result.(WorkerError)
		if !ok {
			t.Errorf("Expected WorkerError. Got %t.", result)
		}

		if we.Error.Error() != "error test" {
			t.Errorf("Expected error \"error test\". Got %q.",
				we.Error.Error())
		}
	}()

	ic <- struct{}{}

	// Clean shutdown.
	close(ic)

	wp.Wait()
}

func TestWorkerPool_WorkerFuncSuccess(t *testing.T) {
	wp, _ := NewWorkerPool(
		func(interface{}) (interface{}, error) {
			return "test result", nil
		}, 1)

	ic := make(chan interface{})
	_ = wp.SetInputChannel(ic)

	oc := wp.GetOutputChannel()

	_ = wp.Start(context.Background())

	go func() {
		result := <-oc

		r, ok := result.(string)
		if !ok {
			t.Errorf("Expected string. Got %t.", result)
		}

		if r != "test result" {
			t.Errorf("Expected result \"test result\". Got %q.", r)
		}
	}()

	ic <- struct{}{}

	// Clean shutdown.
	close(ic)

	wp.Wait()
}
