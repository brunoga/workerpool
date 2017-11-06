package workerpool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brunoga/workerpool/worker"
)

func TestWorkerPool_New_NilWorkerFunc(t *testing.T) {
	wp, err := New(nil, 10)

	if wp != nil {
		t.Errorf("Expected nil WorkerPool.")
	}

	if err != worker.ErrNilWorkerFunc {
		t.Errorf("Expected ErrNilWorkerFunc error. Got %q.", err)

	}
}

func TestWorkerPool_New_InvalidNumWorkers(t *testing.T) {
	wp, err := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 0)

	if wp != nil {
		t.Errorf("Expected nil WorkerPool.")
	}

	if err != ErrInvalidNumWorkers {
		t.Errorf("Expected ErrInvalidNumWorkers error. Got %v.", err)

	}
}

func TestWorkerPool_New_Success(t *testing.T) {
	wp, err := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp == nil {
		t.Errorf("Expected non-nil WorkerPool.")
	}

	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}
}

func TestWorkerPool_GetOutputChannel(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	if wp.outputChannel != nil {
		t.Errorf("Internal channel was not nil before " +
			"GetOutputChannel().")
	}

	c, err := wp.GetOutputChannel()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}

	if c != wp.outputChannel {
		t.Errorf("Internal and returned channels do not match.")
	}
}
func TestWorkerPool_SetInputChannel_NilInputChannel(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	err := wp.SetInputChannel(nil)
	if err != worker.ErrNilInputChannel {
		t.Errorf("Expected ErrNilInputChannel error. Got %q.", err)
	}
}

func TestWorkerPool_SetInputChannel(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	err := wp.SetInputChannel(make(chan interface{}))
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}
}

func TestWorkerPool_Start_NilOutputChannel(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	_ = wp.SetInputChannel(make(chan interface{}))

	err := wp.Start(context.Background())
	if err != worker.ErrNilOutputChannel {
		t.Errorf("Expected ErrNilOutputChannel error. Got %v.", err)
	}
}

func TestWorkerPool_Start_NilInputChannel(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	_, _ = wp.GetOutputChannel()

	err := wp.Start(context.Background())
	if err != worker.ErrNilInputChannel {
		t.Errorf("Expected ErrNilInputChannel error. Got %v.", err)
	}
}

func TestWorkerPool_Start_AlreadyStarted(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)
	_ = wp.SetInputChannel(make(chan interface{}))
	_, _ = wp.GetOutputChannel()

	// Do not leak goroutines.
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	_ = wp.Start(ctx)

	err := wp.Start(ctx)
	if err != worker.ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}
}

func TestWorkerPool_Stop_NotStarted(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 2)

	err := wp.Stop()
	if err != worker.ErrNotStarted {
		t.Errorf("Expected ErrNotStarted error. Got %v.", err)
	}
}

func TestWorker_Stop_Success(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 2)

	_, _ = wp.GetOutputChannel()
	_ = wp.SetInputChannel(make(chan interface{}))
	_ = wp.Start(context.Background())

	err := wp.Stop()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}

	err = wp.Wait()
	if err != context.Canceled {
		t.Errorf("Expected Canceled error. Got %v.", err)
	}
}

func TestWorkerPool_Wait_NotStarted(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	_ = wp.SetInputChannel(make(chan interface{}))
	_, _ = wp.GetOutputChannel()

	err := wp.Wait()
	if err != worker.ErrNotStarted {
		t.Errorf("Expected ErrNotStarted error. Got %q.", err)
	}
}

func TestWorkerPool_Wait_Success(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		}, 1)

	_ = wp.SetInputChannel(make(chan interface{}))
	_, _ = wp.GetOutputChannel()

	ctx, cancelFunc := context.WithTimeout(context.Background(),
		1*time.Millisecond)
	defer cancelFunc()

	_ = wp.Start(ctx)

	err := wp.Wait()
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded error. Got %q.",
			err)
	}
}

func TestWorkerPool_WorkerFuncError(t *testing.T) {
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, fmt.Errorf("error test")
		}, 1)

	ic := make(chan interface{})
	_ = wp.SetInputChannel(ic)

	oc, _ := wp.GetOutputChannel()

	_ = wp.Start(context.Background())

	go func() {
		result := <-oc

		we, ok := result.(worker.WorkerError)
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
	wp, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			time.Sleep(1 * time.Millisecond)
			return "test result", nil
		}, 10)

	ic := make(chan interface{})
	_ = wp.SetInputChannel(ic)

	oc, _ := wp.GetOutputChannel()

	_ = wp.Start(context.Background())

	go func() {
		for i := 0; i < 10; i++ {
			result := <-oc

			r, ok := result.(string)
			if !ok {
				t.Errorf("Expected string. Got %t.", result)
			}

			if r != "test result" {
				t.Errorf("Expected result \"test result\". "+
					"Got %q.", r)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		ic <- struct{}{}
	}

	// Clean shutdown.
	close(ic)

	err := wp.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
}
