package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorker_New_NilWorkerFunc(t *testing.T) {
	w, err := New(nil)
	if w != nil {
		t.Errorf("Expected nil Worker.")
	}
	if err != ErrNilWorkerFunc {
		t.Errorf("Expected ErrNilWorkerFunc error. Got %q.", err)
	}
}

func TestWorker_New_Success(t *testing.T) {
	w, err := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	if w == nil {
		t.Errorf("Expected non-nil Worker.")
	}
}

func TestWorker_GetInputChannel_AllocChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	ic, err := w.GetInputChannel()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	if ic == nil {
		t.Errorf("Expected non-nil input channel.")
	}
}

func TestWorker_GetInputChannel_ExistingChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	ic1, _ := w.GetInputChannel()

	ic2, err := w.GetInputChannel()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	if ic2 == nil {
		t.Errorf("Expected non-nil input channel.")
	}
	if ic2 != ic1 {
		t.Errorf(
			"First and second returned input channels do not match")
	}
}

func TestWorker_SetInputChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	ic1 := make(chan interface{})

	err := w.SetInputChannel(ic1)
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}

	ic2, _ := w.GetInputChannel()
	if ic1 != ic2 {
		t.Errorf("Internal and set input channels do not match")
	}
}

func TestWorker_GetOutputChannel_AllocChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc, err := w.GetOutputChannel()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	if oc == nil {
		t.Errorf("Expected non-nil output channel.")
	}
}

func TestWorker_GetOutputChannel_ExistingChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc1, _ := w.GetOutputChannel()

	oc2, err := w.GetOutputChannel()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	if oc2 == nil {
		t.Errorf("Expected non-nil output channel.")
	}
	if oc2 != oc1 {
		t.Errorf("First and second returned output channels do not " +
			"match")
	}
}

func TestWorker_SetOutputChannel_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc1 := make(chan interface{})

	err := w.SetOutputChannel(oc1)
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
	oc2, _ := w.GetOutputChannel()
	if oc1 != oc2 {
		t.Errorf("Internal and set output channels do not match")
	}
}

func TestWorker_Start_NilContext(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	err := w.Start(nil)
	if err != ErrNilContext {
		t.Errorf("Expected ErrNilContext error. Got %q.", err)
	}
}

func TestWorker_Start_NilOutputChannel(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetInputChannel()

	err := w.Start(context.Background())
	if err != ErrNilOutputChannel {
		t.Errorf("Expected ErrNilOutputChannel error. Got %q.", err)
	}
}

func TestWorker_Start_NilInputChannel(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	err := w.Start(context.Background())
	if err != ErrNilInputChannel {
		t.Errorf("Expected ErrNilInputChannel error. Got %q.", err)
	}
}

func TestWorker_Start_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc := make(chan interface{})
	_ = w.SetOutputChannel(oc)

	_, _ = w.GetInputChannel()

	err := w.Start(context.Background())
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}

	close(oc)
}

func TestWorker_Start_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	_ = w.Start(context.Background())

	err := w.Start(context.Background())
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}

	close(ic)
}

func TestWorker_GetInputChannel_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic1 := make(chan interface{})
	_ = w.SetInputChannel(ic1)

	_ = w.Start(context.Background())

	ic2, err := w.GetInputChannel()
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}
	if ic2 != nil {
		t.Errorf("Expected nil input channel")
	}

	close(ic1)
}

func TestWorker_SetInputChannel_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	_ = w.Start(context.Background())

	err := w.SetInputChannel(ic)
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}

	close(ic)
}

func TestWorker_GetOutputChannel_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc1 := make(chan interface{})
	_ = w.SetOutputChannel(oc1)

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	_ = w.Start(context.Background())

	oc2, err := w.GetOutputChannel()
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}
	if oc2 != nil {
		t.Errorf("Expected nil input channel")
	}

	close(oc1)
	close(ic)
}

func TestWorker_SetOutputChannel_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	oc := make(chan interface{})
	_ = w.SetOutputChannel(oc)

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	_ = w.Start(context.Background())

	err := w.SetOutputChannel(oc)
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}

	close(oc)
	close(ic)
}

func TestWorker_AddToWaitGroup_AlreadyStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	_ = w.Start(context.Background())

	var wg sync.WaitGroup
	err := w.AddToWaitGroup(&wg)
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted error. Got %q.", err)
	}

	close(ic)
}

func TestWorker_AddToWaitGroup_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	var wg sync.WaitGroup
	err := w.AddToWaitGroup(&wg)
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}

	_ = w.Start(context.Background())

	go func() {
		time.Sleep(1 * time.Millisecond)
		close(ic)
	}()

	wg.Wait()
}

func TestWorker_Stop_NotStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	err := w.Stop()
	if err != ErrNotStarted {
		t.Errorf("Expected ErrNotStarted error. Got %q.", err)
	}
}

func TestWorker_Stop_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()
	_, _ = w.GetInputChannel()
	_ = w.Start(context.Background())

	err := w.Stop()
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}
}

func TestWorker_Wait_NotStarted(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	err := w.Wait()
	if err != ErrNotStarted {
		t.Errorf("Expected ErrNotStarted error. Got %q.", err)
	}
}

func TestWorker_Wait_Success(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)
	_ = w.Start(context.Background())

	go func() {
		time.Sleep(1 * time.Millisecond)
		close(ic)
	}()

	err := w.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}
}

func TestWorker_Wait_Cancelation(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()
	_, _ = w.GetInputChannel()
	_ = w.Start(context.Background())

	go func() {
		time.Sleep(1 * time.Millisecond)
		w.Stop()
	}()

	err := w.Wait()
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error. Got %q.", err)
	}
}

func TestWorker_Wait_DeadlineExceeded(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, nil
		})

	_, _ = w.GetOutputChannel()
	_, _ = w.GetInputChannel()

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(1*time.Millisecond))
	defer cancel()

	_ = w.Start(ctx)

	err := w.Wait()
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded error. Got %q.",
			err)
	}
}

func TestWorker_WorkerFuncError(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			return nil, fmt.Errorf("error test")
		})

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	oc, _ := w.GetOutputChannel()

	_ = w.Start(context.Background())

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

	w.Wait()
}

func TestWorker_WorkerFuncSuccess(t *testing.T) {
	w, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			time.Sleep(1 * time.Millisecond)
			return "test result", nil
		})

	ic := make(chan interface{})
	_ = w.SetInputChannel(ic)

	oc, _ := w.GetOutputChannel()

	_ = w.Start(context.Background())

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

	err := w.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}
}

func TestWorker_WorkerFuncSuccess_MultipleWorkers(t *testing.T) {
	w1, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			time.Sleep(1 * time.Millisecond)
			return "test result", nil
		})

	w2, _ := New(
		func(interface{}, context.Context) (interface{}, error) {
			time.Sleep(1 * time.Millisecond)
			return "test result", nil
		})

	ic := make(chan interface{})
	_ = w1.SetInputChannel(ic)
	_ = w2.SetInputChannel(ic)

	oc := make(chan interface{})
	_ = w1.SetOutputChannel(oc)
	_ = w2.SetOutputChannel(oc)

	var wg sync.WaitGroup
	_ = w1.AddToWaitGroup(&wg)
	_ = w2.AddToWaitGroup(&wg)

	ctx := context.Background()
	_ = w1.Start(ctx)
	_ = w2.Start(ctx)

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

	go func() {
		time.Sleep(1 * time.Millisecond)

		// Clean shutdown.
		close(ic)
	}()

	wg.Wait()

	// At this point workers finished and cleaned up. Wait will say that
	// workers are not started.

	err := w1.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}

	err = w2.Wait()
	if err != nil {
		t.Errorf("Expected nil error. Got %v.", err)
	}
}
