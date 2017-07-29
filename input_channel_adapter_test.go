package workerpool

import (
	"testing"
)

func TestInputChannelAdapter_NotChannel(t *testing.T) {
	_, err := InputChannelAdapter(2)
	if err != ErrNotChannel {
		t.Errorf("Expected ErrNotChannel. Got %q.", err)
	}
}

func TestInputChannelAdapter_NonReadableChannel(t *testing.T) {
	_, err := InputChannelAdapter(make(chan<- int64))
	if err != ErrNotReadableChannel {
		t.Errorf("Expected ErrNotReadableChannel error. Got %q.", err)
	}
}

func TestInputChannelAdapter_Ok(t *testing.T) {
	nonInterfaceChannel := make(chan int64)

	ic, err := InputChannelAdapter(nonInterfaceChannel)
	if err != nil {
		t.Errorf("Expected nil error. Got %q.", err)
	}

	go func() {
		item := <-ic

		v, ok := item.(int64)
		if !ok {
			t.Errorf("Expected int64. Got %t.", item)
		}

		if v != 2 {
			t.Errorf("Expected 2. Got %d.", v)
		}
	}()

	nonInterfaceChannel <- 2

	close(nonInterfaceChannel)
}
