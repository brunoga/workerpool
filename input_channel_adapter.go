package workerpool

import (
	"errors"
	"reflect"
)

var (
	ErrNotChannel         = errors.New("not a channel")
	ErrNotReadableChannel = errors.New("not a readable channel")
)

// InputChannelAdapter expects a readable channel of any value as input and
// returns a readable channel of interface values. Items are copied from the
// input channel to the output channel via an internal goroutine.
func InputChannelAdapter(
	nonInterfaceChannel interface{}) (<-chan interface{}, error) {
	// Get type of input channel.
	reflectionType := reflect.TypeOf(nonInterfaceChannel)

	// Check if it is indeed a channel.
	if reflectionType.Kind() != reflect.Chan {
		return nil, ErrNotChannel
	}

	// Check if it is a readable channel.
	if reflectionType.ChanDir()&reflect.RecvDir == 0 {
		return nil, ErrNotReadableChannel
	}

	interfaceChannel := make(chan interface{})

	go func() {
		reflectionValue := reflect.ValueOf(nonInterfaceChannel)

		for {
			// Copy items from non interface channel to interface
			// channel.
			item, ok := reflectionValue.Recv()
			if !ok {
				close(interfaceChannel)

				break
			}

			interfaceChannel <- item.Interface()
		}
	}()

	return interfaceChannel, nil
}
