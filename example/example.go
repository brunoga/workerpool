package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/brunoga/workerpool"
	"github.com/brunoga/workerpool/worker"
)

var (
	flagNumWorkers = flag.Int("num_workers", runtime.NumCPU(),
		"number of workers to use")
	flagMaxNumber = flag.Uint64("max_number", 10,
		"max number to compute the prime of")
)

// generateSequence send asequence of number from 0 to maxNumber to the channel
// it returns. The returned channel will be closed after all numbers are sent,
func generateSequence(maxNumber uint64) chan uint64 {
	// Create output channel.
	output := make(chan uint64)

	go func() {
		for i := uint64(0); i < maxNumber; i++ {
			output <- i
		}
		close(output)
	}()

	return output
}

// justWait simulates an individual operation that takes some time (1 second).
func justWait(i interface{}, ctx context.Context) (interface{}, error) {
	time.Sleep(1 * time.Second)
	return i, nil
}

func main() {
	flag.Parse()

	// Start a WorkerPool for the justWait function.
	workerPool, err := workerpool.New(justWait, *flagNumWorkers)
	if err != nil {
		panic(err)
	}

	// WorkerPool input is the channel where numbers will be sent to.
	interfaceChannel, err := worker.InputChannelAdapter(
		generateSequence(*flagMaxNumber))
	err = workerPool.SetInputChannel(interfaceChannel)
	if err != nil {
		panic(err)
	}

	// Get WorkerPool output channel.
	outputChannel, err := workerPool.GetOutputChannel()
	if err != nil {
		panic(err)
	}

	// Start work.
	err = workerPool.Start(context.Background())
	if err != nil {
		panic(err)
	}

	// Read results and handle them.
	for result := range outputChannel {
		switch result.(type) {
		case uint64:
			n := result.(uint64)
			fmt.Println("Item", n, "done.")
		default:
			panic("should never happend")
		}
	}
}
