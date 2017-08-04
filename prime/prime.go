package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"time"

	"github.com/brunoga/workerpool"
)

var (
	flagNumWorkers = flag.Int("num_workers", runtime.NumCPU(),
		"number of workers to use")
	flagMaxNumber = flag.Uint64("max_number", 100000,
		"max number to compute the prime of")
)

// generateNumbers starts a goroutine that will return random uint64 numbers
// between 0 and maxNumber (using the given Rand instance) and send them
// through the returned channel. It will generate maxNumber numbers and close
// the channel.
func generateNumbers(maxNumber uint64, r *rand.Rand) <-chan interface{} {
	// Create output channel.
	output := make(chan interface{})

	i := uint64(0)
	go func() {
		for {
			output <- (r.Uint64() % maxNumber)

			i++
			if i >= maxNumber {
				break
			}
		}

		close(output)
	}()

	return output
}

// isPrime is a workerpool.WorkerFunc implementation that expects an uint64
// number as input and determines if it is a prime number or not. It returns the
// original uint64 number and a nil error on success and nil and a non-nil error
// on failure (including when the number is not prime).
func isPrime(i interface{}, ctx context.Context) (interface{}, error) {
	n, ok := i.(uint64)
	if !ok {
		return nil, fmt.Errorf("expected uint64. Got %t", i)
	}

	// Optimization. Only need to check divisors up to the suqare root of
	// the given number.
	n2 := uint64(math.Ceil(math.Sqrt(float64(n))))

	if n2 < 2 {
		return nil, fmt.Errorf("not prime")
	}

	for divisor := uint64(2); divisor <= n2; divisor++ {
		if (n % divisor) == 0 {
			return nil, fmt.Errorf("not prime")
		}
	}

	return n, nil
}

func main() {
	flag.Parse()

	// Start a WorkerPool for the isPrime function.
	workerPool, err := workerpool.New(isPrime, *flagNumWorkers)
	if err != nil {
		panic(err)
	}

	// Initialize random generator for the generateNumbers() function.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// WorkerPool input is the channel where numbers will be sent to.
	err = workerPool.SetInputChannel(generateNumbers(*flagMaxNumber, r))
	if err != nil {
		panic(err)
	}

	// Get WorkerPool output channel.
	outputChannel := workerPool.GetOutputChannel()

	// Start work.
	err = workerPool.Start(context.Background())
	if err != nil {
		panic(err)
	}

	// Read re3sults and handle them.
	for result := range outputChannel {
		switch result.(type) {
		case uint64:
			// Prime number.
			n := result.(uint64)
			fmt.Println(n, "is prime.")
		case workerpool.WorkerError:
			// Error (including non-prime numbers).
			workerError := result.(workerpool.WorkerError)
			if workerError.Error.Error() != "not prime" {
				// Only print anything if it is an actual error.
				fmt.Println(workerError)
			}
		default:
			panic("should never happend")
		}
	}
}
