# workerpool
Go worker pool library.

This library makes it easier to create multiple workers to execute identical tasks over multiple input items
in parallel. Items are sent through an input channel, processed with a provided worker function, and the results
are sent to an output channel.

The library allows arbitrary task interruption and timeouts using a context.
