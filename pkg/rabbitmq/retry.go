package rabbitmq

import "errors"

// RequeueError wraps an error with explicit requeue control for message handlers.
// When a MessageHandler returns a RequeueError, the consumer will Nack the message
// with the specified requeue flag. Plain errors (not wrapped in RequeueError) default
// to requeue=false for backward compatibility.
type RequeueError struct {
	Err     error
	Requeue bool
}

// Error implements the error interface.
func (e *RequeueError) Error() string {
	return e.Err.Error()
}

// Unwrap returns the wrapped error for use with errors.Is and errors.As.
func (e *RequeueError) Unwrap() error {
	return e.Err
}

// Requeue wraps an error to indicate the message should be requeued on handler failure.
// Use this for transient errors that may succeed on retry.
//
// Example:
//
//	if err := processMessage(msg); err != nil {
//	    return rmq.Requeue(err)  // Message will be requeued
//	}
func Requeue(err error) error {
	return &RequeueError{Err: err, Requeue: true}
}

// NoRequeue wraps an error to indicate the message should NOT be requeued on handler failure.
// Use this for permanent errors that should not be retried.
//
// Example:
//
//	if err := validateMessage(msg); err != nil {
//	    return rmq.NoRequeue(err)  // Message will be discarded
//	}
func NoRequeue(err error) error {
	return &RequeueError{Err: err, Requeue: false}
}

// ShouldRequeue checks if an error indicates the message should be requeued.
// Returns false for plain errors (backward compatible) and true/false based on
// the RequeueError wrapper if present.
func ShouldRequeue(err error) bool {
	var re *RequeueError
	if errors.As(err, &re) {
		return re.Requeue
	}
	return false
}
