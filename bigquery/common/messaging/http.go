// Copyright (c) 2025 Skyflow, Inc.

package messaging

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type HttpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type RequestFlow interface {
	// Sends the next request in the flow.
	// A returned error indicates an unrecoverable error.
	// Recoverable errors may occur, in which case no error is returned.
	// `IsDone` can be used to check whether retries are required.
	Send() error

	// Checks whether the request flow has completed or whether retries are required.
	// Just because the request flow is done does not mean it was successful.
	IsDone() bool
}

type ExponentialBackoff struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration
	MaxTries            uint
}

func DefaultExponentialBackoff() *ExponentialBackoff {
	return &ExponentialBackoff{
		MaxTries:            3,
		InitialInterval:     1 * time.Second,
		RandomizationFactor: 0.3,
		Multiplier:          2,
		MaxInterval:         20 * time.Second,
	}
}

type RequestSender func(RequestFlow, *ExponentialBackoff) error

func SendRequest(
	flow RequestFlow,
	exponentialBackoff *ExponentialBackoff,
) error {
	// Two arguments are required by the backoff package; using a placeholder
	operation := func() (placeholder int, err error) {
		if err = flow.Send(); err != nil {
			// If an error is returned, it is unrecoverable - do not retry
			err = backoff.Permanent(err)
			return
		}
		if !flow.IsDone() {
			// Retry is required - return a dummy error to indicate a retry is required
			err = errors.New("")
			return
		}
		// The flow is successful
		return
	}

	_, err := backoff.Retry(
		context.TODO(),
		operation,
		backoff.WithBackOff(&backoff.ExponentialBackOff{
			InitialInterval:     exponentialBackoff.InitialInterval,
			RandomizationFactor: exponentialBackoff.RandomizationFactor,
			Multiplier:          exponentialBackoff.Multiplier,
			MaxInterval:         exponentialBackoff.MaxInterval,
		}),
		backoff.WithMaxTries(exponentialBackoff.MaxTries),
	)

	var permanent *backoff.PermanentError
	if errors.As(err, &permanent) {
		// Only return unrecoverable errors
		return err
	}
	return nil
}

func IsInformationalStatusCode(statusCode int) bool {
	// 1xx
	return http.StatusContinue <= statusCode && statusCode < http.StatusOK
}

func IsSuccessfulStatusCode(statusCode int) bool {
	// 2xx
	return http.StatusOK <= statusCode && statusCode < http.StatusMultipleChoices
}

func IsRedirectionStatusCode(statusCode int) bool {
	// 3xx
	return http.StatusMultipleChoices <= statusCode && statusCode < http.StatusBadRequest
}

func IsClientErrorStatusCode(statusCode int) bool {
	// 4xx
	return http.StatusBadRequest <= statusCode && statusCode < http.StatusInternalServerError
}

func IsServerErrorStatusCode(statusCode int) bool {
	// 5xx
	return http.StatusInternalServerError <= statusCode && statusCode < 600
}
