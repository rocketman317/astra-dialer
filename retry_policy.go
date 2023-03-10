package main

import (
	"math"
	"math/rand"
	"time"

	"github.com/gocql/gocql"
)

// copy-pasted and adapted from gocql.ExponentialBackoffRetryPolicy
type RetryPolicy struct {
	NumRetries int
	Min, Max   time.Duration
}

func (e *RetryPolicy) Attempt(q gocql.RetryableQuery) bool {
	if q.Attempts() > e.NumRetries {
		return false
	}
	napTime := getExponentialTime(e.Min, e.Max, q.Attempts())
	time.Sleep(napTime)
	return true
}

func (e *RetryPolicy) GetRetryType(err error) gocql.RetryType {
	switch t := err.(type) {
	case *gocql.RequestErrWriteTimeout:
		if t.WriteType == "COUNTER" && t.Received > 0 {
			return gocql.Rethrow
		}
	}
	return gocql.RetryNextHost
}

// used to calculate exponentially growing time
func getExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1)) //nolint:gosec
	// add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2) //nolint:gosec
	if napDuration > float64(max) {
		return max
	}
	return time.Duration(napDuration)
}
