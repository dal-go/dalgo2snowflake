package dalgo2snowflake

import (
	"context"
	"time"
)

// Shutdown terminally closes the client and waits at most two seconds for all
// admitted logical operations and physical work to finish. A shorter caller
// deadline/cancellation takes priority. Nil or pre-aborted ctx still closes the
// client before returning a safe error. Close itself remains nonblocking.
//
// Close every reader before Shutdown: an unfinished reader is still a logical
// operation, even when it has no network work. Timed-out Shutdown never forces
// release, abandons caller callbacks or cancels independent owned cleanup.
// Nil confirms local quiescence only, not remote cancellation or credit bounds.
func (c *Client) Shutdown(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Second)
	if ctx != nil {
		if callerDeadline, ok := ctx.Deadline(); ok {
			// Project even a caller's wall-only deadline onto our monotonic
			// clock. An elapsed private cap must never depend on wall changes.
			observed := time.Now()
			if remaining := callerDeadline.Sub(observed); remaining < deadline.Sub(observed) {
				deadline = observed.Add(remaining)
			}
		}
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	_ = c.Close()
	if ctx == nil {
		return failure("input")
	}
	check := func() error {
		if err := safeContext(ctx); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	for {
		if err := check(); err != nil {
			return err
		}
		c.mu.Lock()
		empty := len(c.active) == 0
		changed := c.stateChanged
		c.mu.Unlock()
		// No successful return based on a snapshot taken before the final
		// caller callback/monotonic check. There is no post-success cleanup.
		if err := check(); err != nil {
			return err
		}
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-changed:
		}
	}
}
