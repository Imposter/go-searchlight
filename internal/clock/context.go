package clock

import (
	"context"
	"errors"
	"time"
)

// WithTimeout is [context.WithTimeout] on c: the context ends once d has passed by c.
// See [WithDeadline].
func WithTimeout(parent context.Context, c Clock, d time.Duration) (context.Context, context.CancelFunc) {
	return WithDeadline(parent, c, c.Now().Add(d))
}

// WithDeadline is [context.WithDeadline] on c: the context ends once c reaches
// deadline, and its Err is then context.DeadlineExceeded. Bound by it a wait that
// polls on c, so a test that drives c drives the bound too.
//
// On [Real] it is context.WithDeadline itself. On any other clock the context is
// cancelled by a c.AfterFunc timer and reports no Deadline: c's time is not the
// process's, and a deadline passed on to a driver or a peer is read by the process's
// clock.
func WithDeadline(parent context.Context, c Clock, deadline time.Time) (context.Context, context.CancelFunc) {
	if _, ok := c.(Real); ok {
		return context.WithDeadline(parent, deadline)
	}
	ctx, cancel := context.WithCancelCause(parent)
	t := c.AfterFunc(c.Until(deadline), func() { cancel(context.DeadlineExceeded) })
	d := &clockDeadline{Context: ctx, done: make(chan struct{})}
	context.AfterFunc(ctx, func() {
		t.Stop()
		close(d.done)
	})
	return d, func() { cancel(context.Canceled) }
}

// clockDeadline reports context.DeadlineExceeded as its Err once its clock's
// deadline cancelled it, as a context.WithDeadline context does. It has a Done
// channel of its own, closed once the inner context ends, so the contexts derived
// from it take their Err from it rather than from the inner context.
type clockDeadline struct {
	context.Context
	done chan struct{}
}

func (c *clockDeadline) Done() <-chan struct{} { return c.done }

func (c *clockDeadline) Err() error {
	select {
	case <-c.done:
	default:
		return nil
	}
	if errors.Is(context.Cause(c.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}
