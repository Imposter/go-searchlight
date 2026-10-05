package clock

import (
	"context"
	"errors"
	"sync"
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
	done := make(chan struct{})
	d := &clockDeadline{Context: ctx, done: done, finish: sync.OnceFunc(func() {
		t.Stop()
		close(done)
	})}
	context.AfterFunc(ctx, d.finish)
	return d, func() {
		cancel(context.Canceled)
		d.finish()
	}
}

// clockDeadline reports context.DeadlineExceeded as its Err once its clock's
// deadline cancelled it, as a context.WithDeadline context does. It has a Done
// channel of its own, closed once the inner context ends, so the contexts derived
// from it take their Err from it rather than from the inner context. Done and Err
// close it themselves when they find the inner context ended, so a cancel is seen at
// once, as with the context package's contexts.
type clockDeadline struct {
	context.Context
	done   chan struct{}
	finish func()
}

func (c *clockDeadline) Done() <-chan struct{} {
	if c.Context.Err() != nil {
		c.finish()
	}
	return c.done
}

func (c *clockDeadline) Err() error {
	if c.Context.Err() == nil {
		return nil
	}
	c.finish()
	if errors.Is(context.Cause(c.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}
