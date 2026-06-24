package loopper

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sxwebdev/xutils/loggerutil"
)

type Loopper struct {
	options Options

	running atomic.Bool

	// mu guards started/stopped and serializes them with wg.Add, so that no
	// wg.Add can happen once Stop has marked the loop stopped. Without this,
	// a Trigger racing Stop/Wait could Add after Wait returned and panic with
	// "WaitGroup is reused before previous Wait has returned".
	mu      sync.Mutex
	started bool
	stopped bool

	wg     sync.WaitGroup
	cancel context.CancelFunc

	fn func(context.Context)
}

func New(fn func(context.Context), opts ...Option) *Loopper {
	if fn == nil {
		panic("function cannot be nil")
	}

	options := Options{
		period:         time.Second * 60,
		contextTimeout: time.Second * 30,
		logger:         &loggerutil.EmptyLogger{},
	}

	for _, o := range opts {
		o(&options)
	}

	return &Loopper{
		options: options,
		fn:      fn,
	}
}

func (l *Loopper) Start(parent context.Context) {
	l.mu.Lock()
	if l.started || l.stopped {
		// Already started, or stopped before ever starting: do nothing rather
		// than leak a second ticker goroutine and overwrite l.cancel.
		l.mu.Unlock()
		return
	}
	l.started = true
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.wg.Add(1)
	l.mu.Unlock()

	// Run immediately if leading is true.
	if l.options.leading {
		l.tryRun(ctx, l.fn)
	}

	go func() {
		defer l.wg.Done()

		t := time.NewTicker(l.options.period)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// tryRun is a no-op once stopped, so the cancelled ctx (next
				// iteration) is what actually ends the goroutine.
				l.tryRun(ctx, l.fn)
			}
		}
	}()
}

// Trigger triggers the loop to run the provided function immediately if not already running
func (l *Loopper) Trigger(ctx context.Context) bool {
	return l.tryRun(ctx, l.fn)
}

// Stop stops the loop
func (l *Loopper) Stop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	cancel := l.cancel
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// Wait waits for all running operations to complete (graceful shutdown)
func (l *Loopper) Wait() {
	l.wg.Wait()
}

// tryRun tries to run a function within the loop's context
func (l *Loopper) tryRun(parent context.Context, fn func(context.Context)) bool {
	// Take the overlap flag first (cheap, lock-free) so the common "already
	// running" rejection does not contend on the mutex.
	if !l.running.CompareAndSwap(false, true) {
		return false
	}

	// Serialize the stopped check with wg.Add so a concurrent Stop can never
	// let an Add slip through after Wait has started.
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		l.running.Store(false)
		return false
	}
	l.wg.Add(1)
	l.mu.Unlock()

	go func() {
		now := time.Now()
		defer func() {
			if r := recover(); r != nil {
				l.options.logger.Errorf("panic recovered in loop: %v\n%s", r, debug.Stack())
			}
			l.running.Store(false)
			l.options.logger.Debugf("loop iteration took %s", time.Since(now))
			l.wg.Done()
		}()

		ctx := parent
		var cancel context.CancelFunc
		if l.options.contextTimeout > 0 {
			ctx, cancel = context.WithTimeout(parent, l.options.contextTimeout)
			defer cancel()
		}

		fn(ctx)
	}()

	return true
}
