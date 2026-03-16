package pipeline

import (
	"context"
	"time"
)

// StepOption configures a Step.
type StepOption func(*Step)

// Action creates an action step that executes once.
func Action(name string, do ActionFunc, opts ...StepOption) Step {
	s := Step{
		Name:   name,
		Action: &ActionStep{Do: do},
	}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}

// Poll creates a poll step that checks a condition repeatedly.
func Poll(name string, check PollFunc, opts ...StepOption) Step {
	s := Step{
		Name: name,
		Poll: &PollStep{Check: check},
	}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}

// Branch creates a branch step that picks a path based on a condition.
func Branch(name string, decide BranchFunc, paths map[string][]Step, opts ...StepOption) Step {
	s := Step{
		Name:   name,
		Branch: &BranchStep{Decide: decide, Paths: paths},
	}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}

// WithCompensate adds a compensating action to an action step.
// The compensating action is called during rollback if this step completed successfully.
func WithCompensate(fn ActionFunc) StepOption {
	return func(s *Step) {
		if s.Action != nil {
			s.Action.Compensate = fn
		}
	}
}

// WithOnEnter adds a callback that runs before the step executes.
// Useful for firing webhooks, logging, or other pre-step actions.
func WithOnEnter(fn func(ctx context.Context, data DataAccessor) error) StepOption {
	return func(s *Step) {
		s.OnEnter = fn
	}
}

// WithRetry configures retry behavior for action steps.
func WithRetry(maxAttempts int, delay time.Duration, backoff bool) StepOption {
	return func(s *Step) {
		s.Retry = &RetryConfig{
			MaxAttempts:  maxAttempts,
			InitialDelay: delay,
			Backoff:      backoff,
		}
	}
}

// WithMaxPollDuration sets the maximum total polling duration.
func WithMaxPollDuration(d time.Duration) StepOption {
	return func(s *Step) {
		if s.Poll != nil {
			s.Poll.MaxDuration = d
		}
	}
}
