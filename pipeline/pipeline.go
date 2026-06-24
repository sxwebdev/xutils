package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"
)

// Pipeline is a declarative definition of a multi-step process.
type Pipeline struct {
	// Name is a human-readable name for the pipeline.
	Name string
	// Steps is the ordered list of steps to execute.
	Steps []Step
	// Version is the current version of this pipeline definition.
	// 0 means unversioned (legacy, backward compatible).
	Version int
	// MinResumeVersion is the minimum state version this pipeline can resume.
	// nil means only the current Version is accepted (exact match).
	// Set explicitly to allow resuming older states.
	MinResumeVersion *int
}

// Step is a sealed sum type: exactly one of Action, Poll, or Branch must be set.
type Step struct {
	// Name is a unique identifier for the step within its scope.
	Name string
	// Action is set for one-time execution steps.
	Action *ActionStep
	// Poll is set for steps that check a condition repeatedly.
	Poll *PollStep
	// Branch is set for conditional branching steps.
	Branch *BranchStep
	// OnEnter is called before the step executes (e.g., fire a webhook).
	OnEnter func(ctx context.Context, data DataAccessor) error
	// Retry configures retry behavior for action steps.
	Retry *RetryConfig
}

// ActionStep executes once. Optionally defines a compensating action for rollback.
type ActionStep struct {
	// Do is the forward action.
	Do ActionFunc
	// Compensate is the optional rollback action, called during compensation.
	// If nil, this step has no compensation.
	Compensate ActionFunc
}

// PollStep executes repeatedly until done=true.
type PollStep struct {
	// Check is called on each poll attempt.
	// Returns done=true when the condition is met.
	// Returns retryAfter to indicate when to poll again.
	Check PollFunc
	// MaxDuration limits total polling time. Zero means no limit.
	MaxDuration time.Duration
}

// BranchStep evaluates a condition and picks a sub-pipeline to execute.
type BranchStep struct {
	// Decide returns the name of the path to take.
	Decide BranchFunc
	// Paths maps path names to sub-pipelines.
	Paths map[string][]Step
}

// ActionFunc is the signature for action step functions.
type ActionFunc func(ctx context.Context, data DataAccessor) error

// PollFunc is the signature for poll step functions.
// Returns (done, retryAfter, error).
type PollFunc func(ctx context.Context, data DataAccessor) (done bool, retryAfter time.Duration, err error)

// BranchFunc is the signature for branch decision functions.
// Returns the name of the chosen path.
type BranchFunc func(ctx context.Context, data DataAccessor) (path string, err error)

// RetryConfig configures retry behavior for action steps.
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts. Default is 1 (no retry).
	MaxAttempts int
	// InitialDelay is the delay before the first retry.
	InitialDelay time.Duration
	// Backoff enables exponential backoff when true.
	Backoff bool
}

// DataAccessor provides access to shared data between pipeline steps.
// Values are JSON-serialized for persistence across restarts.
type DataAccessor interface {
	// Set stores a value by key. The value must be JSON-serializable.
	Set(key string, value any)
	// Get retrieves a value by key. Returns (nil, false) if not found.
	Get(key string) (any, bool)
	// All returns a copy of all stored data.
	All() map[string]any
}

// GetData retrieves a typed value from a DataAccessor.
func GetData[T any](data DataAccessor, key string) (T, error) {
	var zero T

	raw, ok := data.Get(key)
	if !ok {
		return zero, fmt.Errorf("pipeline: data key %q not found", key)
	}

	// If the value is json.RawMessage (after restore from snapshot),
	// unmarshal it into the target type.
	if rawJSON, ok := raw.(json.RawMessage); ok {
		var result T
		if err := json.Unmarshal(rawJSON, &result); err != nil {
			return zero, fmt.Errorf("pipeline: unmarshal data key %q: %w", key, err)
		}
		return result, nil
	}

	// Direct type assertion for in-memory values.
	result, ok := raw.(T)
	if !ok {
		return zero, fmt.Errorf("pipeline: data key %q: type assertion to %T failed (got %T)", key, zero, raw)
	}

	return result, nil
}

// dataStore is the default implementation of DataAccessor.
type dataStore struct {
	data map[string]any
}

func newDataStore() *dataStore {
	return &dataStore{data: make(map[string]any)}
}

func (d *dataStore) Set(key string, value any) {
	d.data[key] = value
}

func (d *dataStore) Get(key string) (any, bool) {
	v, ok := d.data[key]
	return v, ok
}

func (d *dataStore) All() map[string]any {
	result := make(map[string]any, len(d.data))
	maps.Copy(result, d.data)
	return result
}

// marshalData serializes the data store for persistence.
// Returns an error if any value fails to marshal.
func (d *dataStore) marshalData() (map[string]json.RawMessage, error) {
	result := make(map[string]json.RawMessage, len(d.data))
	for k, v := range d.data {
		// If already RawMessage, keep as-is.
		if raw, ok := v.(json.RawMessage); ok {
			result[k] = raw
			continue
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("pipeline: failed to marshal data key %q: %w", k, err)
		}
		result[k] = b
	}
	return result, nil
}

// restoreData loads data from persisted JSON.
func (d *dataStore) restoreData(data map[string]json.RawMessage) {
	d.data = make(map[string]any, len(data))
	for k, v := range data {
		d.data[k] = v
	}
}

// effectiveMinResumeVersion returns the minimum state version this pipeline can resume.
// If MinResumeVersion is nil, defaults to Version (exact match only).
func (p *Pipeline) effectiveMinResumeVersion() int {
	if p.MinResumeVersion != nil {
		return *p.MinResumeVersion
	}
	return p.Version
}

// validate checks the pipeline definition for errors.
func (p *Pipeline) validate() error {
	if p.Name == "" {
		return fmt.Errorf("pipeline: name is required")
	}
	if p.Version < 0 {
		return fmt.Errorf("pipeline %q: version must be >= 0", p.Name)
	}
	if p.MinResumeVersion != nil {
		if *p.MinResumeVersion < 0 {
			return fmt.Errorf("pipeline %q: min resume version must be >= 0", p.Name)
		}
		if *p.MinResumeVersion > p.Version {
			return fmt.Errorf("pipeline %q: min resume version (%d) cannot exceed version (%d)", p.Name, *p.MinResumeVersion, p.Version)
		}
	}
	if len(p.Steps) == 0 {
		return fmt.Errorf("pipeline %q: no steps defined", p.Name)
	}
	return validateSteps(p.Steps, nil)
}

func validateSteps(steps []Step, parentPath []string) error {
	names := make(map[string]struct{}, len(steps))
	for i, step := range steps {
		if step.Name == "" {
			return fmt.Errorf("pipeline: step at index %d (path %v) has no name", i, parentPath)
		}

		if _, exists := names[step.Name]; exists {
			return fmt.Errorf("pipeline: duplicate step name %q (path %v)", step.Name, parentPath)
		}
		names[step.Name] = struct{}{}

		// Validate exactly one type is set.
		typeCount := 0
		if step.Action != nil {
			typeCount++
		}
		if step.Poll != nil {
			typeCount++
		}
		if step.Branch != nil {
			typeCount++
		}
		if typeCount != 1 {
			return fmt.Errorf("pipeline: step %q must have exactly one of Action, Poll, or Branch (got %d)", step.Name, typeCount)
		}

		// Validate action.
		if step.Action != nil && step.Action.Do == nil {
			return fmt.Errorf("pipeline: action step %q has nil Do function", step.Name)
		}

		// Validate poll.
		if step.Poll != nil && step.Poll.Check == nil {
			return fmt.Errorf("pipeline: poll step %q has nil Check function", step.Name)
		}

		// Validate branch.
		if step.Branch != nil {
			if step.Branch.Decide == nil {
				return fmt.Errorf("pipeline: branch step %q has nil Decide function", step.Name)
			}
			if len(step.Branch.Paths) == 0 {
				return fmt.Errorf("pipeline: branch step %q has no paths", step.Name)
			}
			for pathName, pathSteps := range step.Branch.Paths {
				childPath := append(append([]string{}, parentPath...), step.Name, pathName)
				if err := validateSteps(pathSteps, childPath); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
