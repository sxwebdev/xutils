package workflow

import (
	"maps"
	"sync"
	"time"
)

/*

	StepState

*/

type StepState struct {
	PreviousStage *string `json:"previous_stage,omitempty"`
	PreviousStep  *string `json:"previous_step,omitempty"`
	CurrentStage  string  `json:"current_stage"`
	CurrentStep   string  `json:"current_step"`
	NextStage     *string `json:"next_stage,omitempty"`
	NextStep      *string `json:"next_step,omitempty"`

	StartTime *time.Time `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`

	Status StepStatus `json:"status"`

	Error string `json:"error,omitempty"`

	mu   sync.RWMutex
	Args map[string]any `json:"args,omitempty"`
}

func NewStepState() *StepState {
	return &StepState{}
}

// SetPreviousStage sets the previous stage for the step.
func (s *StepState) SetPreviousStage(stage string) *StepState {
	if stage != "" {
		s.mu.Lock()
		s.PreviousStage = &stage
		s.mu.Unlock()
	}
	return s
}

// SetPreviousStep sets the previous step for the step.
func (s *StepState) SetPreviousStep(step string) *StepState {
	if step != "" {
		s.mu.Lock()
		s.PreviousStep = &step
		s.mu.Unlock()
	}
	return s
}

// SetCurrentStage sets the current stage for the step.
func (s *StepState) SetCurrentStage(stage string) *StepState {
	s.mu.Lock()
	s.CurrentStage = stage
	s.mu.Unlock()
	return s
}

// SetCurrentStep sets the current step for the step.
func (s *StepState) SetCurrentStep(step string) *StepState {
	s.mu.Lock()
	s.CurrentStep = step
	s.mu.Unlock()
	return s
}

// SetNextStage sets the next stage for the step.
func (s *StepState) SetNextStage(stage string) *StepState {
	if stage != "" {
		s.mu.Lock()
		s.NextStage = &stage
		s.mu.Unlock()
	}
	return s
}

// SetNextStep sets the next step for the step.
func (s *StepState) SetNextStep(step string) *StepState {
	if step != "" {
		s.mu.Lock()
		s.NextStep = &step
		s.mu.Unlock()
	}
	return s
}

// SetStartTime sets the start time for the step.
func (s *StepState) SetStartTime(t time.Time) *StepState {
	if !t.IsZero() {
		s.mu.Lock()
		s.StartTime = &t
		s.mu.Unlock()
	}
	return s
}

// SetEndTime sets the end time for the step.
func (s *StepState) SetEndTime(t time.Time) *StepState {
	if !t.IsZero() {
		s.mu.Lock()
		s.EndTime = &t
		s.mu.Unlock()
	}
	return s
}

// SetStatus sets the status for the step.
func (s *StepState) SetStatus(status StepStatus) *StepState {
	s.mu.Lock()
	s.Status = status
	s.mu.Unlock()
	return s
}

// SetError sets the error for the step.
func (s *StepState) SetError(err error) *StepState {
	s.mu.Lock()
	if err == nil {
		s.Error = ""
	} else {
		s.Error = err.Error()
	}
	s.mu.Unlock()
	return s
}

// clone returns a deep copy of the step state taken under the read lock, so a
// snapshot can be marshaled without racing concurrent mutations of the live
// state.
func (s *StepState) clone() *StepState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c := &StepState{
		CurrentStage: s.CurrentStage,
		CurrentStep:  s.CurrentStep,
		StartTime:    s.StartTime,
		EndTime:      s.EndTime,
		Status:       s.Status,
		Error:        s.Error,
	}
	if s.PreviousStage != nil {
		v := *s.PreviousStage
		c.PreviousStage = &v
	}
	if s.PreviousStep != nil {
		v := *s.PreviousStep
		c.PreviousStep = &v
	}
	if s.NextStage != nil {
		v := *s.NextStage
		c.NextStage = &v
	}
	if s.NextStep != nil {
		v := *s.NextStep
		c.NextStep = &v
	}
	if s.Args != nil {
		c.Args = make(map[string]any, len(s.Args))
		maps.Copy(c.Args, s.Args)
	}
	return c
}

// SetArgs sets the arguments for the step.
func (s *StepState) SetArgs(args map[string]any) *StepState {
	s.mu.Lock()
	s.Args = args
	s.mu.Unlock()

	return s
}

// SetArg sets a single argument for the step.
func (s *StepState) SetArg(key string, value any) *StepState {
	s.mu.Lock()
	if s.Args == nil {
		s.Args = make(map[string]any)
	}

	s.Args[key] = value
	s.mu.Unlock()

	return s
}
