package pipeline

import "fmt"

func (e *Executor) Debugf(format string, args ...any) {
	if e.debug && e.logger != nil {
		e.logger.Debugf(fmt.Sprintf("[pipeline] %s", format), args...)
	}
}

func (e *Executor) Infof(format string, args ...any) {
	if e.logger != nil {
		e.logger.Infof(fmt.Sprintf("[pipeline] %s", format), args...)
	}
}

func (e *Executor) Warnf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Warnf(fmt.Sprintf("[pipeline] %s", format), args...)
	}
}

func (e *Executor) Errorf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Errorf(fmt.Sprintf("[pipeline] %s", format), args...)
	}
}
