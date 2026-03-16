package loggerutil

type EmptyLogger struct{}

func (l *EmptyLogger) Debugf(format string, args ...any) {}
func (l *EmptyLogger) Debugw(format string, args ...any) {}
func (l *EmptyLogger) Infof(format string, args ...any)  {}
func (l *EmptyLogger) Infow(format string, args ...any)  {}
func (l *EmptyLogger) Warnf(format string, args ...any)  {}
func (l *EmptyLogger) Warnw(format string, args ...any)  {}
func (l *EmptyLogger) Errorf(format string, args ...any) {}
func (l *EmptyLogger) Errorw(format string, args ...any) {}

var _ Logger = (*EmptyLogger)(nil)
