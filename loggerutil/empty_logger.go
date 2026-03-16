package loggerutil

type Logger interface {
	Debugf(format string, args ...any)
	Errorf(format string, args ...any)
}

type EmptyLogger struct{}

func (l *EmptyLogger) Debugf(format string, args ...any) {}
func (l *EmptyLogger) Errorf(format string, args ...any) {}

var _ Logger = (*EmptyLogger)(nil)
