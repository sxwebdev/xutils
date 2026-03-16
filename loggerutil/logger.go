package loggerutil

type Logger interface {
	Debugf(format string, args ...any)
	Debugw(format string, args ...any)
	Infof(format string, args ...any)
	Infow(format string, args ...any)
	Warnf(format string, args ...any)
	Warnw(format string, args ...any)
	Errorf(format string, args ...any)
	Errorw(format string, args ...any)
}
