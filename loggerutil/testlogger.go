package loggerutil

import "fmt"

type TestLogger struct{}

// NewTestLogger creates a new instance of TestLogger
func NewTestLogger() *TestLogger {
	return &TestLogger{}
}

func (l *TestLogger) Debugf(format string, args ...any) {
	fmt.Printf("[DEBUG] "+format+"\n", args...)
}

func (l *TestLogger) Debugw(format string, args ...any) {
	fmt.Printf("[DEBUG] "+format+"\n", args...)
}

func (l *TestLogger) Infof(format string, args ...any) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}

func (l *TestLogger) Infow(format string, args ...any) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}

func (l *TestLogger) Warnf(format string, args ...any) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}

func (l *TestLogger) Warnw(format string, args ...any) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}

func (l *TestLogger) Errorf(format string, args ...any) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}

func (l *TestLogger) Errorw(format string, args ...any) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}

var _ Logger = (*TestLogger)(nil)
