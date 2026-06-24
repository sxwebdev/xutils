// Package loggerutil defines a minimal logging interface used across xutils so
// packages can emit diagnostics without depending on a concrete logging backend.
//
// # Core Concepts
//
// [Logger] declares formatted (…f) and key-value (…w) methods at debug, info,
// warn, and error levels. Callers accept a Logger and the application supplies
// an adapter around its real logger (zap, slog, zerolog, …).
//
// Two ready-made implementations are provided:
//
//   - [EmptyLogger] — discards everything; the safe default when no logger is set
//   - [TestLogger] — prints to stdout with a level prefix, handy in tests/examples
//
// # Quick Start
//
//	type Service struct {
//		log loggerutil.Logger
//	}
//
//	func New(log loggerutil.Logger) *Service {
//		if log == nil {
//			log = &loggerutil.EmptyLogger{} // never nil-check at every call site
//		}
//		return &Service{log: log}
//	}
//
//	svc := New(loggerutil.NewTestLogger())
package loggerutil
