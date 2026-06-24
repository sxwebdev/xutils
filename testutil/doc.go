// Package testutil provides small helpers for tests and examples.
//
// [PrintJSON] marshals a value to indented JSON and writes it to stdout,
// returning an error only if the value cannot be marshaled. It is handy for
// eyeballing structures while debugging a test:
//
//	testutil.PrintJSON(resp) // pretty-prints resp as JSON
package testutil
