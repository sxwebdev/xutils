// Package strutil provides small string helpers for formatting and cleanup.
//
// # Formatting
//
// [FormatNumberWithPrecision] inserts a decimal point into an integer-unit
// string (e.g. token amounts in their smallest unit), padding with leading
// zeros as needed:
//
//	strutil.FormatNumberWithPrecision("12345", 2) // "123.45"
//	strutil.FormatNumberWithPrecision("5", 2)     // "0.05"
//
// [FormatDuration] renders a duration in a compact, human-readable form,
// choosing the unit by magnitude (ms, s, m, h, d):
//
//	strutil.FormatDuration(90 * time.Second)        // "1m 30s"
//	strutil.FormatDuration(25 * time.Hour)          // "1d 1h"
//
// # Cleanup
//
// [ClearUTF8String] strips null bytes and invalid UTF-8, collapses runs of
// spaces, drops non-printable runes, and trims the result. [RemoveNullBytes]
// removes only null bytes, leaving everything else intact:
//
//	strutil.ClearUTF8String("  a\x00\xff  b  ") // "a b"
//	strutil.RemoveNullBytes("a\x00b")            // "ab"
package strutil
