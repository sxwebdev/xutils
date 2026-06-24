# strutil

Small string helpers for formatting and cleanup.

## Installation

```bash
go get github.com/sxwebdev/xutils/strutil
```

## Formatting

`FormatNumberWithPrecision` inserts a decimal point into an integer-unit string (e.g. token amounts in
their smallest unit), padding with leading zeros as needed:

```go
strutil.FormatNumberWithPrecision("12345", 2) // "123.45"
strutil.FormatNumberWithPrecision("5", 2)     // "0.05"
strutil.FormatNumberWithPrecision("12345", 0) // "12345" (precision <= 0 returns input)
```

`FormatDuration` renders a duration compactly, choosing the unit by magnitude:

```go
strutil.FormatDuration(500 * time.Millisecond) // "500ms"
strutil.FormatDuration(90 * time.Second)       // "1m 30s"
strutil.FormatDuration(time.Hour + time.Minute) // "1h 1m"
strutil.FormatDuration(25 * time.Hour)         // "1d 1h"
```

## Cleanup

`ClearUTF8String` strips null bytes and invalid UTF-8, collapses runs of spaces, drops non-printable
runes, and trims the result. `RemoveNullBytes` removes only null bytes:

```go
strutil.ClearUTF8String("  a\x00\xff  b  ") // "a b"
strutil.RemoveNullBytes("a\x00b")            // "ab"
```

## API

| Function                               | Description                                      |
| -------------------------------------- | ------------------------------------------------ |
| `FormatNumberWithPrecision(num, prec)` | Insert a decimal point at `prec` digits          |
| `FormatDuration(d)`                    | Human-readable duration (`ms`/`s`/`m`/`h`/`d`)   |
| `ClearUTF8String(s)`                   | Strip invalid UTF-8 / non-printable, collapse ws |
| `RemoveNullBytes(s)`                   | Remove null bytes only                           |
