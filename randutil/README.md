# randutil

Cryptographically secure random generators for strings, fixed-length numbers, and keys.

## Features

- **Crypto-grade entropy** — every generator uses `crypto/rand`, never `math/rand`
- **No modulo bias** — string generation uses rejection sampling for a uniform distribution
- **Safe-by-default keys** — key generation rejects sizes below 128-bit
- **Raw or encoded keys** — base64url string or raw bytes for direct crypto use

## Installation

```bash
go get github.com/sxwebdev/xutils/randutil
```

## Random Strings

```go
// Default alphabet (a-z A-Z 0-9 _-@#$%):
s, err := randutil.GenerateRandomString(32)

// Custom alphabet:
s, err := randutil.GenerateRandomString(16, randutil.WithAlphabet("0123456789abcdef"))
```

- A non-positive length returns an empty string.
- `WithAlphabet("")` is ignored, keeping the default alphabet.

## Random Numbers

```go
n, err := randutil.GenerateRandomNumber(6) // e.g. 482913 — always 6 digits, no leading zero
```

- `length` must be between 1 and 19.
- At length 19 the upper bound is clamped to `math.MaxInt64` (`10^19 - 1` does not fit in `int64`).

## Keys

```go
// URL-safe, unpadded base64 string (API keys, tokens, secrets):
key, err := randutil.GenerateKey(randutil.RecommendedKeySize)

// Raw bytes (HMAC / AES keys):
raw, err := randutil.GenerateKeyBytes(randutil.RecommendedKeySize)
```

| Constant             | Value | Meaning                         |
| -------------------- | ----- | ------------------------------- |
| `MinKeySize`         | 16    | Minimum allowed size (128-bit)  |
| `RecommendedKeySize` | 32    | Recommended size (256-bit)      |

Sizes below `MinKeySize` are rejected with an error, so a weak key cannot be created by accident.

## Security

The entropy source is `crypto/rand`, the strongest randomness available in Go. On Go 1.24+ a reader
failure aborts the process instead of silently falling back to weak randomness, so the generators
never return low-entropy output. The strength of a key is determined solely by its size — prefer
`RecommendedKeySize` (256-bit) for secrets and long-lived keys.

## API

| Function                       | Description                                              |
| ------------------------------ | -------------------------------------------------------- |
| `GenerateRandomString(n, ...)` | Random string of length `n` from an alphabet             |
| `GenerateRandomNumber(length)` | Random `int64` with exactly `length` digits              |
| `GenerateKey(nBytes)`          | Base64url key string from `nBytes` of entropy            |
| `GenerateKeyBytes(nBytes)`     | Raw key bytes from `nBytes` of entropy                   |
| `WithAlphabet(s)`              | Option: set the alphabet for `GenerateRandomString`      |
