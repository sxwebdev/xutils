# testutil

Small helpers for tests and examples.

## Installation

```bash
go get github.com/sxwebdev/xutils/testutil
```

## Usage

`PrintJSON` marshals a value to indented JSON and writes it to stdout, returning an error only if the
value cannot be marshaled. Handy for eyeballing structures while debugging a test:

```go
func TestSomething(t *testing.T) {
    resp := callAPI()
    _ = testutil.PrintJSON(resp) // pretty-prints resp as JSON
}
```

## API

| Function       | Description                                 |
| -------------- | ------------------------------------------- |
| `PrintJSON(v)` | Pretty-print `v` as indented JSON to stdout |
