# dbutil

Helpers for working with `database/sql`.

## Installation

```bash
go get github.com/sxwebdev/xutils/dbutil
```

## Transactions

`WrapTx` runs a function inside a transaction, committing on success and rolling back on error:

```go
err := dbutil.WrapTx(ctx, db, func(tx *sql.Tx) error {
    if _, err := tx.ExecContext(ctx, q1); err != nil {
        return err // triggers rollback
    }
    _, err := tx.ExecContext(ctx, q2)
    return err
})
```

## Pagination

`Pagination` converts 1-based `page` / `pageSize` into SQL `limit` and `offset`:

```go
limit, offset, err := dbutil.Pagination(page, pageSize, dbutil.WithMaxLimit(100))
```

- A nil `page` means page 1; a nil `pageSize` means a page size of 100.
- `page` must be `>= 1`; `pageSize` must be `>= 1`.
- By default **no** maximum page size is enforced — pass `WithMaxLimit` to cap it.

`FindResponseWithCount[T]` is a generic paginated response; `NewFindResponseWithCount` normalizes a nil
slice to an empty one so it marshals as `[]` rather than `null`:

```go
resp := dbutil.NewFindResponseWithCount(items, total)
```

## Column Types

`Duration` is a `time.Duration` that round-trips through JSON (as a string like `"1h30m"`) and SQL (as an
int64 nanosecond count):

```go
type Config struct {
    Timeout dbutil.Duration `json:"timeout"`
}
```

`JSONField` wraps `json.RawMessage` so a JSON/JSONB column can be scanned and stored without an
intermediate struct:

```go
var meta dbutil.JSONField
_ = rows.Scan(&meta)
m := meta.ConvertToMap()
var dst MyType
_ = meta.ConvertToAny(&dst)
```

## API

| Symbol                      | Description                                       |
| --------------------------- | ------------------------------------------------- |
| `WrapTx(ctx, db, fn)`       | Run `fn` in a transaction (commit/rollback)       |
| `Pagination(page, size, …)` | Compute SQL `limit`/`offset` from page/pageSize   |
| `WithMaxLimit(n)`           | Option: cap the page size                         |
| `FindResponseWithCount[T]`  | Generic `{items, count}` paginated response       |
| `Duration`                  | `time.Duration` with JSON + SQL marshaling        |
| `JSONField`                 | `json.RawMessage` column wrapper with conversions |
