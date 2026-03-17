# Database and Cache

## dbutil — Database utilities

### Pagination

Calculates `LIMIT` and `OFFSET` from page/pageSize values. Default page size: 100.

```go
import "github.com/sxwebdev/xutils/dbutil"

// Basic usage (page=nil defaults to 1, pageSize=nil defaults to 100)
limit, offset, err := dbutil.Pagination(nil, nil)
// limit=100, offset=0

// With values
page := uint32(3)
size := uint32(20)
limit, offset, err := dbutil.Pagination(&page, &size)
// limit=20, offset=40

// With max limit validation
limit, offset, err := dbutil.Pagination(&page, &size, dbutil.WithMaxLimit(50))
// returns error if pageSize > 50
```

**Validation rules:**

- `page` must be >= 1 (0 is an error)
- `pageSize` must be >= 1
- If `MaxLimit` is set, `pageSize` must not exceed it

### JSONField

Custom type for storing arbitrary JSON in database columns. Implements `sql.Scanner`, `driver.Valuer`, `json.Marshaler`, `json.Unmarshaler`.

```go
type Record struct {
    ID       int
    Metadata dbutil.JSONField
}

// From Go struct to JSON column
var meta dbutil.JSONField
meta.UnmarshalFromAny(map[string]any{"key": "value"})

// Use in SQL queries (implements driver.Valuer)
db.Exec("INSERT INTO records (metadata) VALUES (?)", meta)

// Scan from SQL (implements sql.Scanner)
db.QueryRow("SELECT metadata FROM records WHERE id = ?", id).Scan(&meta)

// Convert back
m := meta.ConvertToMap() // map[string]any

// Or into a typed struct
var config MyConfig
meta.ConvertToAny(&config)
```

### Duration

Custom type wrapping `time.Duration` for database storage. Stores as string (e.g., `"5m30s"`). Implements `sql.Scanner`, `driver.Valuer`, JSON marshal/unmarshal.

```go
type Task struct {
    Timeout dbutil.Duration
}

// JSON: {"timeout": "5m30s"}
// DB column: "5m30s"

d := dbutil.Duration(5 * time.Minute)
d.String()       // "5m0s"
d.ToDuration()   // time.Duration

// Scan from DB
var d dbutil.Duration
row.Scan(&d)
```

### FindResponseWithCount

Generic response wrapper for paginated queries.

```go
type User struct {
    ID   int
    Name string
}

users := []User{{1, "Alice"}, {2, "Bob"}}
resp := dbutil.NewFindResponseWithCount(users, 100)
// resp.Items = users, resp.Count = 100
```

## cacheutil — Cache interface

Generic interface for key-value storage with JSON and TTL support. Implement this with Redis, in-memory, or any backend.

```go
import "github.com/sxwebdev/xutils/cacheutil"

// Interface to implement:
type ICache interface {
    Get(ctx context.Context, key []byte) ([]byte, error)
    Set(ctx context.Context, key []byte, value []byte, expiration time.Duration) error
    Delete(ctx context.Context, key []byte) error
    Keys(ctx context.Context, prefix []byte) ([]string, error)
    KeysAndValues(ctx context.Context, prefix []byte) (map[string][]byte, error)
    GetFromJSON(ctx context.Context, key []byte, dst any) error
    SetJSON(ctx context.Context, key []byte, value any, expiration time.Duration) error
    Exists(ctx context.Context, key []byte) (bool, error)
}
```

**Usage pattern:**

```go
type UserService struct {
    cache cacheutil.ICache
}

func (s *UserService) GetUser(ctx context.Context, id string) (*User, error) {
    var user User
    err := s.cache.GetFromJSON(ctx, []byte("user:"+id), &user)
    if err == nil {
        return &user, nil
    }

    // cache miss — load from DB
    user, err = s.db.FindUser(ctx, id)
    if err != nil {
        return nil, err
    }

    // cache for 5 minutes
    _ = s.cache.SetJSON(ctx, []byte("user:"+id), user, 5*time.Minute)
    return &user, nil
}
```
