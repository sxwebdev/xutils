// Package dbutil provides helpers for working with database/sql.
//
// # Transactions
//
// [WrapTx] runs a function inside a transaction, committing on success and
// rolling back on error (or panic-free early return):
//
//	err := dbutil.WrapTx(ctx, db, func(tx *sql.Tx) error {
//		if _, err := tx.ExecContext(ctx, q1); err != nil {
//			return err // triggers rollback
//		}
//		_, err := tx.ExecContext(ctx, q2)
//		return err
//	})
//
// # Pagination
//
// [Pagination] converts 1-based page / pageSize into SQL limit and offset.
// A nil page means page 1; a nil pageSize means a page size of 100. By default
// no maximum page size is enforced — pass [WithMaxLimit] to cap it:
//
//	limit, offset, err := dbutil.Pagination(page, pageSize, dbutil.WithMaxLimit(100))
//
// [FindResponseWithCount] is a generic paginated response holding the items and
// a total count; [NewFindResponseWithCount] normalizes a nil slice to an empty
// one so it marshals as [] rather than null.
//
// # Column Types
//
// [Duration] is a time.Duration that round-trips through JSON (as a string such
// as "1h30m") and SQL (as an int64 nanosecond count), implementing
// json.Marshaler/Unmarshaler and sql.Scanner/driver.Valuer.
//
// [JSONField] wraps json.RawMessage so a JSON/JSONB column can be scanned and
// stored without an intermediate struct, with helpers to convert into a map or
// an arbitrary destination:
//
//	var row struct {
//		Meta    dbutil.JSONField
//		Timeout dbutil.Duration
//	}
//	_ = rows.Scan(&row.Meta, &row.Timeout)
//	m := row.Meta.ConvertToMap()
package dbutil
