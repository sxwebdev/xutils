package dbutil_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/dbutil"
)

func TestPagination(t *testing.T) {
	t.Run("defaults when both nil", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(nil, nil)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("page nil defaults to first page", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(nil, new(uint32(20)))
		require.NoError(t, err)
		assert.Equal(t, uint32(20), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("pageSize nil defaults to 100", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(3)), nil)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), limit)
		assert.Equal(t, uint32(200), offset)
	})

	t.Run("page 1", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(1)), new(uint32(10)))
		require.NoError(t, err)
		assert.Equal(t, uint32(10), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("page 2", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(2)), new(uint32(10)))
		require.NoError(t, err)
		assert.Equal(t, uint32(10), limit)
		assert.Equal(t, uint32(10), offset)
	})

	t.Run("page 5 pageSize 25", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(5)), new(uint32(25)))
		require.NoError(t, err)
		assert.Equal(t, uint32(25), limit)
		assert.Equal(t, uint32(100), offset)
	})

	t.Run("error when page is 0", func(t *testing.T) {
		_, _, err := dbutil.Pagination(new(uint32(0)), new(uint32(10)))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "page cannot be 0")
	})

	t.Run("error when pageSize is 0", func(t *testing.T) {
		_, _, err := dbutil.Pagination(new(uint32(1)), new(uint32(0)))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "page size cannot be less than 1")
	})

	t.Run("WithMaxLimit respected", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(1)), new(uint32(50)), dbutil.WithMaxLimit(50))
		require.NoError(t, err)
		assert.Equal(t, uint32(50), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("error when pageSize exceeds max limit", func(t *testing.T) {
		_, _, err := dbutil.Pagination(new(uint32(1)), new(uint32(101)), dbutil.WithMaxLimit(100))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "page size cannot be greater than 100")
	})

	t.Run("max limit 0 disables check", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(1)), new(uint32(5000)), dbutil.WithMaxLimit(0))
		require.NoError(t, err)
		assert.Equal(t, uint32(5000), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("default max limit does not apply without option", func(t *testing.T) {
		// Without WithMaxLimit, there is no cap — any pageSize is valid
		limit, offset, err := dbutil.Pagination(new(uint32(1)), new(uint32(5000)))
		require.NoError(t, err)
		assert.Equal(t, uint32(5000), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("large page number", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(1000)), new(uint32(50)))
		require.NoError(t, err)
		assert.Equal(t, uint32(50), limit)
		assert.Equal(t, uint32(49950), offset)
	})

	t.Run("pageSize 1 is valid", func(t *testing.T) {
		limit, offset, err := dbutil.Pagination(new(uint32(1)), new(uint32(1)))
		require.NoError(t, err)
		assert.Equal(t, uint32(1), limit)
		assert.Equal(t, uint32(0), offset)
	})

	t.Run("pageSize exactly at max limit", func(t *testing.T) {
		limit, _, err := dbutil.Pagination(new(uint32(1)), new(uint32(50)), dbutil.WithMaxLimit(50))
		require.NoError(t, err)
		assert.Equal(t, uint32(50), limit)
	})

	t.Run("pageSize one above max limit", func(t *testing.T) {
		_, _, err := dbutil.Pagination(new(uint32(1)), new(uint32(51)), dbutil.WithMaxLimit(50))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "page size cannot be greater than 50")
	})
}
