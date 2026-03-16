package dbutil

import "fmt"

type PaginationOption func(*paginationOptions)

type paginationOptions struct {
	MaxLimit uint32
}

// WithMaxLimit sets the maximum allowed page size.
func WithMaxLimit(v uint32) PaginationOption {
	return func(o *paginationOptions) {
		o.MaxLimit = v
	}
}

// Pagination calculates limit and offset from page/pageSize values.
// Default max limit is 100; To disable max limit check, set it to 0 or a very large number using WithMaxLimit option.
// If page is nil, it is treated as 1. If pageSize is nil, it is treated as 100.
func Pagination(page, pageSize *uint32, opts ...PaginationOption) (limit, offset uint32, err error) {
	options := paginationOptions{
		MaxLimit: 100,
	}

	for _, opt := range opts {
		opt(&options)
	}

	limit = 100
	offset = 0

	if pageSize != nil {
		limit = *pageSize
	}

	if page != nil {
		if *page == 0 {
			return 0, 0, fmt.Errorf("pagination error: page cannot be 0, must be >= 1")
		}
		offset = (*page - 1) * limit
	}

	if limit < 1 {
		return 0, 0, fmt.Errorf("pagination error: page size cannot be less than 1")
	}

	if options.MaxLimit > 0 && limit > options.MaxLimit {
		return 0, 0, fmt.Errorf("pagination error: page size cannot be greater than %d", options.MaxLimit)
	}

	return
}
