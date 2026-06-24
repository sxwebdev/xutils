package strutil_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sxwebdev/xutils/strutil"
)

func TestClearUTF8String(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain ascii", "hello", "hello"},
		{"trims surrounding spaces", "  hello  ", "hello"},
		{"collapses inner spaces", "a    b", "a b"},
		{"removes null bytes", "a\x00b", "ab"},
		{"drops invalid utf8 as space", "a\xffb", "a b"},
		{"removes valid but non-printable control char", "a\x07b", "ab"},
		{"valid multibyte preserved", "héllo", "héllo"},
		{"empty", "", ""},
		{"only invalid bytes", "\xff\xff", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, strutil.ClearUTF8String(tt.in))
		})
	}
}

func TestRemoveNullBytes(t *testing.T) {
	assert.Equal(t, "abc", strutil.RemoveNullBytes("a\x00b\x00c"))
	assert.Equal(t, "abc", strutil.RemoveNullBytes("abc"))
	assert.Equal(t, "", strutil.RemoveNullBytes("\x00\x00"))
}
