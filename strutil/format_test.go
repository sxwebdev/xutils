package strutil_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sxwebdev/xutils/strutil"
)

func TestFormatNumberWithPrecision(t *testing.T) {
	tests := []struct {
		name      string
		number    string
		precision int
		want      string
	}{
		{"precision zero returns input", "12345", 0, "12345"},
		{"precision negative returns input", "12345", -2, "12345"},
		{"no padding needed", "12345", 2, "123.45"},
		{"pad single digit", "5", 2, "0.05"},
		{"pad to exactly one leading digit", "99", 2, "0.99"},
		{"precision one", "1", 1, "0.1"},
		{"trailing-zero value", "100", 2, "1.00"},
		{"strips existing dots", "1.5", 2, "0.15"},
		{"large precision", "12345", 7, "0.0012345"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, strutil.FormatNumberWithPrecision(tt.number, tt.precision))
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"zero", 0, "0ms"},
		{"sub-second", 500 * time.Millisecond, "500ms"},
		{"exact second", time.Second, "1s"},
		{"seconds", 59 * time.Second, "59s"},
		{"exact minute", time.Minute, "1m"},
		{"minutes and seconds", 90 * time.Second, "1m 30s"},
		{"whole minutes", 2 * time.Minute, "2m"},
		{"exact hour", time.Hour, "1h"},
		{"hours and minutes", time.Hour + time.Minute, "1h 1m"},
		{"whole hours", 2 * time.Hour, "2h"},
		{"exact day", 24 * time.Hour, "1d"},
		{"days and hours", 25 * time.Hour, "1d 1h"},
		{"days hours minutes", 24*time.Hour + time.Hour + time.Minute, "1d 1h 1m"},
		{"days with minutes only", 24*time.Hour + time.Minute, "1d 0h 1m"},
		{"whole days", 48 * time.Hour, "2d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, strutil.FormatDuration(tt.d))
		})
	}
}
