package strutil

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ClearUTF8String removes invalid utf8 characters from string.
func ClearUTF8String(s string) string {
	b := bytes.ReplaceAll([]byte(s), []byte{0}, []byte{})

	result := make([]byte, 0, len(b))

	lastWasSpace := true

	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		if r == utf8.RuneError && size == 1 { //nolint:nestif
			if !lastWasSpace {
				result = append(result, ' ')
				lastWasSpace = true
			}
			b = b[1:]
		} else {
			if r == ' ' {
				if !lastWasSpace {
					result = append(result, ' ')
					lastWasSpace = true
				}
			} else {
				result = append(result, b[:size]...)
				lastWasSpace = false
			}
			b = b[size:]
		}
	}

	filter := func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}

	return strings.Map(filter, strings.TrimSpace(string(result)))
}

// RemoveNullBytes removes null bytes from a string.
func RemoveNullBytes(input string) string {
	return string(bytes.ReplaceAll([]byte(input), []byte{0}, []byte{}))
}
