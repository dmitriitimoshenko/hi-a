package tools

import (
	"strings"
	"unicode"
)

func RemoveLetters(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) {
			return -1
		}
		return r
	}, s)

}
