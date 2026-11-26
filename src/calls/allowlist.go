// Package calls wires WhatsApp voice calls (meowcaller) into a whatsmeow session.
//
// Calls are opt-in per number: a session only answers or places calls to phone
// numbers in its Allowlist. Every other offer is left untouched, so the phone (and
// any other linked device) keeps ringing as if this device did not exist.
package calls

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrNotAllowed is returned when a call targets a number outside the allowlist.
var ErrNotAllowed = errors.New("calls: number is not in the allowlist")

const (
	minPhoneDigits = 8
	maxPhoneDigits = 15 // E.164
)

// NormalizePhone turns a human-written phone number ("+57 313 730-3141") into the
// digits-only international form WhatsApp uses as the user part of a JID.
func NormalizePhone(s string) (string, error) {
	var b strings.Builder
	for _, r := range strings.TrimSpace(s) {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '+' && b.Len() == 0, r == ' ', r == '-', r == '(', r == ')', r == '.':
		default:
			return "", fmt.Errorf("calls: invalid character %q in phone number", r)
		}
	}
	digits := b.String()
	if len(digits) < minPhoneDigits || len(digits) > maxPhoneDigits {
		return "", fmt.Errorf("calls: phone number must have %d-%d digits, got %d", minPhoneDigits, maxPhoneDigits, len(digits))
	}
	return digits, nil
}

// Allowlist is the set of phone numbers a session may call or answer.
// The zero value allows nothing.
type Allowlist struct {
	numbers map[string]struct{}
}

// ParseAllowlist parses a comma-separated list of phone numbers.
func ParseAllowlist(csv string) (Allowlist, error) {
	var list Allowlist
	for _, part := range strings.Split(csv, ",") {
		if strings.TrimSpace(part) == "" {
			continue
		}
		phone, err := NormalizePhone(part)
		if err != nil {
			return Allowlist{}, err
		}
		if list.numbers == nil {
			list.numbers = make(map[string]struct{})
		}
		list.numbers[phone] = struct{}{}
	}
	return list, nil
}

// Allows reports whether phone (any human format) is in the list.
func (a Allowlist) Allows(phone string) bool {
	digits, err := NormalizePhone(phone)
	if err != nil {
		return false
	}
	_, ok := a.numbers[digits]
	return ok
}

// Len returns how many numbers are allowed.
func (a Allowlist) Len() int { return len(a.numbers) }

// Numbers returns the allowed numbers, sorted.
func (a Allowlist) Numbers() []string {
	out := make([]string, 0, len(a.numbers))
	for n := range a.numbers {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
