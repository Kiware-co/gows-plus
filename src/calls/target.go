package calls

import "fmt"

// OutboundTarget validates a phone number for an outbound call and returns the
// "+<digits>" form meowcaller dials. Numbers outside the allowlist are refused
// before anything reaches WhatsApp.
func OutboundTarget(allow Allowlist, phone string) (string, error) {
	digits, err := NormalizePhone(phone)
	if err != nil {
		return "", err
	}
	if !allow.Allows(digits) {
		return "", fmt.Errorf("%w: %s", ErrNotAllowed, digits)
	}
	return "+" + digits, nil
}
