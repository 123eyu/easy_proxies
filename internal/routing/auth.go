package routing

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
)

// MaxSessionNameLength bounds a session ID. The listener username plus the
// separator and this ID must still fit the SOCKS5 255-byte credential field.
const MaxSessionNameLength = 100

// ValidSessionName reports whether name is a bounded, case-sensitive session ID.
func ValidSessionName(name string) bool {
	if len(name) < 1 || len(name) > MaxSessionNameLength {
		return false
	}
	for _, ch := range []byte(name) {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

// ParseUsername accepts the exact configured account, or account.session.
// The session result is the suffix, not the account name. Matching the complete
// base before looking for its delimiter also supports accounts containing dots.
func ParseUsername(base, input string) (session string, ok bool) {
	if base == "" {
		return "", false
	}
	if input == base {
		return "", true
	}
	prefix := base + "."
	if !strings.HasPrefix(input, prefix) {
		return "", false
	}
	session = strings.TrimPrefix(input, prefix)
	if !ValidSessionName(session) {
		return "", false
	}
	return session, true
}

// VerifyProxyCredentials validates credentials without disclosing either value.
// Session users share the base account's password. Hashes make the constant-time
// comparison independent of the supplied and configured password lengths.
func VerifyProxyCredentials(base, password, input, suppliedPassword string, sessionEnabled bool) (session string, ok bool) {
	session, usernameOK := ParseUsername(base, input)
	configuredHash := sha256.Sum256([]byte(password))
	suppliedHash := sha256.Sum256([]byte(suppliedPassword))
	passwordOK := subtle.ConstantTimeCompare(configuredHash[:], suppliedHash[:]) == 1
	if !usernameOK || !passwordOK || !sessionEnabled && session != "" {
		return "", false
	}
	return session, true
}
