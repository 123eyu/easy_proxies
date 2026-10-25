package routing

import (
	"strings"
	"testing"
)

func TestParseUsername(t *testing.T) {
	for _, tt := range []struct {
		base, input, session string
		ok                   bool
	}{
		{"wang", "wang", "", true},
		{"wang", "wang.xxx", "xxx", true},
		{"wang.name", "wang.name", "", true},
		{"wang.name", "wang.name.A_1-z", "A_1-z", true},
		{"wang", "wang.", "", false},
		{"wang", "wang.xxx.more", "", false},
		{"wang", "wangx.xxx", "", false},
		{"wang", "Wang.xxx", "", false},
		{"wang", "wang.会话", "", false},
		{"wang", "wang.bad name", "", false},
		{"wang", "wang." + strings.Repeat("a", 100), strings.Repeat("a", 100), true},
		{"wang", "wang." + strings.Repeat("a", 101), "", false},
		{"", ".xxx", "", false},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParseUsername(tt.base, tt.input)
			if got != tt.session || ok != tt.ok {
				t.Errorf("ParseUsername = (%q, %v), want (%q, %v)", got, ok, tt.session, tt.ok)
			}
		})
	}
}

func TestVerifyProxyCredentials(t *testing.T) {
	for _, tt := range []struct {
		name, input, password, session string
		enabled, ok                    bool
	}{
		{"base", "wang.name", "secret", "", false, true},
		{"session", "wang.name.xyz", "secret", "xyz", true, true},
		{"disabled", "wang.name.xyz", "secret", "", false, false},
		{"wrong_password", "wang.name.xyz", "wrong", "", true, false},
		{"wrong_user", "other.xyz", "secret", "", true, false},
		{"empty_session", "wang.name.", "secret", "", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := VerifyProxyCredentials("wang.name", "secret", tt.input, tt.password, tt.enabled)
			if got != tt.session || ok != tt.ok {
				t.Errorf("credential verification = (%q, %v), want (%q, %v)", got, ok, tt.session, tt.ok)
			}
		})
	}
}
