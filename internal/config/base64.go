package config

import (
	"encoding/base64"
	"fmt"
	"strings"
)

func decodeProxyBase64(value string) ([]byte, error) {
	value = strings.Join(strings.Fields(value), "")
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, err := encoding.DecodeString(value); err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid base64 subscription")
}
