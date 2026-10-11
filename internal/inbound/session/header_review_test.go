package session

import (
	"net/http"
	"testing"
)

func TestStripAllConnectionHeaderValues(t *testing.T) {
	headers := http.Header{"Connection": []string{"keep-alive, X-Hop-One", "X-Hop-Two"}, "X-Hop-One": []string{"a"}, "X-Hop-Two": []string{"b"}, "X-End-To-End": []string{"keep"}}
	stripProxyHeaders(headers, false)
	if headers.Get("X-Hop-One") != "" || headers.Get("X-Hop-Two") != "" || headers.Get("X-End-To-End") != "keep" {
		t.Fatalf("hop-by-hop cleanup incomplete: %v", headers)
	}
	if !isUpgrade(http.Header{"Connection": []string{"keep-alive", "Upgrade"}, "Upgrade": []string{"websocket"}}) {
		t.Fatal("second Connection value ignored")
	}
}
