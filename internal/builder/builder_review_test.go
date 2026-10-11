package builder

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	"github.com/sagernet/sing-box/option"
)

func TestClashCredentialIPv6ToOutbound(t *testing.T) {
	for _, protocol := range []string{"vmess", "vless", "trojan", "anytls", "hysteria2", "tuic", "ss"} {
		content := `proxies:
  - name: "Name + 中文"
    type: ` + protocol + `
    server: "2001:db8::1"
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    password: "password@/:?#%+中文"
    cipher: aes-128-gcm
    alterId: 64
`
		nodes, err := config.ParseSubscriptionContent(content)
		if err != nil || len(nodes) != 1 {
			t.Fatalf("%s parse: %v", protocol, err)
		}
		outbound, err := buildNodeOutbound("node", nodes[0].URI, false)
		if err != nil {
			t.Fatalf("%s build: %v", protocol, err)
		}
		switch opts := outbound.Options.(type) {
		case *option.VMessOutboundOptions:
			if opts.Server != "2001:db8::1" || opts.AlterId != 64 || opts.Security != "aes-128-gcm" {
				t.Fatalf("lostVMessoptions: %+v", opts)
			}
		case *option.TrojanOutboundOptions:
			if opts.Password != "password@/:?#%+中文" {
				t.Fatal("trojan password changed")
			}
		case *option.AnyTLSOutboundOptions:
			if opts.Password != "password@/:?#%+中文" {
				t.Fatal("anytls password changed")
			}
		case *option.ShadowsocksOutboundOptions:
			if opts.Password != "password@/:?#%+中文" {
				t.Fatal("SS password changed")
			}
		case *option.Hysteria2OutboundOptions:
			if opts.Password != "password@/:?#%+中文" {
				t.Fatal("hysteria2 password changed")
			}
		case *option.TUICOutboundOptions:
			if opts.Password != "password@/:?#%+中文" {
				t.Fatal("TUIC password changed")
			}
		}
	}
}

func TestMalformedURIErrorsOmitCredentials(t *testing.T) {
	const secret = "fixture-secret-password"
	for _, uri := range []string{
		"trojan://" + secret + "@[fixture.invalid:443",
		"http://fixture-user:" + secret + "@fixture.invalid:notaport",
		"vmess://" + secret + "@[fixture.invalid:443",
		"ssr://" + secret + "!invalid-base64",
	} {
		_, err := buildNodeOutbound("fixture", uri, false)
		if err == nil {
			t.Fatalf("malformed URI unexpectedly accepted")
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "fixture-user") {
			t.Fatalf("credential leaked in error: %v", err)
		}
	}
	if _, err := buildVMessOptionsFromURL("vmess://"+secret+"@[fixture.invalid:443", false); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("VMess parse error did not redact credentials: %v", err)
	}
}

func TestUpstreamPortBoundsAndHTTPSDefault(t *testing.T) {
	for _, scheme := range []string{"vless", "trojan", "anytls", "socks5", "http", "https", "tuic", "hysteria2"} {
		for _, port := range []string{"0", "65536", "99999"} {
			_, err := buildNodeOutbound("invalid", scheme+"://uuid:password@fixture.invalid:"+port, false)
			if err == nil {
				t.Fatalf("%s accepted invalid port %s", scheme, port)
			}
		}
	}
	u, _ := url.Parse("https://fixture.invalid")
	opts, err := buildHTTPProxyOptions(u, false)
	if err != nil || opts.ServerPort != 443 {
		t.Fatalf("HTTPS default = %d %v", opts.ServerPort, err)
	}
	if _, err := Build(nil); err == nil {
		t.Fatal("nilconfig must return error")
	}
}

func TestVMessBase64VariantsAndPort(t *testing.T) {
	json := []byte(`{"add":"fixture.invalid","id":"11111111-1111-1111-1111-111111111111","port":443,"ps":"中文","net":"tcp"}`)
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		opts, err := buildVMessOptions("vmess://"+encoding.EncodeToString(json)+"#display", false)
		if err != nil || opts.ServerPort != 443 {
			t.Fatalf("base64variant failed: %v", err)
		}
	}
	uri := "vmess://" + base64.StdEncoding.EncodeToString([]byte(`{"add":"fixture.invalid","id":"uuid","port":65536}`))
	if _, err := buildVMessOptions(uri, false); err == nil {
		t.Fatal("oversizeVMess port accepted")
	}
}
