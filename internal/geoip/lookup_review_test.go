package geoip

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fixtureMMDB is a synthetic one-node IPv4 database. Both halves of the
// address space point to the same country record; no downloaded/user data is
// needed to test the real MaxMind parser and mmap lifecycle.
func fixtureMMDB(iso string) []byte {
	stringValue := func(value string) []byte { return append([]byte{0x40 | byte(len(value))}, []byte(value)...) }
	mapValue := func(entries ...[]byte) []byte {
		result := []byte{0xe0 | byte(len(entries)/2)}
		for _, entry := range entries {
			result = append(result, entry...)
		}
		return result
	}
	data := mapValue(stringValue("country"), mapValue(stringValue("iso_code"), stringValue(iso), stringValue("names"), mapValue(stringValue("en"), stringValue("Fixture"))))
	metadata := mapValue(stringValue("node_count"), []byte{0xc1, 1}, stringValue("record_size"), []byte{0xa1, 24}, stringValue("ip_version"), []byte{0xa1, 4}, stringValue("database_type"), stringValue("GeoIP2-Country"), stringValue("binary_format_major_version"), []byte{0xa1, 2})
	result := []byte{0, 0, 17, 0, 0, 17}
	result = append(result, make([]byte, 16)...)
	result = append(result, data...)
	result = append(result, make([]byte, 1024)...)
	result = append(result, []byte("\xab\xcd\xefMaxMind.com")...)
	return append(result, metadata...)
}

func fixtureLookup(t testing.TB, iso string) *Lookup {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.mmdb")
	if err := os.WriteFile(path, fixtureMMDB(iso), 0600); err != nil {
		t.Fatal(err)
	}
	lookup, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lookup.Close() })
	return lookup
}

type fixtureRoundTripper func(*http.Request) (*http.Response, error)

func (fn fixtureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }
func fixtureDownloads(t *testing.T, fn fixtureRoundTripper) {
	t.Helper()
	previous := http.DefaultTransport
	http.DefaultTransport = fn
	t.Cleanup(func() { http.DefaultTransport = previous })
}
func fixtureDownloadResponse(data []byte) *http.Response {
	return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Header: make(http.Header)}
}

func TestDisabledAndZeroLookupAreUsable(t *testing.T) {
	disabled, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	for _, lookup := range []*Lookup{disabled, {}} {
		if lookup.IsEnabled() {
			t.Fatal("disabled lookup is enabled")
		}
		for _, uri := range []string{"http://127.0.0.1:8080", "http://must-not-resolve.invalid:8080"} {
			if got := lookup.LookupURI(uri); got.Code != RegionOther || got.Country != "Unknown" {
				t.Fatalf("disabled result=%+v", got)
			}
		}
		if err := lookup.Update(); err == nil {
			t.Fatal("empty database path accepted")
		}
		if err := lookup.Close(); err != nil {
			t.Fatal(err)
		}
		if err := lookup.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLookupCloseIsIdempotentAndDisablesLookups(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	if got := lookup.LookupURI("http://192.0.2.1:8080"); got.Code != RegionJP {
		t.Fatalf("fixture result=%+v", got)
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	if lookup.IsEnabled() {
		t.Fatal("closed lookup still enabled")
	}
	if got := lookup.LookupURI("http://192.0.2.1:8080"); got.Code != RegionOther {
		t.Fatalf("closed lookup returned cached result=%+v", got)
	}
	if err := lookup.Update(); err == nil {
		t.Fatal("closed lookup accepted update")
	}
}

func TestLookupNegativeCacheExpiresAndDNSMissesCoalesce(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	var calls atomic.Int32
	lookup.resolveHost = func(context.Context, string) ([]net.IPAddr, error) {
		calls.Add(1)
		return nil, errors.New("fixture DNS unavailable")
	}
	lookup.LookupURI("http://fixture.test:8080")
	lookup.LookupURI("http://fixture.test:8080")
	if calls.Load() != 1 {
		t.Fatalf("negative cache calls=%d", calls.Load())
	}
	lookup.cacheMu.Lock()
	entry := lookup.dnsCache["fixture.test"]
	entry.expiresAt = time.Now().Add(-time.Second)
	lookup.dnsCache["fixture.test"] = entry
	lookup.cacheMu.Unlock()
	started, release := make(chan struct{}), make(chan struct{})
	lookup.resolveHost = func(context.Context, string) ([]net.IPAddr, error) {
		calls.Add(1)
		close(started)
		<-release
		return []net.IPAddr{{IP: net.IPv4(192, 0, 2, 1)}}, nil
	}
	const count = 32
	results := make(chan RegionInfo, count)
	for i := 0; i < count; i++ {
		go func() { results <- lookup.LookupURI("http://fixture.test:8080") }()
	}
	<-started
	close(release)
	for i := 0; i < count; i++ {
		if got := <-results; got.Code != RegionJP {
			t.Fatalf("recovered DNS result=%+v", got)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("shared DNS miss was resolved %d times", calls.Load()-1)
	}
}

func TestLookupCacheIsBounded(t *testing.T) {
	lookup := &Lookup{}
	for i := 0; i < maxDNSCacheEntries*2; i++ {
		lookup.cacheHost(strconv.Itoa(i), unknownRegion(), dnsCacheTTL)
	}
	if len(lookup.dnsCache) > maxDNSCacheEntries {
		t.Fatalf("cache entries=%d", len(lookup.dnsCache))
	}
}

func TestLookupDoesNotCacheMissFromOlderDatabaseEpoch(t *testing.T) {
	lookup := &Lookup{cacheEpoch: 1}
	lookup.cacheHostAtEpoch("fixture.test", RegionInfo{Code: RegionJP}, dnsCacheTTL, 0)
	if _, ok := lookup.cachedHost("fixture.test"); ok {
		t.Fatal("stale in-flight result restored an invalidated cache entry")
	}
	lookup.cacheHostAtEpoch("fixture.test", RegionInfo{Code: RegionUS}, dnsCacheTTL, 1)
	if got, ok := lookup.cachedHost("fixture.test"); !ok || got.Code != RegionUS {
		t.Fatalf("current cache epoch was rejected: %+v %v", got, ok)
	}
}

func TestUpdateReloadsDatabaseAndInvalidatesRegionCache(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	if got := lookup.LookupURI("http://192.0.2.1:8080"); got.Code != RegionJP {
		t.Fatal(got)
	}
	fixtureDownloads(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != DefaultGeoIPURL {
			t.Fatal("unexpected download URL")
		}
		return fixtureDownloadResponse(fixtureMMDB("US")), nil
	})
	if err := lookup.Update(); err != nil {
		t.Fatal(err)
	}
	if got := lookup.LookupURI("http://192.0.2.1:8080"); got.Code != RegionUS {
		t.Fatalf("stale country after update=%+v", got)
	}
	reopened, err := New(lookup.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.LookupIP("192.0.2.1"); got.Code != RegionUS {
		t.Fatalf("disk database was not replaced=%+v", got)
	}
	entries, err := os.ReadDir(filepath.Dir(lookup.path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("update staging files leaked=%v", entries)
	}
}

func TestInvalidUpdateKeepsExistingDatabase(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	data := append(bytes.Repeat([]byte{0}, 1024), []byte("MaxMind.com")...)
	fixtureDownloads(t, func(*http.Request) (*http.Response, error) { return fixtureDownloadResponse(data), nil })
	if err := lookup.Update(); err == nil {
		t.Fatal("marker-only corrupt database accepted")
	}
	if got := lookup.LookupIP("192.0.2.1"); got.Code != RegionJP {
		t.Fatalf("failed update changed working database=%+v", got)
	}
	if err := validateMMDB(lookup.path); err != nil {
		t.Fatalf("working disk database damaged=%v", err)
	}
}

func TestCloseCancelsInFlightUpdateWithoutRevivingReader(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	started := make(chan struct{})
	fixtureDownloads(t, func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	updated := make(chan error, 1)
	go func() { updated <- lookup.Update() }()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- lookup.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel update download")
	}
	if err := <-updated; err == nil {
		t.Fatal("cancelled update succeeded")
	}
	if lookup.IsEnabled() {
		t.Fatal("updater revived closed reader")
	}
}

func TestConcurrentLookupUpdateAndClose(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	fixtureDownloads(t, func(*http.Request) (*http.Response, error) { return fixtureDownloadResponse(fixtureMMDB("US")), nil })
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lookup.LookupURI("http://192.0.2.1:8080")
				lookup.LookupIP("192.0.2.1")
				lookup.IsEnabled()
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); lookup.Update() }()
	wg.Add(1)
	go func() { defer wg.Done(); lookup.Close() }()
	wg.Wait()
	if lookup.IsEnabled() {
		t.Fatal("concurrent close left reader enabled")
	}
}

func BenchmarkLookupURICached(b *testing.B) {
	lookup := fixtureLookup(b, "JP")
	lookup.resolveHost = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(192, 0, 2, 1)}}, nil
	}
	lookup.LookupURI("http://fixture.test:8080")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lookup.LookupURI("http://fixture.test:8080")
		}
	})
}

func TestCloseWaitsForAutoUpdateWorker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.mmdb")
	if err := os.WriteFile(path, fixtureMMDB("JP"), 0600); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	fixtureDownloads(t, func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	lookup, err := NewWithAutoUpdate(path, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		lookup.Close()
		t.Fatal("auto update worker did not start")
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lookup.updateDone:
	default:
		t.Fatal("Close returned before auto update worker exited")
	}
}

func TestOversizedDownloadDoesNotReplaceDatabase(t *testing.T) {
	lookup := fixtureLookup(t, "JP")
	fixtureDownloads(t, func(*http.Request) (*http.Response, error) {
		response := fixtureDownloadResponse(nil)
		response.ContentLength = maxDatabaseBytes + 1
		return response, nil
	})
	if err := lookup.Update(); err == nil {
		t.Fatal("oversized database download accepted")
	}
	if got := lookup.LookupIP("192.0.2.1"); got.Code != RegionJP {
		t.Fatalf("oversized download changed database=%+v", got)
	}
	entries, err := os.ReadDir(filepath.Dir(lookup.path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("failed oversized download leaked staging files=%v", entries)
	}
}
