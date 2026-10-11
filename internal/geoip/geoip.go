package geoip

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/ssuri"

	"github.com/oschwald/geoip2-golang"
	"golang.org/x/sync/singleflight"
)

// Region codes
const (
	RegionJP    = "jp"
	RegionKR    = "kr"
	RegionUS    = "us"
	RegionHK    = "hk"
	RegionTW    = "tw"
	RegionSG    = "sg"
	RegionOther = "other"
)

// Default GeoIP database download URL
const (
	DefaultGeoIPURL        = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb"
	maxDatabaseBytes int64 = 64 << 20
)

// RegionInfo contains region details
type RegionInfo struct {
	Code    string // "jp", "kr", "us", "hk", "tw", "other"
	Country string // Full country name
	ISOCode string // ISO country code
}

// Lookup provides GeoIP lookup functionality
type Lookup struct {
	db             *geoip2.Reader
	mu             sync.RWMutex
	path           string
	updateInterval time.Duration
	stopChan       chan struct{}
	updateDone     chan struct{}
	updateOnce     sync.Once
	dnsCache       map[string]cachedRegion
	cacheMu        sync.RWMutex
	cacheEpoch     uint64
	updateMu       sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	closed         bool
	dnsGroup       singleflight.Group
	resolveHost    func(context.Context, string) ([]net.IPAddr, error)
}

type cachedRegion struct {
	info      RegionInfo
	expiresAt time.Time
}

const (
	dnsCacheTTL         = 15 * time.Minute
	dnsNegativeCacheTTL = time.Minute
	maxDNSCacheEntries  = 4096
)

func unknownRegion() RegionInfo {
	return RegionInfo{Code: RegionOther, Country: "Unknown"}
}

// EnsureDatabase checks if the GeoIP database exists, and downloads it if not
func EnsureDatabase(dbPath string) error {
	if dbPath == "" {
		return nil
	}

	// Check if file already exists and is valid
	info, err := os.Stat(dbPath)
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("geoip database path is not a file: %s", dbPath)
		}
		if info.Size() > 0 {
			return nil // File exists and has content
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat geoip database: %w", err)
	}

	log.Printf("📥 GeoIP database not found at %s, downloading...", dbPath)

	if err := downloadDatabase(dbPath); err != nil {
		return err
	}

	log.Printf("✅ GeoIP database downloaded successfully to %s", dbPath)
	return nil
}

// progressWriter tracks download progress
type progressWriter struct {
	total       int64
	downloaded  int64
	lastPercent int64
	lastLog     time.Time
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)
	p.downloaded += int64(n)

	now := time.Now()
	if p.total > 0 {
		percent := p.downloaded * 100 / p.total
		if percent >= 100 || percent >= p.lastPercent+10 || now.Sub(p.lastLog) >= 3*time.Second {
			log.Printf("   Progress: %d%% (%d/%d bytes)", percent, p.downloaded, p.total)
			p.lastPercent = percent
			p.lastLog = now
		}
	} else if now.Sub(p.lastLog) >= 3*time.Second {
		log.Printf("   Downloaded: %d bytes", p.downloaded)
		p.lastLog = now
	}

	return n, nil
}

// validateMMDB performs basic validation of MMDB file format
func validateMMDB(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 1024 {
		return fmt.Errorf("file too small (%d bytes)", info.Size())
	}

	// Check for MaxMind metadata in the last 8KB
	const tailSize int64 = 8192
	readSize := tailSize
	if info.Size() < readSize {
		readSize = info.Size()
	}
	if _, err := file.Seek(-readSize, io.SeekEnd); err != nil {
		return err
	}
	buf := make([]byte, readSize)
	if _, err := io.ReadFull(file, buf); err != nil && err != io.ErrUnexpectedEOF {
		return err
	}
	if !bytes.Contains(buf, []byte("MaxMind.com")) {
		return fmt.Errorf("missing MaxMind metadata")
	}
	// A metadata marker alone is not sufficient: reject damaged tree/data
	// sections before replacing a working database.
	db, err := geoip2.Open(path)
	if err != nil {
		return fmt.Errorf("invalid MMDB: %w", err)
	}
	return db.Close()
}

// New creates a new GeoIP lookup instance
func New(dbPath string) (*Lookup, error) {
	return NewWithAutoUpdate(dbPath, 0)
}

// NewWithAutoUpdate creates a new GeoIP lookup instance with auto-update support
func NewWithAutoUpdate(dbPath string, updateInterval time.Duration) (*Lookup, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lookup := &Lookup{
		path: dbPath, updateInterval: updateInterval,
		stopChan: make(chan struct{}), dnsCache: make(map[string]cachedRegion),
		ctx: ctx, cancel: cancel, resolveHost: net.DefaultResolver.LookupIPAddr,
	}
	if dbPath == "" {
		return lookup, nil
	}

	// Ensure database exists (download if needed)
	if err := EnsureDatabase(dbPath); err != nil {
		cancel()
		return nil, fmt.Errorf("ensure database: %w", err)
	}

	db, err := geoip2.Open(dbPath)
	if err != nil {
		cancel()
		return nil, err
	}

	lookup.db = db

	// Start auto-update goroutine if interval is set
	if updateInterval > 0 {
		lookup.updateDone = make(chan struct{})
		go func() {
			defer close(lookup.updateDone)
			lookup.autoUpdateLoop()
		}()
		log.Printf("🔄 GeoIP auto-update enabled (interval: %v)", updateInterval)
	}

	return lookup, nil
}

// autoUpdateLoop periodically updates the GeoIP database
func (l *Lookup) autoUpdateLoop() {
	ticker := time.NewTicker(l.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := l.Update(); err != nil {
				log.Printf("⚠️  GeoIP auto-update failed: %v", err)
			}
		case <-l.stopChan:
			return
		}
	}
}

// Update downloads and reloads the GeoIP database
func (l *Lookup) Update() error {
	l.updateMu.Lock()
	defer l.updateMu.Unlock()
	l.mu.RLock()
	closed, dbPath, ctx := l.closed, l.path, l.ctx
	l.mu.RUnlock()
	if closed {
		return errors.New("GeoIP lookup is closed")
	}
	if dbPath == "" {
		return errors.New("GeoIP database path is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Printf("🔄 Updating GeoIP database...")

	// Each updater owns its staging path, even when separate lookups use the
	// same database file. Never overwrite another updater's mapped file.
	temp, err := os.CreateTemp(filepath.Dir(dbPath), ".geoip-update-*.mmdb")
	if err != nil {
		return fmt.Errorf("create update temp file: %w", err)
	}
	tempPath := temp.Name()
	temp.Close()
	defer os.Remove(tempPath)
	if err := downloadDatabaseContext(ctx, tempPath); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Open new database
	newDB, err := geoip2.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open new database: %w", err)
	}

	// Hot-swap the database
	l.mu.Lock()
	if l.closed || ctx.Err() != nil {
		l.mu.Unlock()
		newDB.Close()
		return errors.New("GeoIP lookup is closed")
	}
	// Publish only after the on-disk replacement succeeds. Otherwise retain
	// the old reader and report the failed update to its caller.
	if err := os.Rename(tempPath, dbPath); err != nil {
		l.mu.Unlock()
		newDB.Close()
		return fmt.Errorf("replace database file: %w", err)
	}
	oldDB := l.db
	l.db = newDB
	l.cacheMu.Lock()
	l.cacheEpoch++
	l.dnsCache = make(map[string]cachedRegion)
	l.cacheMu.Unlock()
	l.mu.Unlock()

	// Close old database
	if oldDB != nil {
		oldDB.Close()
	}

	log.Printf("✅ GeoIP database updated successfully")
	return nil
}

// downloadDatabase downloads the GeoIP database to the specified path
func downloadDatabase(dbPath string) error {
	return downloadDatabaseContext(context.Background(), dbPath)
}

func downloadDatabaseContext(ctx context.Context, dbPath string) error {
	// Create parent directory if needed
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
	}

	// Download with timeout
	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, DefaultGeoIPURL, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	if resp.ContentLength > maxDatabaseBytes {
		return fmt.Errorf("GeoIP database exceeds %d-byte download limit", maxDatabaseBytes)
	}

	// Create temp file
	tempFile, err := os.CreateTemp(dir, ".geoip-download-*.mmdb")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	cleanup := true
	defer func() {
		if tempFile != nil {
			tempFile.Close()
		}
		if cleanup {
			os.Remove(tempPath)
		}
	}()

	// Copy with progress tracking
	progress := &progressWriter{total: resp.ContentLength}
	reader := io.TeeReader(io.LimitReader(resp.Body, maxDatabaseBytes+1), progress)
	written, err := io.Copy(tempFile, reader)
	if err != nil {
		return err
	}
	if written > maxDatabaseBytes {
		return fmt.Errorf("GeoIP database exceeds %d-byte download limit", maxDatabaseBytes)
	}

	// Verify download completeness
	if resp.ContentLength > 0 && written < resp.ContentLength {
		return fmt.Errorf("incomplete download (%d/%d bytes)", written, resp.ContentLength)
	}

	// Sync and close
	if err := tempFile.Sync(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	tempFile = nil
	if err := validateMMDB(tempPath); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Rename to target path
	if err := os.Rename(tempPath, dbPath); err != nil {
		return err
	}
	cleanup = false

	return nil
}

// Close closes the GeoIP database and stops auto-update
func (l *Lookup) Close() error {
	// Stop auto-update goroutine
	l.updateOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
		if l.stopChan != nil {
			close(l.stopChan)
		}
	})

	// Cancellation interrupts an in-flight download before waiting for the
	// updater, preventing it from reviving the reader after Close returns.
	l.updateMu.Lock()
	l.mu.Lock()
	l.closed = true
	var err error
	if l.db != nil {
		db := l.db
		l.db = nil
		err = db.Close()
	}
	l.mu.Unlock()
	l.updateMu.Unlock()
	if l.updateDone != nil {
		<-l.updateDone
	}
	return err
}

// IsEnabled returns true if GeoIP lookup is available
func (l *Lookup) IsEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.db != nil
}

// LookupIP returns region info for an IP address
func (l *Lookup) LookupIP(ipStr string) RegionInfo {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.db == nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	record, err := l.db.Country(ip)
	if err != nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	isoCode := record.Country.IsoCode
	country := record.Country.Names["en"]
	if country == "" {
		country = isoCode
	}

	return RegionInfo{
		Code:    isoCodeToRegion(isoCode),
		Country: country,
		ISOCode: isoCode,
	}
}

// LookupURI extracts server from URI and returns region info
func (l *Lookup) LookupURI(uri string) RegionInfo {
	// Disabled/closed lookup never attempts DNS or writes to an uninitialized
	// cache. The zero value is intentionally usable too.
	if !l.IsEnabled() {
		return unknownRegion()
	}
	host := extractHostFromURI(uri)
	if host == "" {
		return unknownRegion()
	}
	host = strings.ToLower(host)
	if cached, ok := l.cachedHost(host); ok {
		return cached
	}
	// Node subscriptions commonly repeat one hostname hundreds of times. A
	// shared miss performs one DNS query, rather than one per build worker.
	value, _, _ := l.dnsGroup.Do(host, func() (any, error) {
		if cached, ok := l.cachedHost(host); ok {
			return cached, nil
		}
		l.cacheMu.RLock()
		epoch := l.cacheEpoch
		l.cacheMu.RUnlock()
		address, ttl := host, dnsCacheTTL
		if net.ParseIP(host) == nil {
			parent := l.ctx
			if parent == nil {
				parent = context.Background()
			}
			ctx, cancel := context.WithTimeout(parent, 3*time.Second)
			defer cancel()
			resolve := l.resolveHost
			if resolve == nil {
				resolve = net.DefaultResolver.LookupIPAddr
			}
			ips, err := resolve(ctx, host)
			if err != nil || len(ips) == 0 {
				result := unknownRegion()
				l.cacheHostAtEpoch(host, result, dnsNegativeCacheTTL, epoch)
				return result, nil
			}
			address = ips[0].IP.String()
		}
		result := l.LookupIP(address)
		l.cacheHostAtEpoch(host, result, ttl, epoch)
		return result, nil
	})
	return value.(RegionInfo)
}

func (l *Lookup) cachedHost(host string) (RegionInfo, bool) {
	l.cacheMu.RLock()
	cached, ok := l.dnsCache[host]
	l.cacheMu.RUnlock()
	return cached.info, ok && time.Now().Before(cached.expiresAt)
}

func (l *Lookup) cacheHost(host string, info RegionInfo, ttl time.Duration) {
	l.cacheMu.RLock()
	epoch := l.cacheEpoch
	l.cacheMu.RUnlock()
	l.cacheHostAtEpoch(host, info, ttl, epoch)
}

func (l *Lookup) cacheHostAtEpoch(host string, info RegionInfo, ttl time.Duration, epoch uint64) {
	l.cacheMu.Lock()
	defer l.cacheMu.Unlock()
	if l.cacheEpoch != epoch {
		return // A completed database update invalidated this in-flight miss.
	}
	if l.dnsCache == nil {
		l.dnsCache = make(map[string]cachedRegion)
	}
	if len(l.dnsCache) >= maxDNSCacheEntries {
		// Keep allocation bounded for large/changing subscriptions; expiring
		// entries naturally refill on demand. Arbitrary eviction is sufficient
		// here because the cache is only an optimization, never routing state.
		for key := range l.dnsCache {
			delete(l.dnsCache, key)
			break
		}
	}
	l.dnsCache[host] = cachedRegion{info: info, expiresAt: time.Now().Add(ttl)}
}

// extractHostFromURI extracts the host/IP from various proxy URI formats
func extractHostFromURI(uri string) string {
	lowerURI := strings.ToLower(uri)

	// VMess: typically base64-encoded JSON — must be handled specially
	if strings.HasPrefix(lowerURI, "vmess://") {
		return extractVMessHost(uri)
	}

	// Shadowsocks: SIP002 and legacy whole-payload base64 formats.
	if strings.HasPrefix(lowerURI, "ss://") || strings.HasPrefix(lowerURI, "shadowsocks://") {
		return extractSSHost(uri)
	}

	// SSR: base64 encoded
	if strings.HasPrefix(lowerURI, "ssr://") {
		return extractSSRHost(uri)
	}

	// All other standard URL-parseable schemes
	standardSchemes := []string{
		"vless://", "trojan://",
		"hysteria://", "hysteria2://", "hy2://",
		"anytls://", "tuic://",
		"socks5://", "socks5h://", "socks://",
		"http://", "https://",
	}
	for _, scheme := range standardSchemes {
		if strings.HasPrefix(lowerURI, scheme) {
			parsed, err := url.Parse(uri)
			if err != nil {
				return ""
			}
			return parsed.Hostname()
		}
	}

	// Fallback: try url.Parse for any unknown scheme
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

// extractVMessHost extracts the server address from a vmess:// URI.
// VMess URIs come in two formats:
//  1. Base64 JSON: vmess://base64({"add":"1.2.3.4", ...})
//  2. URL format:  vmess://uuid@host:port?...
func extractVMessHost(uri string) string {
	// Strip scheme (case-insensitive)
	idx := strings.Index(uri, "://")
	if idx < 0 {
		return ""
	}
	encoded := uri[idx+3:]
	if encoded == "" {
		return ""
	}

	// Strip fragment (#name) if present — it breaks base64 decode
	if hashIdx := strings.Index(encoded, "#"); hashIdx >= 0 {
		encoded = encoded[:hashIdx]
	}

	// Try base64 JSON format first (most common)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(encoded)
	}
	if err == nil && len(decoded) > 0 {
		var obj struct {
			Add string `json:"add"`
		}
		if json.Unmarshal(decoded, &obj) == nil && obj.Add != "" {
			return obj.Add
		}
	}

	// Fallback: try URL format vmess://uuid@host:port?...
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func extractSSHost(uri string) string {
	parsed, err := ssuri.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Server
}

func extractSSRHost(uri string) string {
	// SSR is complex, skip for now - will be marked as "other"
	return ""
}

// isoCodeToRegion maps ISO country codes to our region codes
func isoCodeToRegion(isoCode string) string {
	switch strings.ToUpper(isoCode) {
	case "JP":
		return RegionJP
	case "KR":
		return RegionKR
	case "US":
		return RegionUS
	case "HK":
		return RegionHK
	case "TW":
		return RegionTW
	case "SG":
		return RegionSG
	default:
		return RegionOther
	}
}

// AllRegions returns all supported region codes
func AllRegions() []string {
	return []string{RegionJP, RegionKR, RegionUS, RegionHK, RegionTW, RegionSG, RegionOther}
}

// RegionName returns the display name for a region code
func RegionName(code string) string {
	switch code {
	case RegionJP:
		return "Japan"
	case RegionKR:
		return "Korea"
	case RegionUS:
		return "USA"
	case RegionHK:
		return "Hong Kong"
	case RegionTW:
		return "Taiwan"
	case RegionSG:
		return "Singapore"
	case RegionOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// RegionEmoji returns the flag emoji for a region code
func RegionEmoji(code string) string {
	switch code {
	case RegionJP:
		return "🇯🇵"
	case RegionKR:
		return "🇰🇷"
	case RegionUS:
		return "🇺🇸"
	case RegionHK:
		return "🇭🇰"
	case RegionTW:
		return "🇹🇼"
	case RegionSG:
		return "🇸🇬"
	case RegionOther:
		return "🌍"
	default:
		return "❓"
	}
}
