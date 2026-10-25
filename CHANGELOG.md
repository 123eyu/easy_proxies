# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.3.0] - 2026-10-25

### Added
- `pool.blacklist_failed_pin` (default `true`, editable in the pool settings panel as「绑定节点拨号失败时立即拉黑」) controls whether a named session's failed pin blacklists its node immediately. Set it to `false` to keep the automatic re-route but fall back to the `pool.failure_threshold` accumulation path.
- Session IDs accept up to 100 characters (`[A-Za-z0-9_-]{1,100}`) instead of 64, in the proxy auth path, the management link API, and the WebUI validation. The listener username plus separator must still fit the SOCKS5 255-byte credential field.

### Changed
- Require Go 1.26.10 and pin the Docker builder to `golang:1.26.10`, closing the reachable stdlib advisory GO-2026-6809 (`crypto/x509` intermediate-certificate distrust, fixed in 1.26.10) that the release gate flagged.

### Fixed
- Named sessions no longer stay pinned to a dead upstream node. When the dial that fails is the one through the bound node, that request now gets one extra selection even if `retry_enabled: false` or the retry budget is already spent, and a successful re-route migrates the binding; if that attempt fails too, the original pin is kept so a single blip does not silently move the session to another exit. This removes the case where one session ID kept answering `502` while every other session kept working, e.g. `wang.abc_1` failing while `wang.abc-1` succeeded — which is never caused by the character itself, since `_` and `-` are both ordinary session-ID characters.
- A named session's pinned node is blacklisted immediately when its own dial fails on a transport fault, instead of waiting for `pool.failure_threshold` to accumulate: the re-routed request no longer leaves a dead node in rotation for every other session. 429 and timeout/connection-reset/503 failures keep their configured cooldowns, and UDP packet failures through a TCP pin never blacklist the node. `POST /api/nodes/{tag}/release` or the WebUI still restores it immediately.
- Reading a session binding no longer renews its idle lifetime. Only a successful dial refreshes `sticky.session_ttl`, so a session whose every attempt fails can expire instead of being kept alive by failed connections, and the LRU order stays aligned with binding expiry.

## [3.2.0] - 2026-10-12

### Added
- **Named sessions on the main HTTP/SOCKS5 pool entry**: authenticate as `username.sessionID` to reuse a TCP upstream node across connections; the bare username retains normal pool scheduling. Session IDs accept 1–64 ASCII letters, digits, underscores, or hyphens. Available in `pool`/`hybrid` mode with a configured listener username; password verification uses the existing listener credentials.
- **Bounded session lifecycle**: `sticky.session_enabled` defaults to true, `sticky.session_ttl` defaults to 24 hours of connection-dial inactivity, and `sticky.max_sessions` defaults to 10,000 bindings with LRU eviction. Manager-owned bindings match stable URI identities across subscription refreshes/core reloads, but are cleared on process restart.
- **Proxy session management API**: list and reset named bindings with `/api/proxy-sessions`, reset an individual full-username binding with `/api/proxy-sessions/{id}`, and generate HTTP/SOCKS5 links with `/api/proxy-sessions/links` without dialing or creating a binding.
- **Session routing UI**: add dashboard connection shortcuts and a dedicated session page with random IDs, masked URL previews, HTTP/SOCKS5 URL/curl copying, binding search, and single/all-session reset controls; expose named-session settings alongside the compatible source-IP entry.
- **Quality gates**: reusable CI checks for formatting, vet, unit/race tests, production feature tags, UI/entrypoint fixtures, reachable vulnerability scanning, and Linux amd64/arm64 builds before Docker publication.
- **Project review report**: document completed fixes, measured performance, upgrade behavior, validation limits, and the next iteration roadmap in `docs/project-review-2026-10-11.md`.

### Changed
- Keep the existing `sticky.enabled`/`sticky.port` source-IP entry compatible and independent of main-port named sessions. Fixed sessions bind upstream nodes, not guaranteed public exit IPs; failover follows pool availability/retry policy without migrating established connections or replaying application requests. SOCKS5 UDP fallback is request-local and does not replace the TCP binding.
- Document main-port session usage, lifecycle limits, compatibility, and management APIs; clarify the default Docker configuration path and replacing placeholder nodes before use. Add the Chinese session routing guide and update the example's supported protocol list.
- Refresh the native HTML management layout with mobile navigation, node search/status filters, labelled controls and keyboard focus support, reduced-motion support, and chart-library offline degradation, while retaining light/dark/system themes without new frontend dependencies.
- Require Go 1.26.9 and update the Docker builder and compatible networking/cryptography/gRPC dependencies; keep sing-box and its coupled gVisor/uTLS versions unchanged.
- Render node configuration/diagnostics views in on-demand 300-row batches, cap probe timelines, and suspend dashboard streams/log polling in hidden tabs. Add cancellable requests, bounded timeouts, and visible errors for busy probe streams.
- Use identity-indexed named-session selection and a fixed-capacity, allocation-free log write ring. Read subscription status through a lightweight snapshot instead of copying every node.
- Apply management password changes immediately, revoke existing login sessions, and expose authentication status/logout through `GET`/`DELETE /api/auth`.

### Fixed
- Canonicalize VMess node identities: URL-format links ignore query-parameter ordering; encoded JSON links ignore the display-name `ps` field, top-level JSON key ordering, and supported base64 encoding variants. Normalize legacy `node_ports.json` keys during migration to preserve existing ports rather than reassigning nodes on upgrade; equivalent identities also keep named-session bindings across reloads.
- Stage only management-owned settings and persist them with one `SaveManagementSettings` operation, retaining `sticky` settings and on-disk node/subscription configuration rather than overwriting it from a stale full-config snapshot. Coordinate settings publication with node CRUD through the manager's optional settings lock.
- Only configure the sing-box Clash API traffic service when `with_clash_api` is compiled in and management is enabled. Default source builds no longer fail startup by requesting an unavailable optional feature; Docker feature-tagged builds retain traffic statistics.
- Do not count caller cancellation during a pool dial as a node failure or unnecessarily switch its session binding.
- Mask proxy passwords in startup entry-point logs, and correctly encode URL credentials and IPv6 hosts. Full credentialed URLs remain available through the management copy/generation flow, using the existing management authentication settings.
- Apply valid host selection and URL encoding to existing proxy exports as well as generated session links: do not export wildcard listen addresses, bracket IPv6 correctly, and percent-encode special characters in credentials. Preserve the existing healthy-node/default, `all=true`, pool, and GeoIP export behavior.
- Preserve management login cookies when a reverse proxy forwards non-Bearer Authorization headers; explicit Bearer credentials retain precedence, including invalid or malformed tokens.
- Derive displayed node health from backend availability/initial-check/blacklist state instead of cumulative failure counts; report non-successful API responses, including failed node releases, rather than displaying false success. Fix the timestamp formatter's undefined-variable reference.
- Disable settings saves after a settings-load error to prevent blank form values from overwriting configuration; clear generated credentialed links on management authentication expiry and safely escape special characters in full session usernames used by display/reset controls.
- Release the manager lock before HTTP shutdown so settings handlers do not deadlock service termination; bound shutdown without an explicit deadline to a 5-second drain and force-close unfinished streams afterward.
- Serialize configuration read-modify-write transactions across writers, write atomically in ordinary directories, and retain locked in-place compatibility for file bind mounts. Preserve file modes and independent node, management, and subscription fields during concurrent saves.
- Serialize runtime lifecycle operations; propagate startup failures/cancellation, prevent reload after close, return independent configuration snapshots, and roll back runtime/GeoIP state when reload or subscription cache persistence fails.
- Keep manual subscription refresh available when periodic refresh is disabled; coalesce concurrent refreshes, cancel/join the worker on stop, preserve inline nodes/settings, and retain the previous subscription set if any provider fails or returns empty content. Runtime URL removal and timer disabling now supersede stale worker settings.
- Preserve active manual node bans across successful probes and all-members-banned fallback; validate health probes against an actual successful HTTP response and fairly rotate equal-load candidates. Closing an old pool no longer unregisters its replacement.
- Preserve GeoIP CONNECT pre-read data, close both tunnel ends on cancellation/EOF, propagate listener failures, strip all hop-by-hop header values, and stream responses. Bound/validate MMDB updates, cancel them on close, and add bounded TTL/singleflight DNS caching.
- Make traffic SSE parsing frame-complete, bounded, and cancellation-aware; eliminate background goroutines created by unused management servers, use bounded probe worker pools, and prevent stale probe results from overwriting newer health state.
- Validate settings without network side effects; reject malformed/multiple JSON objects and invalid values without publishing them. Preserve disabled GeoIP `0s` compatibility without silently enabling automatic updates. Protect management writes with same-origin checks and HttpOnly/SameSite cookies.
- Preserve URL/base64 subscription variants, literal `+` in names, IPv6 endpoints, special credentials, VMess cipher/alterId, and optional SIP002 root paths. Reject invalid upstream ports, use 443 by default for HTTPS proxies, and omit complete credentialed URIs from URL parse errors.
- Avoid reserved listener ports and integer wraparound during node-port allocation; repair duplicated saved assignments and reject explicit port conflicts consistently regardless of node order, retaining existing stable ports. Honor YAML inline manual ports at startup, including intentional changes to their own stale saved assignments.
- Fail fast on invalid entrypoint configuration paths and keep real configuration, node lists, databases, logs, and local build artifacts out of the Docker context.

## [3.1.2] - 2026-10-08

### Fixed
- Preserve `grpc-opts.grpc-service-name` when converting Clash VMess and Trojan nodes to URIs, so their gRPC transports use the configured service name instead of an empty one (#37).

## [3.1.1] - 2026-09-19

### Fixed
- **Failure cooldowns were force-released almost immediately, so `rate_limit_cooldown` barely took effect**: the cooldown added in 3.1.0 is stored in the same state as a threshold blacklist, and two pre-existing paths cleared that state without distinguishing the two.
  - `releaseIfAllBlacklistedLocked` is a last-resort "every member is banned, let them all back in" escape hatch on the request path. In `multi-port`/`hybrid` mode the builder gives each node its own **one-member** pool, so "every member is banned" became trivially true the instant that single member cooled down — the node was released on the very next request and the effective cooldown was ~0
  - A successful health probe called `forceRelease()`. The sweep runs every 5 minutes over all nodes and probes a different host than the failing request used, so a node whose upstream quota was exhausted still passed and was released — capping any cooldown at ~5 minutes
  - Live cooldowns now survive both automatic paths. Permanent blacklists keep their original auto-clear behaviour (the #8/#9 fix is preserved), and manual release from the WebUI or `POST /api/nodes/{tag}/release` still clears everything immediately
  - **Behaviour change**: when every member of a pool is cooling down at once, requests now fail until the earliest cooldown expires instead of being handed back to a node known to be rate-limited. The 60s default keeps this brief; size `rate_limit_cooldown` against your pool accordingly
- **`"429"` matched inside unrelated numbers**: classification used a bare substring test, so `dial tcp 203.0.113.5:44290: i/o timeout` was treated as rate limiting — applying the long rate-limit cooldown instead of the 60s transient one, and, because cooldowns never increment the failure counter, permanently exempting such a node from the blacklist. Status codes are now matched with digit boundaries (`429` and `503` alike), so ports and dotted quads no longer collide
- Blacklist log lines printed the configured `blacklist_duration` even when an already-running longer ban won, contradicting the expiry timestamp on the same line; they now print the real remaining span

## [3.1.0] - 2026-09-19

### Added
- **Stable per-node ports**: in `multi-port`/`hybrid` mode, each node keeps the same local port across subscription refreshes and process restarts
  - Ports are preserved by a stable node identity derived from the URI (ignoring the display name and query-parameter order), so renamed or reordered subscription nodes keep their port
  - Assignments are persisted to `node_ports.json` next to `config.yaml` and restored on startup
- **Shadowsocks-compatible link format**: support for additional Shadowsocks URI variants (#28)
- **WebUI: export all nodes**: new "全部导出" button and `GET /api/export?all=true` that exports every node regardless of health (dead or alive); the default export still returns only healthy/available nodes
- **WebUI: initial health-check progress**: the startup/periodic node sweep now publishes live progress (`done/total`, available, failed) through `GET /api/nodes` and drives the shared progress bar, so the dashboard shows "初始化探测中 N/M" instead of appearing frozen until the sweep finishes
- **Configurable transient and rate-limit cooldowns**: two new `pool` keys replace the previously hardcoded 60s cooldown — `transient_cooldown` (default `60s`) for timeouts, connection resets and 503, and `rate_limit_cooldown` (falls back to `transient_cooldown` when unset) for HTTP 429 / "too many requests". Both accept Go duration strings (`30s`, `10m`, `2h`), are editable live from the WebUI 设置 → 节点池配置 (Pool) panel via the 「暂时性故障冷却」 and 「429 限流冷却」 fields, and are exposed through `GET`/`PUT /api/settings` as `pool.transient_cooldown` / `pool.rate_limit_cooldown`. Setting `rate_limit_cooldown: 2h` keeps a rate-limited node out of rotation for hours instead of a minute; changing either value requires a config reload (#34)

### Changed
- **Transient failures (429 rate-limit, timeouts, connection resets) no longer trigger the 24h blacklist**: such errors are common for shared/free nodes briefly rate-limited by their CDN and usually clear on their own. They now impose only a configurable cooldown — `pool.transient_cooldown`, default `60s`, with rate-limit (429) failures separately tunable via `pool.rate_limit_cooldown` — and do not count toward the 3-strikes permanent-blacklist threshold; permanent faults (handshake/cert/protocol failures, 404, etc.) keep the original 3 → 24h behavior. This stops a burst of concurrent requests from mass-blacklisting otherwise-healthy nodes for a full day
- Improved configuration persistence diagnostics and error handling
- `entrypoint.sh` now detects the "bind-mount of a non-existent file → Docker creates a directory" foot-gun for `config.yaml`/`nodes.txt` and exits with an actionable fix instead of a vague runtime crash
- Removed `start.sh` and `diagnose.sh` helper scripts; `docker compose up -d` (with a directory mount) is now the documented path. README/docs updated to inline the equivalent checks

### Fixed
- **GeoIP region selection now works through a proxy request header**: the region could previously only be given as a path/authority prefix (`jp/example.com:443`), which most HTTP clients mangle — `net/http` parses a `CONNECT jp/example.com:443` authority into `URL.Host=jp` + `URL.Path=/example.com:443`, so the region was silently dropped. Clients can now send `X-Easyproxy-Region: jp`, which works identically for plain HTTP requests and CONNECT tunnels; the header is stripped before the request is forwarded upstream, an unknown region returns `400` instead of falling back to the default pool, and the original prefix forms still work (#32) — thanks @xingpingcn
- **Error messages now match actual mount configuration**: entrypoint.sh error messages previously hardcoded `./data/` paths, causing confusion when using file-mount mode (`-v ./nodes.txt:/etc/easy_proxies/nodes.txt`). Now displays correct fix instructions for both directory-mount and file-mount configurations
- **Initial/periodic health check hangs, leaving 0 nodes available**: `probeAllNodes` (the startup and periodic sweep) called `probe(ctx)` inline with no hard-timeout guard — a separate path from the batch probe. A protocol dial that ignores `ctx` blocked the worker forever, wedging its semaphore slot; the 32 slots filled up and the sweep stalled, so nodes stayed `initialCheckDone=false` and the dashboard showed 0 available even though manual/batch probes found thousands reachable. Each worker now races the probe against its deadline in a goroutine and always releases its slot within the timeout
- **Batch probe hangs forever (WebUI frozen at "N/M")**: a probe against a node that stalls (accepts TCP but never responds, or a protocol dial/handshake that ignores context cancellation) could block indefinitely, so the stuck goroutines occupied every semaphore slot and `wg.Wait` never returned — freezing the run (e.g. stuck around 1600/8363). Fixed with two layers: (1) `Probe` now runs the check in its own goroutine and races it against the 10s context deadline, so it always returns even if the underlying dial ignores `ctx`; (2) a connection watchdog force-closes the probe connection when the deadline fires, unwinding the stalled goroutine (some sing-box connection types don't honor `SetDeadline`, so the deadline alone was insufficient)
- **WebUI: dashboard blacklist/abnormal count stuck at 0**: `GET /api/nodes` returned only the filtered healthy set, so the frontend never saw blacklisted/unavailable nodes and their count always showed 0. It now returns the full node set, restoring the count and making blacklisted nodes visible in the table with a working "解封" (release) button
- WebUI: long node names and URIs are now truncated so they no longer break the table layout
- Prevent crash from malformed VLESS `packetEncoding` nodes
- Preserve inline nodes when a subscription update occurs

## [3.0.1] - 2026-06-17

### Added
- WebUI: sticky proxy settings are now editable from the dashboard

## [3.0.0] - 2026-06-17

### Added
- **Sticky Proxy**: Optional dedicated entry port (default `listener.port + 1`, e.g. `2324`) that pins each client to a single upstream node by source IP, keeping the egress IP stable instead of rotating per connection
  - Coexists with the regular non-sticky pool entry (`2323`) — choose per port
  - Pin is permanent until the node is blacklisted/removed, then re-selects automatically
  - New `sticky` config section (`enabled`, `port`); listen address and credentials inherited from `listener`
  - Pool/hybrid mode only
- **Log Rotation**: Configurable log file rotation with size limits, backup count, and compression
  - New `log` section in config with `output`, `file`, `max_size`, `max_backups`, `max_age`, `compress` options
  - Uses lumberjack for automatic log rotation
  - Defaults: 50MB max size, 3 backups, 7 days retention
- **WebUI Console**: Real-time log streaming in the dashboard
  - In-memory ring buffer captures last 1000 log lines
  - WebSocket-based live log streaming to browser
  - Console tab in WebUI for instant log viewing
- **AnyTLS Protocol**: Support for AnyTLS outbound protocol
  - Parse `anytls://` URIs from subscriptions
  - Full TLS configuration support
- **TUIC Protocol**: Support for TUIC outbound protocol
  - Parse `tuic://` URIs with UUID and password authentication
  - Congestion control and UDP relay mode configuration
  - Full TLS/ALPN support
- **Clash API Integration**: Embedded Clash API controller
  - Internal controller at `127.0.0.1:9092`
  - Enables Clash-compatible tooling integration

### Changed
- **Subscription Parsing**: Improved Clash YAML format detection
  - User-Agent changed to `clash-verge/v2.2.3` for better compatibility
  - YAML detection sample size increased from 200 to 16384 characters
  - Better support for modern proxy types (AnyTLS, TUIC) in Clash format
- **Docker Entrypoint**: Fixed bind-mount permission issues
  - Uses gosu for privilege dropping
  - Ensures proper file ownership for nodes.txt and logs

### Fixed
- Docker nodes.txt permission denied on bind-mount
- VMess node name extraction from base64 payload
- Cross-platform file locking for Windows support

## [2.0.0] - 2025-01-XX

### Added
- SOCKS5 inbound protocol support via Mixed type
- Cross-platform file locking for Windows support
- GeoIP database auto-download and hot-reload
- Hysteria2 (hy2://) protocol support
- Comprehensive security and performance improvements

### Changed
- Major protocol, performance and UI overhaul
- Improved subscription parsing with better error handling
- Enhanced dashboard with real-time statistics

## [1.1.0] - 2024-12-XX

### Added
- GeoIP region routing and dashboard statistics
- Global skip_cert_verify option
- Node port assignment persistence across reloads
- ARM64 support for Docker image

### Fixed
- Hybrid mode export credentials
- Settings save permission issues
- Health check timing after node registration

## [1.0.0] - 2024-11-XX

### Added
- Initial release
- Pool, multi-port, and hybrid runtime modes
- Support for vmess, vless, trojan, ss, hysteria2, socks5, http protocols
- Subscription support (Base64/plain text/Clash YAML)
- Web dashboard with node management
- Automatic health checks and blacklist recovery
- Configurable DNS resolver
