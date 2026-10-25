# Easy Proxies

[简体中文](README_ZH.md)

> A sing-box based proxy pool manager -- aggregate many upstream proxy nodes into one stable, health-checked, load-balanced local proxy endpoint.

## Features

- **Three runtime modes**: `pool` (single-port load balancing), `multi-port` (one port per node), and `hybrid` (both simultaneously)
- **Wide protocol support**: VLESS, VMess, Trojan, Shadowsocks, Hysteria2, TUIC, AnyTLS, SOCKS5, HTTP/HTTPS
- **Automatic health checking** with configurable failure thresholds, blacklist duration, and separate cooldowns for transient (timeout/reset/503) and rate-limit (429) failures, plus manual blacklist/release from the dashboard
- **Automatic fail-over retry**: when a node's dial fails, the request is retried on another healthy node (configurable attempts)
- **GeoIP region routing**: classify nodes by country and route traffic through a specific region via a dedicated HTTP proxy endpoint
- **Session routing on the main port**: use `username.sessionID` to keep the same upstream node across connections, with failover when that node fails; the bare username keeps normal pool scheduling. The optional legacy source-IP sticky port remains available
- **Multiple node sources**: inline config, `nodes.txt` file, or subscription URLs (Base64, plain text, Clash YAML)
- **Subscription auto-refresh with hot-reload**: periodically fetches subscription updates and reloads without restart
- **Stable per-node ports**: in `multi-port`/`hybrid` mode each node keeps the same local port across subscription refreshes and restarts (persisted to `node_ports.json`)
- **WebUI dashboard**: real-time node status, traffic charts, diagnostics, log console, and common runtime/subscription settings
- **Management API**: RESTful endpoints for node CRUD, probing, blacklisting, subscription management, and config reload
- **Configurable DNS resolver** with fallback servers and IPv4/IPv6 strategy control
- **Log rotation**: size-based rotation with configurable backup count, age, and compression
- **Multi-platform Docker**: supports amd64 and arm64 with host networking

## Quick Start

### 1. Prepare Configuration

```bash
cp config.example.yaml config.yaml
touch nodes.txt
```

Edit `config.yaml` and add your proxy nodes (inline nodes, `nodes.txt` file, or subscription URLs). Replace the example `vless://uuid@server:443` entry before use: it is documentation, not a working node. If you use subscriptions or a node file, remove that inline example after configuring the actual source.

> Why `touch nodes.txt`? If you bind-mount a host *file* path that doesn't exist yet (e.g. `-v ./data/nodes.txt:/etc/easy_proxies/nodes.txt`), Docker creates a **directory** named `nodes.txt` on the host and mounts that. The container then sees a directory where it expects a file and fails opaquely. Pre-creating the file (or mapping the **directory** `./data:/etc/easy_proxies`, which auto-generates files) avoids this. If you hit it, run `rm -rf ./data/nodes.txt && touch ./data/nodes.txt` and restart.

### 2. Run with Docker (Recommended)

Config files are auto-generated on first run. The default directory mount reads **`./data/config.yaml`**, not the repository-root `config.yaml`. For a prepared configuration, copy it to `data/config.yaml` before starting; otherwise edit the generated file and configure real nodes before using the proxy.

```bash
mkdir -p data logs
docker run --user $(id -u):$(id -g) \
  -v $(pwd)/data:/etc/easy_proxies \
  -v $(pwd)/logs:/app/logs \
  --network host \
  ghcr.io/jasonwong1991/easy_proxies:latest
```

Or use docker compose:
```bash
docker compose up -d
```

### 3. Run from Source

Use **Go 1.26.10 or newer**; the Docker builder is pinned to Go 1.26.10.

```bash
go run ./cmd/easy_proxies --config config.yaml
```

The default source build excludes optional sing-box features, including the Clash API traffic counters; the management UI still works, but real-time traffic charts need `with_clash_api`. To use the same feature tags as the Docker image:

```bash
go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" \
  ./cmd/easy_proxies --config config.yaml
```

### 4. Access WebUI

Open `http://localhost:9091` in your browser.

## Upgrading to v3.3.0

See the [bilingual release notes](docs/releases/v3.3.0.md) and [full changelog](CHANGELOG.md). Session IDs may now be up to 100 characters, and a named session whose pinned node fails to dial re-routes once and blacklists that node immediately (`pool.blacklist_failed_pin`, default `true`). Pin the image to `ghcr.io/jasonwong1991/easy_proxies:3.3.0` and, after the publication workflow finishes:

```bash
docker compose pull easy_proxies
docker compose up -d --force-recreate easy_proxies
```

## Upgrading to v3.2.0

See the [bilingual release notes](docs/releases/v3.2.0.md) and [full changelog](CHANGELOG.md). Back up your mounted configuration directory and retain `node_ports.json` before upgrading. To pin the release, set the Compose image to `ghcr.io/jasonwong1991/easy_proxies:3.2.0`; after its publication workflow finishes, run:

```bash
docker compose pull easy_proxies
docker compose up -d --force-recreate easy_proxies
```

Recreating the container clears in-memory named sessions, but saved per-node ports survive. A management password change takes effect immediately and requires a new login. Directory mounts provide atomic configuration replacement; individual-file bind mounts remain compatible through a locked in-place fallback, without the same crash/power-loss atomicity guarantees. Source builds need Go 1.26.10+ and the production feature tags for optional protocols/traffic statistics.

## Configuration

### Runtime Modes

| Mode | Description |
|------|-------------|
| `pool` | Single port proxy pool. All nodes share one port with load balancing |
| `multi-port` | One local port per node for direct access |
| `hybrid` | Both pool + multi-port simultaneously |

### Pool Scheduling

| Algorithm | Description |
|-----------|-------------|
| `sequential` | Round-robin through healthy nodes |
| `random` | Random node selection |
| `balance` | Least-connections balancing |
| `latency` | Pick the node with the lowest measured latency |

### Minimal Config Example

```yaml
mode: pool

listener:
  address: 0.0.0.0
  port: 2323
  username: user
  password: pass

pool:
  mode: sequential    # sequential / random / balance / latency
  failure_threshold: 3
  blacklist_duration: 24h
  transient_cooldown: 60s   # cooldown after timeout / connection reset / 503
  rate_limit_cooldown: 60s  # cooldown after HTTP 429; set e.g. 2h to avoid the node longer
  retry_enabled: true # retry on another node when a dial fails
  retry_attempts: 3   # max total dial attempts per request
  blacklist_failed_pin: true  # blacklist a session pin's node immediately when its dial fails (429/timeout keep their cooldown)

management:
  enabled: true
  listen: 0.0.0.0:9091
  probe_target: http://cp.cloudflare.com/generate_204
  password: ""

dns:
  server: 223.5.5.5
  port: 53
  strategy: prefer_ipv4

nodes_file: nodes.txt
```

### Session Routing (pool/hybrid mode)

The main HTTP/SOCKS5 entry supports both ordinary pool scheduling and fixed-node sessions without another port. Configure a non-empty listener username and use its existing password; a strong, non-empty password is recommended:

```yaml
listener:
  address: 0.0.0.0
  port: 2323
  username: wang
  password: "1234"  # example only; choose your own password

sticky:
  session_enabled: true # defaults to true when omitted
  session_ttl: 24h      # release bindings after this much idle time
  max_sessions: 10000   # bound session memory usage
  enabled: false       # legacy source-IP sticky port; independent of session_enabled
  port: 2324
```

| Proxy URL | Routing |
| --- | --- |
| `http://wang:1234@127.0.0.1:2323` | Ordinary pool scheduling |
| `http://wang.account01:1234@127.0.0.1:2323` | Fixed upstream node for session `account01` |
| `socks5h://wang.account01:1234@127.0.0.1:2323` | The same session over SOCKS5, with proxy-side DNS |

Session IDs are chosen by the client: **1–100 characters matching `[A-Za-z0-9_-]`**. No registration is needed. Reuse an ID to reuse its node; use another ID for a separate binding. Different IDs can still select the same node. The password remains the listener password; the session suffix is not a password or a separate account.

```bash
# Same session across new HTTP/HTTPS connections.
curl --proxy 'http://wang.account01:1234@127.0.0.1:2323' https://example.com

# The same session over SOCKS5.
curl --proxy 'socks5h://wang.account01:1234@127.0.0.1:2323' https://example.com
```

An unavailable, cooling-down, blacklisted, removed, or dial-failing bound node is replaced according to the pool's availability and retry policy. The new node becomes the session binding. A dial that fails through the bound node itself always buys one extra selection in that request, even with `retry_enabled: false`, because a dead pin is a binding error rather than a scheduling retry; if that attempt fails too the binding is kept so a single blip does not silently move the session to another exit. That node is also blacklisted on the spot — transport faults for `pool.blacklist_duration` (default 24h) instead of waiting for `pool.failure_threshold`, while 429/timeout failures only take their configured cooldown — so other sessions are no longer routed to it. This fixes the **upstream node**, not the node provider's public exit IP. Established tunnels are not migrated, and HTTP application errors do not cause replay of requests already sent.

The fixed binding covers TCP connections (HTTP, CONNECT, and SOCKS5 CONNECT). SOCKS5 UDP can reuse a compatible TCP-bound node, but falls back for that request if necessary without changing the TCP binding; a fixed UDP exit is not guaranteed.

Bindings survive subscription refreshes and core reloads when the same upstream URI identity remains. They expire after the configured idle TTL and may be released when the session capacity is exceeded; a process restart clears all bindings. Idle time is renewed only by successful dials: failed attempts do not extend a binding, and traffic on an already-open tunnel does not either. Session routing requires `pool`/`hybrid` mode and a configured listener username. Set `session_enabled: false` to retain exact-username authentication only.

#### Legacy source-IP sticky port

`sticky.enabled: true` still opens the separate legacy entry (default `listener.port + 1`, usually `2324`). Its address and credentials reuse `listener`, and it pins the **client source IP** rather than a username session. For example, `http://wang:1234@127.0.0.1:2324` retains the old behavior. Multiple clients behind the same NAT share that IP-based binding. This option is independent of main-port session routing; `session_ttl` and `max_sessions` do not change the legacy IP binding lifecycle.

See the [session routing guide (Chinese)](docs/session-routing.md) for lifecycle, compatibility, and troubleshooting details.

### Full Config Reference

See [config.example.yaml](config.example.yaml) for the full documented configuration with all available options.

## GeoIP Region Routing

### Overview

When GeoIP is enabled, Easy Proxies automatically classifies your proxy nodes by geographic region and provides a separate HTTP proxy endpoint that lets you route traffic through nodes in a specific country/region.

### Supported Regions

| Code | Region |
|------|--------|
| `jp` | Japan 🇯🇵 |
| `kr` | South Korea 🇰🇷 |
| `us` | United States 🇺🇸 |
| `hk` | Hong Kong 🇭🇰 |
| `tw` | Taiwan 🇹🇼 |
| `sg` | Singapore 🇸🇬 |
| `other` | All other regions |

### Configuration

```yaml
geoip:
  enabled: true
  database_path: "./GeoLite2-Country.mmdb"
  listen: "0.0.0.0"          # defaults to listener.address if omitted
  port: 1221                  # defaults to listener.port if omitted
  auto_update_enabled: true   # auto-update the GeoIP database
  auto_update_interval: 24h   # check interval
```

The GeoIP router reuses the `listener.username` and `listener.password` for proxy authentication.

Key behaviors:
- The GeoIP database (MaxMind GeoLite2-Country) is **auto-downloaded** on first startup
- Auto-update is enabled by default (checks every 24h) with hot-reload -- no restart needed
- Node region classification happens automatically during startup and on every reload
- Nodes whose IP cannot be resolved or looked up are placed in the `other` category

### How to Use

The GeoIP router is an HTTP proxy that listens on its own port. Select a
region with the `X-Easyproxy-Region` proxy request header. This works for
both regular HTTP requests and HTTPS CONNECT tunnels without changing the
target URL.

```bash
# Route through Japanese nodes.
curl --proxy http://user:pass@localhost:1221 \
  --proxy-header 'X-Easyproxy-Region: jp' \
  https://www.google.com

# Route through US nodes.
curl --proxy http://user:pass@localhost:1221 \
  --proxy-header 'X-Easyproxy-Region: us' \
  http://example.com

# No header uses the global pool.
curl --proxy http://user:pass@localhost:1221 https://www.google.com
```

Supported values are `jp`, `kr`, `us`, `hk`, `tw`, `sg`, and `other`.
Unsupported values return `400 Bad Request`. The header is removed before a
regular HTTP request is forwarded to the target server.

#### Legacy Path Fallback

Path-based region selection remains available for clients that actually send
the path in the proxy request, including `CONNECT jp/example.com:443`.
Standard HTTP proxy clients do not transmit a path included in the proxy URL,
so a proxy URL such as `http://localhost:1221/jp/` cannot select a region.
Use a proxy-specific header facility instead.

### How It Works

1. On startup, each node's server IP is resolved and looked up in the MaxMind GeoLite2-Country database
2. Nodes are grouped into per-region pools (`pool-jp`, `pool-kr`, `pool-us`, etc.) with independent health checking
3. The GeoIP router listens on its own port and selects a region from a proxy request header
4. Matching requests are routed through the corresponding region pool; unmatched requests use the global pool
5. Each region pool uses the same scheduling algorithm configured in the `pool` section
6. DNS lookup results are cached to avoid repeated resolution on reload

## Supported Protocols

| Protocol | URI Schemes | Transport |
|----------|-------------|-----------|
| VLESS | `vless://` | TCP, WS, HTTP/2, gRPC, HTTPUpgrade; TLS/Reality/uTLS |
| VMess | `vmess://` | WS, HTTP/2, gRPC, HTTPUpgrade; TLS/uTLS |
| Trojan | `trojan://` | WS, HTTP/2, gRPC, HTTPUpgrade; TLS/Reality/uTLS |
| Shadowsocks | `ss://`, `shadowsocks://` | Direct; SIP002 and legacy whole-payload Base64 formats |
| Hysteria2 | `hysteria2://`, `hy2://` | QUIC-based |
| TUIC | `tuic://` | QUIC-based |
| AnyTLS | `anytls://` | TLS |
| SOCKS5 | `socks5://`, `socks5h://`, `socks://` | Direct |
| HTTP | `http://`, `https://` | Direct |

## Node Sources

### Inline Nodes

```yaml
nodes:
  - uri: "vless://uuid@server:443?security=tls&type=ws&path=/path#Name"
  - uri: "ss://base64(method:password@server:port)#Name"
```

### Nodes File

```yaml
nodes_file: nodes.txt
```

One proxy URI per line. Lines starting with `#` are comments.

### Subscriptions

```yaml
subscriptions:
  - "https://provider.example/api?token=xxx"

subscription_refresh:
  enabled: true
  interval: 1h
  timeout: 30s
  fetch_concurrency: 16
```

Supports Base64, plain text, and Clash YAML formats. When subscriptions are configured, fetched nodes are written to `nodes_file`. Subscription changes trigger automatic hot-reload without restart.

**Multi-source Node Merging**: When both `nodes` and `subscriptions` are configured:
- Inline nodes (defined in `nodes` array) and subscription nodes are merged together
- Subscription updates preserve inline nodes instead of overwriting them
- Node order: inline nodes first, followed by subscription nodes
- Each node's source (inline/subscription) is tracked and displayed in the management UI

**Stable Ports** (`multi-port`/`hybrid`): each node is identified by a stable key derived from its URI (ignoring the display name and parameter order), so a node keeps the same local port even when the subscription renames or reorders it. Assignments are saved to `node_ports.json` next to `config.yaml` and restored on restart.

For encoded VMess JSON links, the display-name `ps` field, top-level JSON key ordering, and supported base64 encoding variants are also excluded from identity differences. Older port-map keys are automatically normalized on load, preserving existing assignments when upgrading; do not delete `node_ports.json` just to adopt the new identity format.

## WebUI Dashboard

Access at `http://your-server:9091` (configurable via the `management` section).

Features:

- **Dashboard**: Proxy connection shortcuts, real-time node status, traffic charts, region availability, latency monitoring, connectivity probes, and node exports; node search and status filters
- **Session Routing**: Generate a session ID and HTTP/SOCKS5 URL, copy the full URL or curl command, inspect bindings, and reset one or all named sessions. Previews and startup logs mask passwords; copied URLs contain the actual proxy credentials
- **Node Config**: Add/edit/delete inline nodes, with search and on-demand 300-row batches
- **Diagnostics**: Node statistics and probe timelines, with search and on-demand 300-row batches
- **Console**: Application logs from the bounded in-memory buffer; polling pauses when the tab is hidden
- **Settings**: Edit common runtime settings and subscription URLs from the browser; changes persist to `config.yaml`

The dashboard and session page adapt to mobile screens. Current node health follows the backend availability flags rather than historical failure totals; failed operations display errors instead of reporting success.

When `management.password` is empty, authentication is bypassed.

Changing the management password takes effect immediately and invalidates existing login sessions. Sign in again before applying other pending changes. Most proxy/runtime settings still require a reload, which interrupts existing connections. Manual subscription refresh remains available when periodic refresh is disabled; a failed/empty provider keeps the previous subscription set and reports an error.

## Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth` | GET, POST, DELETE | Authentication status, login, logout |
| `/api/settings` | GET, PUT | Read/update settings |
| `/api/nodes` | GET | List all nodes with status |
| `/api/nodes/{tag}/probe` | POST | Test node connectivity |
| `/api/nodes/{tag}/blacklist` | POST | Manually blacklist a node |
| `/api/nodes/{tag}/release` | POST | Release node from blacklist |
| `/api/nodes/probe-all` | POST | Probe all nodes (SSE stream) |
| `/api/export` | GET | Export local proxy URLs; healthy nodes by default, `all=true` for all nodes, `scheme=http/socks5/all` |
| `/api/subscription/config` | GET, PUT | Manage subscription URLs |
| `/api/subscription/status` | GET | Check subscription status |
| `/api/subscription/refresh` | POST | Trigger manual refresh |
| `/api/nodes/config` | GET, POST, PUT, DELETE | CRUD for node config |
| `/api/proxy-sessions` | GET, DELETE | List named-session bindings or reset all bindings |
| `/api/proxy-sessions/{id}` | DELETE | Reset one binding; `id` is the full username, e.g. `wang.account01` |
| `/api/proxy-sessions/links` | POST | Generate an HTTP/SOCKS5 URL from `session_id` and `scheme`, without connecting or binding a node |
| `/api/reload` | POST | Reload sing-box instance |

Exported URLs contain actual proxy credentials. Exports and session-link generation avoid wildcard listener addresses and correctly encode credentials and IPv6 hosts; configure `external_ip` when the client-facing host differs from the management host. Generate fixed-session URLs on the Session Routing page rather than treating node exports as session registrations.

## Docker Deployment

### docker-compose.yml

The default setup uses host networking (recommended for automatic port management). Config directory is auto-generated on first run:

```yaml
services:
  easy_proxies:
    image: ghcr.io/jasonwong1991/easy_proxies:latest
    container_name: easy_proxies
    restart: unless-stopped
    network_mode: host
    user: "${UID:-10001}:${GID:-10001}"
    volumes:
      - ./data:/etc/easy_proxies
      - ./logs:/app/logs
```

### Important Notes

- **Zero-config**: When mapping a directory, `config.yaml` and `nodes.txt` are auto-generated on first run.
- **Permissions**: Use `--user $(id -u):$(id -g)` to match your host user for file access.
- **Multi-platform**: Supports amd64 and arm64 architectures.
- **Reload**: `/api/reload` and subscription refresh will interrupt active connections.

### Ports

| Port | Usage |
|------|-------|
| 2323 | Main HTTP/SOCKS5 pool entry: bare username for normal scheduling, `username.sessionID` for a fixed-node session |
| 9091 | WebUI and Management API |
| 9092 | Internal Clash API (localhost only, traffic stats) |
| 1221 | GeoIP region router (when enabled, configurable) |
| 24000+ | Multi-port mode (one per node) |

In `multi-port`/`hybrid` mode, per-node ports are persisted to `node_ports.json` (next to `config.yaml`) and restored on restart, so each node keeps a stable port across restarts and subscription refreshes. Delete this file to force a clean reassignment.

## Troubleshooting

### Configuration Persistence Issues

**Problem**: Configuration changes made via WebUI are lost after container restart or rebuild.

**Quick Diagnosis**:
```bash
# Check your data directory layout and permissions
ls -la data/
[ -f data/config.yaml ]  || echo "missing data/config.yaml"
[ -d data/config.yaml ]   && echo "BUG: data/config.yaml is a directory (see Quick Start note)"
[ -d data/nodes.txt ]    && echo "BUG: data/nodes.txt is a directory (see Quick Start note)"
```

**Common Causes and Solutions**:

1. **File Permission Issues**:
   ```bash
   # Fix permissions
   chown -R $(id -u):$(id -g) data
   chmod 755 data
   chmod 644 data/config.yaml data/nodes.txt
   ```

2. **Incorrect Volume Mapping**:
   - Ensure `docker-compose.yml` uses `./data:/etc/easy_proxies`
   - Do not use absolute paths or incorrect directories

3. **Missing UID/GID on Startup**:
   ```bash
   # Correct startup command
   UID=$(id -u) GID=$(id -g) docker-compose up -d
   ```

**Verify Configuration Persistence**:
```bash
# Check file modification times
ls -lh data/config.yaml data/nodes.txt

# Check container logs for save confirmations
docker-compose logs -f | grep "Saved"
```

**Detailed Troubleshooting**: See [docs/troubleshooting-persistence.md](docs/troubleshooting-persistence.md)

### Docker Permission Issues

**Problem**: When using `docker-compose.yml` with volume mapping, you may encounter permission errors like "permission denied" or "cannot write to /etc/easy_proxies".

**Root Cause**: The container runs as a non-root user (specified by `user: "${UID:-10001}:${GID:-10001}"` in docker-compose.yml), but the mounted host directory may have different ownership.

**Solutions**:

1. **Use docker compose with a directory mount (Recommended)**:
   ```bash
   mkdir -p data logs
   sudo chown -R $(id -u):$(id -g) data logs
   docker compose up -d
   ```
   This maps the whole `./data` directory; `config.yaml` and `nodes.txt` are auto-generated as files on first run.

2. **Pre-create config files** (alternative, for file-level mounts):
   ```bash
   mkdir -p data
   cp config.example.yaml data/config.yaml
   touch data/nodes.txt
   chown -R $(id -u):$(id -g) data
   docker compose up -d
   ```

**For docker run command**:
```bash
mkdir -p data logs
chmod -R u+w data logs
docker run --user $(id -u):$(id -g) \
  -v $(pwd)/data:/etc/easy_proxies \
  -v $(pwd)/logs:/app/logs \
  --network host \
  ghcr.io/jasonwong1991/easy_proxies:latest
```

### Other Common Issues

- **"Config file not found"**: Ensure `config.yaml` exists in the mounted directory
- **"Cannot bind port"**: Check if the port is already in use by another service
- **"All nodes failed health check"**: Verify your proxy URIs are correct and the upstream servers are reachable
- **"Proxy was working but suddenly stopped"**: Check if the node is blacklisted (appears after 3 consecutive failures, default duration: 24h)
  - **Solution 1**: Release via WebUI - click the "Release" button next to the node
  - **Solution 2**: Release via API - `POST http://localhost:9091/api/nodes/{tag}/release`
  - **Solution 3**: Reduce blacklist duration in `config.yaml`:
    ```yaml
    pool:
      blacklist_duration: 1h  # Change from default 24h to 1h
    ```
  - Check logs for blacklist events: `docker compose logs | grep "BLACKLISTED"`
  - **Note**: transient failures (HTTP 429, timeouts, connection resets, 503) never reach the 24h blacklist at all -- they do not count toward `failure_threshold` and instead apply a short cooldown (`transient_cooldown`, default `60s`). If a node is rate-limited often, give 429 its own longer cooldown so the pool skips that node for hours instead of a minute:
    ```yaml
    pool:
      transient_cooldown: 60s   # timeout / connection reset / 503
      rate_limit_cooldown: 2h   # HTTP 429 -- avoid this node for 2 hours
    ```
    - A node on cooldown is skipped, not blacklisted -- it rejoins the pool automatically once the cooldown expires, with no manual action needed.
    - If *every* node in a pool is cooling down at once, requests fail until the earliest cooldown expires, rather than falling back to a node known to be rate-limited. Choose `rate_limit_cooldown` with your pool size in mind -- a long value on a small pool means a real outage window.
    - Manual release (the WebUI 解封 button, or `POST /api/nodes/{tag}/release`) clears a live cooldown immediately if you need the node back right away.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Development

Use **Go 1.26.10 or newer** (see `go.mod`) and Node.js 24 for fixture tests. The Docker builder is pinned to Go 1.26.10. The publication workflow requires the checks below before building/publishing images; actual run results are recorded in GitHub Actions:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
FEATURE_TAGS='with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api'
go test -count=1 -tags "$FEATURE_TAGS" ./...
sh -n entrypoint.sh
node --test scripts/test-ui.mjs scripts/test-entrypoint.mjs
```

For changes affecting runtime behavior, add loopback/temporary-directory regressions rather than testing against live subscriptions. Optional browser tests are in `scripts/test-ui-browser.mjs` (Playwright and Chrome required).

See the [project review and iteration roadmap](docs/project-review-2026-10-11.md) for completed fixes, benchmark scope, upgrade notes, and remaining work. Directory mounts remain recommended for atomic configuration replacement; individual-file mounts use a locked in-place fallback with weaker crash guarantees.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=jasonwong1991/easy_proxies&type=Date)](https://star-history.com/#jasonwong1991/easy_proxies&Date)

## Acknowledgements

This project is built on top of [sing-box](https://github.com/SagerNet/sing-box) — a universal proxy platform that powers all the underlying protocol implementations and transports. Huge thanks to the SagerNet team and contributors for their outstanding work.

## License

MIT License
