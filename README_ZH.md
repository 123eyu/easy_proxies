# Easy Proxies

[English](README.md) | 简体中文

Easy Proxies 是一个基于 sing-box 的代理池管理工具。

目标是把大量上游节点统一成稳定的本地 HTTP/SOCKS5 代理入口，同时支持按节点独立端口访问。

## 当前能力

- 运行模式：`pool`、`multi-port`、`hybrid`。
- 实际构建的上游协议：`vmess`、`vless`、`trojan`、`ss/shadowsocks`、`hysteria2/hy2`、`socks5/socks`、`http/https`、`anytls`、`tuic`。
- 节点来源：
  - `config.yaml` 的 `nodes`
  - `nodes_file`（每行一个 URI）
  - `subscriptions`（支持 Base64/纯文本/Clash YAML 解析）
- 自动健康检查、失败熔断和黑名单恢复；暂时性故障（超时/连接重置/503）与限流故障（HTTP 429）分别使用可配置的独立冷却时间。
- 拨号失败自动重试：节点拨号失败时自动切换到另一个健康节点重试（可配置次数）。
- 端口稳定（multi-port/hybrid）：每个节点按 URI 稳定标识（忽略名称与参数顺序），订阅刷新或重启后保持同一本地端口（持久化到 config.yaml 同目录的 `node_ports.json`）。
- 主端口会话路由：使用 `用户名.sessionID` 在多次连接间固定上游节点，节点失效时切换；普通用户名沿用原池调度。旧版按来源 IP 绑定的独立粘性端口继续兼容（仅 pool/hybrid 模式）。
- Web 管理面板 + API：
  - 代理接入快捷卡片、节点搜索/状态筛选、探测/导出
  - **会话路由页**：生成会话 ID、复制 HTTP/SOCKS5 URL 或 curl、查看绑定与重置会话；预览隐藏密码，复制的完整地址含真实认证信息
  - **手动拉黑/解封节点**
  - 动态设置（`external_ip`、`probe_target`、`skip_cert_verify`、`geoip`）
  - 节点配置增删改查 + 重载
  - 订阅状态查询 + 手动刷新 + **保存即时生效**
  - **实时日志控制台**（有界内存日志缓冲区，标签页隐藏时暂停轮询）
- 面板支持移动端布局；节点当前状态以服务端可用性为准，历史失败次数不等于当前异常，操作失败会展示实际错误。
- 新增可配置 DNS 解析器（对 VMess 域名节点非常关键）。
- 可选 GeoIP 标记（支持 JP/KR/US/HK/TW/SG 地域分区，可在 WebUI 中开关，支持自动更新和热重载）。
- **可配置日志轮转**，支持大小限制、备份数量和压缩。

## 快速开始

### 1）准备配置

```bash
cp config.example.yaml config.yaml
touch nodes.txt
```

编辑 `config.yaml`，并配置节点来源（`nodes.txt` / `subscriptions` / `nodes`）。默认的 `vless://uuid@server:443` 只是格式示例，请先替换成真实节点；使用订阅或节点文件时，配置好实际来源后再删除该内联示例，避免产生 `lookup server: NXDOMAIN` 探测噪声。

> 为什么要 `touch nodes.txt`？如果你用文件级挂载（如 `-v ./data/nodes.txt:/etc/easy_proxies/nodes.txt`）而宿主机上该文件不存在，Docker 会在宿主机上创建一个名为 `nodes.txt` 的**目录**并挂载进去，容器内就出现"本应是文件却是目录"的坑。预先创建文件（或直接挂载**目录** `./data:/etc/easy_proxies`，首启动会自动生成文件）可避免。若已踩坑：`rm -rf ./data/nodes.txt && touch ./data/nodes.txt` 后重启。

### 2）启动

Docker 默认挂载 **`./data`** 目录，实际读取的是 `data/config.yaml`，不是仓库根目录的 `config.yaml`。已准备好配置时，先复制到 `data/config.yaml` 再启动；首次启动也可自动生成配置文件，随后编辑该文件、配置真实节点后再使用代理。

```bash
docker compose up -d
```

本地运行（需 **Go 1.26.10 或更新版本**，Docker builder 固定为 Go 1.26.10）：

```bash
go run ./cmd/easy_proxies -config config.yaml
```

普通源码构建不包含可选的 sing-box 功能，包括 Clash API 流量统计；管理面板仍可使用，但实时流量图需要 `with_clash_api`。需要与 Docker 镜像相同的功能标签时使用：

```bash
go run -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" \
  ./cmd/easy_proxies -config config.yaml
```

## 升级到 v3.3.0

详见 [中英双语发布说明](docs/releases/v3.3.0.md) 和 [完整更新日志](CHANGELOG.md)。会话 ID 上限提升到 100 位；命名会话绑定的节点拨号失败时，本次请求会自动换节点重试，并立即拉黑该节点（`pool.blacklist_failed_pin`，默认 `true`）。固定版本部署时，将 Compose 镜像设为 `ghcr.io/jasonwong1991/easy_proxies:3.3.0`；该版本镜像发布工作流完成后执行：

```bash
docker compose pull easy_proxies
docker compose up -d --force-recreate easy_proxies
```

## 升级到 v3.2.0

详见 [中英双语发布说明](docs/releases/v3.2.0.md) 和 [完整更新日志](CHANGELOG.md)。升级前备份挂载的配置目录，并保留 `node_ports.json`。固定版本部署时，将 Compose 镜像设为 `ghcr.io/jasonwong1991/easy_proxies:3.2.0`；该版本镜像发布工作流完成后执行：

```bash
docker compose pull easy_proxies
docker compose up -d --force-recreate easy_proxies
```

重建容器会清空内存中的命名会话，已保存的节点端口仍保留。修改管理密码立即生效，需要重新登录。推荐目录挂载以获得配置原子替换；单文件 bind mount 继续使用锁内原地写兼容路径，但不具备相同的断电/崩溃原子性保障。源码构建需 Go 1.26.10+；可选协议/流量统计还需完整生产标签。

## 最小配置示例（Pool）

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
  transient_cooldown: 60s   # 暂时性故障（超时/连接重置/503）后的冷却时间
  rate_limit_cooldown: 60s  # HTTP 429 限流后的冷却时间，可设为 2h 以更长时间避开该节点
  retry_enabled: true # 拨号失败时切换到另一节点重试
  retry_attempts: 3   # 每个请求的最大拨号次数
  blacklist_failed_pin: true  # 命名会话绑定的节点拨号失败时立即拉黑（429/超时类仍只冷却）

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

## GeoIP 地区路由

启用 GeoIP 后，Easy Proxies 会按节点的地理位置自动分类，并提供独立
HTTP 代理入口，以便通过指定地区的节点转发流量。

### 支持的地区

| 代码 | 地区 |
| --- | --- |
| `jp` | 日本 |
| `kr` | 韩国 |
| `us` | 美国 |
| `hk` | 香港 |
| `tw` | 台湾 |
| `sg` | 新加坡 |
| `other` | 其他地区 |

### 配置

```yaml
geoip:
  enabled: true
  database_path: "./GeoLite2-Country.mmdb"
  listen: "0.0.0.0"          # 省略时使用 listener.address
  port: 1221                  # 省略时使用 listener.port
  auto_update_enabled: true   # 自动更新 GeoIP 数据库
  auto_update_interval: 24h   # 检查间隔
```

GeoIP 路由器复用 `listener.username` 和 `listener.password` 进行代理认证。

- 首次启动时会自动下载 MaxMind GeoLite2-Country 数据库。
- 默认每 24 小时检查更新，并热重载数据库，无需重启。
- 启动和每次重载时都会自动重新分类节点。
- 无法解析或查询 IP 归属地的节点会进入 `other` 分类。

### 使用方法

GeoIP 路由器是独立端口上的 HTTP 代理。使用代理专用请求头
`X-Easyproxy-Region` 选择地区；普通 HTTP 请求和 HTTPS `CONNECT`
隧道都支持，且无需修改目标 URL。

```bash
# 通过日本节点访问 HTTPS 目标。
curl --proxy http://user:pass@localhost:1221 \
  --proxy-header 'X-Easyproxy-Region: jp' \
  https://www.google.com

# 通过美国节点访问 HTTP 目标。
curl --proxy http://user:pass@localhost:1221 \
  --proxy-header 'X-Easyproxy-Region: us' \
  http://example.com

# 不设置请求头时使用全局节点池。
curl --proxy http://user:pass@localhost:1221 https://www.google.com
```

支持的值为 `jp`、`kr`、`us`、`hk`、`tw`、`sg` 和 `other`。不支持的值会
返回 `400 Bad Request`。转发普通 HTTP 请求前会删除该内部请求头，不会将它
发送给目标服务器。

#### 旧路径格式兼容

仍保留实际代理请求中携带路径时的地区选择，以及
`CONNECT jp/example.com:443` 的旧格式兼容。标准 HTTP 代理客户端通常不会
把代理地址中的路径发送给代理服务，因此 `http://localhost:1221/jp/`
不能可靠地选择地区，应改用代理专用请求头功能。

## 主端口会话路由（仅 Pool/Hybrid 模式）

同一个 HTTP/SOCKS5 主入口（默认 `2323`）同时支持普通调度和固定会话，不需要额外端口。先为 listener 配置非空用户名，认证使用其原有密码；建议设置非空强密码：

```yaml
listener:
  address: 0.0.0.0
  port: 2323
  username: wang
  password: "1234"  # 示例密码，请自行修改

sticky:
  session_enabled: true # 省略时默认开启
  session_ttl: 24h      # 绑定空闲超过此时间后释放
  max_sessions: 10000   # 会话容量上限，限制内存占用
  enabled: false       # 旧来源 IP 粘性端口，与 session_enabled 相互独立
  port: 2324
```

| 代理地址 | 行为 |
| --- | --- |
| `http://wang:1234@127.0.0.1:2323` | 普通池调度，沿用已有调度模式 |
| `http://wang.account01:1234@127.0.0.1:2323` | 会话 `account01` 固定同一上游节点 |
| `socks5h://wang.account01:1234@127.0.0.1:2323` | SOCKS5 使用同一会话，由代理解析目标域名 |

会话 ID 由客户端自定义，无需提前注册，支持 **1–100 个 `[A-Za-z0-9_-]` 字符**。同一 ID 复用同一绑定，换一个 ID 就建立独立绑定，但不同 ID 不保证分配不同节点。密码仍是 listener 密码；后缀不是新账号或认证密码。

```bash
# 多次新建连接使用同一会话。
curl --proxy 'http://wang.account01:1234@127.0.0.1:2323' https://example.com

# HTTP 与 SOCKS5 可共用相同会话。
curl --proxy 'socks5h://wang.account01:1234@127.0.0.1:2323' https://example.com
```

- 绑定节点不可用、进入冷却/黑名单、被移除或拨号失败时，按池可用性与重试策略切换，并保留新绑定。绑定的节点自身拨号失败时，本次请求会额外重新选一次节点（即使 `retry_enabled: false`），成功后迁移绑定；这一次也失败则保留原绑定，避免一次抖动就更换出口。该节点同时立即拉黑（真实传输故障按 `pool.blacklist_duration`，默认 24h；429/超时类只按冷却处理），其它会话不再被分配到它。
- 固定的是**上游节点**，不保证节点提供方的公网出口 IP 永久不变。已经建立的连接不迁移，已发送请求不会因 HTTP 业务错误自动重放。
- 固定绑定针对 TCP（HTTP、CONNECT、SOCKS5 CONNECT）。SOCKS5 UDP 可优先复用支持 UDP 的绑定节点；需要回退时只影响该请求，不改 TCP 绑定，不保证 UDP 固定出口。
- 订阅刷新与核心重载时，仍存在的同一 URI 稳定身份可保留绑定；空闲超过 TTL 或触及容量清理时可释放绑定。空闲时间只由**成功拨号**续期，失败连接不续期，长连接中的持续流量也不续期。**进程重启会清空会话**。
- 该功能要求 `pool`/`hybrid` 模式且 listener 已配置非空用户名。设置 `session_enabled: false` 可恢复仅接受完整普通用户名的认证方式。

### 旧来源 IP 粘性端口兼容

`sticky.enabled: true` 仍额外监听旧版粘性端口（默认 `listener.port + 1`，通常为 `2324`），监听地址与认证复用 listener。使用 `http://wang:1234@127.0.0.1:2324` 时，仍按**来源 IP** 固定上游节点，不使用用户名后缀识别会话；同一 NAT 出口的客户端会共享这类绑定。它与主端口会话功能相互独立，`session_ttl` 和 `max_sessions` 不改变旧 IP 绑定的生命周期。

完整的生命周期、兼容性和排错说明见 [会话路由指南](docs/session-routing.md)。

## DNS 配置说明

`dns` 会同时影响 sing-box DNS 客户端和 VMess 域名拨号解析：

```yaml
dns:
  server: 223.5.5.5
  fallback_servers:    # 备用 DNS 服务器（主 DNS 解析失败时使用）
    - 8.8.8.8
    - 1.1.1.1
  port: 53
  strategy: prefer_ipv4
```

`strategy` 可选值：

- `as_is`
- `prefer_ipv4`
- `prefer_ipv6`
- `ipv4_only`
- `ipv6_only`

如果日志中出现 `lookup <domain>: empty result`，请优先检查该 DNS 配置是否可达且策略合理。

## 运行模式

- `pool`：所有节点共享一个本地 HTTP/SOCKS5 入口。
- `multi-port`：每个节点一个独立本地 HTTP/SOCKS5 端口。
- `hybrid`：同时启用 pool + multi-port。

## 节点来源行为

- 配置了 `subscriptions` 时：
  - 会抓取订阅节点并追加到运行节点列表
  - `nodes_file` 作为订阅节点写入路径
  - 启动阶段不再从 `nodes_file` 读取节点
  - 可通过 `subscription_refresh.fetch_concurrency` 调整订阅抓取并发数（默认 16，最大 32）
- `nodes`（内联节点）只要存在就会参与运行。
- **多来源节点合并**：当同时配置 `nodes` 和 `subscriptions` 时：
  - 内联节点（config.yaml 中的 `nodes`）和订阅节点会合并使用
  - 订阅更新时会保留内联节点，不会覆盖
  - 节点顺序：内联节点在前，订阅节点在后
  - 各节点的来源标识（inline/subscription）会在管理界面中显示
- **端口稳定**（multi-port/hybrid）：节点按 URI 稳定标识（忽略名称与参数顺序），订阅改名或重排都保持同一本地端口；分配结果保存到 config.yaml 同目录的 `node_ports.json`，重启后自动恢复。删除该文件可强制重新分配。
  - VMess base64 JSON 链接还忽略名称字段 `ps`、顶层 JSON 字段顺序和支持的 base64 编码形式差异。旧版端口映射 key 会在载入时自动归一化并复用既有端口，升级身份格式时不需要删除 `node_ports.json`。

## 协议支持注意事项

运行时真正支持的协议：

- `vmess`
- `vless`
- `trojan`
- `ss` / `shadowsocks`
  - 支持 SIP002：`ss://base64(method:password)@server:port#name`
  - 支持旧格式：`ss://base64(method:password@server:port)#name`
- `hysteria2` / `hy2`
- `socks5` / `socks5h` / `socks`
- `http` / `https`
- `anytls`
- `tuic`

订阅解析阶段可能识别到更多 URI 前缀（兼容输入），但不在上述列表中的协议会在构建阶段被跳过。

## 管理 API（核心）

- `GET|POST|DELETE /api/auth`（查询登录状态 / 登录 / 退出）
- `GET|PUT /api/settings`
- `GET /api/nodes`
- `POST /api/nodes/{tag}/probe`
- `POST /api/nodes/{tag}/release`
- `POST /api/nodes/{tag}/blacklist`
- `POST /api/nodes/probe-all`（SSE）
- `GET /api/export`（默认导出健康节点，`all=true` 导出全部；`scheme=http/socks5/all`）
- `GET|PUT /api/subscription/config`
- `GET|POST /api/subscription/status|refresh`
- `GET|POST|PUT|DELETE /api/nodes/config[...]`
- `GET|DELETE /api/proxy-sessions`（命名会话绑定列表 / 重置全部绑定）
- `DELETE /api/proxy-sessions/{id}`（重置单个绑定；id 为完整用户名，例如 `wang.account01`）
- `POST /api/proxy-sessions/links`（传入 `session_id`、`scheme` 生成 HTTP/SOCKS5 地址，不连接、不预先绑定）
- `POST /api/reload`

`management.password` 为空时，Web/API 不要求登录。

`GET /api/auth` 查询登录状态，`POST /api/auth` 登录，`DELETE /api/auth` 退出。修改管理密码立即生效并撤销已有登录会话，请重新登录后再应用其他待处理设置。节点配置和诊断页支持搜索及每批 300 条按需展开；隐藏标签页时暂停实时流和日志轮询，批量探测忙碌或请求失败会显示具体错误。

导出的本地代理 URL 含真实认证信息。节点导出和会话链接生成均会替换通配监听地址，并正确编码特殊字符凭据和 IPv6 主机；客户端实际访问地址与管理地址不同时，设置 `external_ip`。固定会话地址请在会话路由页生成，节点导出不是会话注册。

## 重要运行说明

- 重载（`/api/reload` 或订阅刷新）会中断现有连接。
- Settings API 会把配置写回 `config.yaml`；部分设置需要重载后才能完全生效。
- 管理密码无需重载；修改后需要重新登录。关闭订阅定时刷新不影响手动刷新；任一订阅源失败或返回空内容时保留原有订阅集合并提示错误。
- 建议挂载配置目录，常规保存使用原子替换并保留文件权限；单文件挂载使用锁内原地写兼容路径，不具备相同的断电/进程崩溃原子性保障。
- 省略项默认值可在 `internal/config/config.go` 中查看。
- 日志轮转通过 `log` 配置段设置；当 `output: file` 时，日志同时写入控制台和文件，并自动轮转。

## 常见问题

### 配置持久化问题

**问题描述**：在 Docker 环境中通过 WebUI 修改配置后，重启或重建容器时配置被重置。

**快速诊断**：
```bash
# 检查 data 目录结构和权限
ls -la data/
[ -f data/config.yaml ]  || echo "缺少 data/config.yaml"
[ -d data/config.yaml ]   && echo "异常：data/config.yaml 是目录（见快速开始说明）"
[ -d data/nodes.txt ]    && echo "异常：data/nodes.txt 是目录（见快速开始说明）"
```

**常见原因和解决方案**：

1. **文件权限问题**：
   ```bash
   # 修复权限
   chown -R $(id -u):$(id -g) data
   chmod 755 data
   chmod 644 data/config.yaml data/nodes.txt
   ```

2. **卷映射错误**：
   - 确保 `docker-compose.yml` 中使用 `./data:/etc/easy_proxies`
   - 不要使用绝对路径或错误的目录

3. **启动时未传递 UID/GID**：
   ```bash
   # 正确的启动方式
   UID=$(id -u) GID=$(id -g) docker-compose up -d
   ```

**验证配置是否保存**：
```bash
# 查看文件修改时间
ls -lh data/config.yaml data/nodes.txt

# 查看容器日志，确认保存成功
docker-compose logs -f | grep "Saved"
```

**详细故障排查**：参见 [docs/troubleshooting-persistence.md](docs/troubleshooting-persistence.md)

### Docker 权限问题

**问题描述**：使用 `docker-compose.yml` 映射配置目录时，可能遇到 "permission denied" 或 "cannot write to /etc/easy_proxies" 等权限错误。

**原因分析**：容器以非 root 用户运行（docker-compose.yml 中指定 `user: "${UID:-10001}:${GID:-10001}"`），但宿主机挂载目录的所有权可能不匹配。

**解决方案**：

1. **使用 docker compose 挂载目录（推荐）**：
   ```bash
   mkdir -p data logs
   sudo chown -R $(id -u):$(id -g) data logs
   docker compose up -d
   ```
   直接挂载整个 `./data` 目录，首启动会自动生成 `config.yaml` 和 `nodes.txt` 文件。

2. **预先创建配置文件**（备选，适用于文件级挂载）：
   ```bash
   mkdir -p data
   cp config.example.yaml data/config.yaml
   touch data/nodes.txt
   chown -R $(id -u):$(id -g) data
   docker compose up -d
   ```

**docker run 命令方式**：
```bash
mkdir -p data logs
chmod -R u+w data logs
docker run --user $(id -u):$(id -g) \
  -v $(pwd)/data:/etc/easy_proxies \
  -v $(pwd)/logs:/app/logs \
  --network host \
  ghcr.io/jasonwong1991/easy_proxies:latest
```

### 其他常见问题

- **"配置文件未找到"**：确保挂载目录中存在 `config.yaml` 文件
- **"无法绑定端口"**：检查端口是否被其他服务占用
- **"所有节点健康检查失败"**：验证代理 URI 格式正确，且上游服务器可达
- **"代理之前正常使用，突然失效"**：检查节点是否被加入黑名单（连续失败 3 次后触发，默认持续 24 小时）
  - **解决方案 1**：通过 WebUI 释放 - 点击节点旁边的"释放"按钮
  - **解决方案 2**：通过 API 释放 - `POST http://localhost:9091/api/nodes/{tag}/release`
  - **解决方案 3**：在 `config.yaml` 中降低黑名单持续时间：
    ```yaml
    pool:
      blacklist_duration: 1h  # 从默认的 24h 改为 1h
    ```
  - 查看黑名单事件日志：`docker compose logs | grep "BLACKLISTED"`
  - **说明**：暂时性故障（HTTP 429、超时、连接重置、503）根本不会触发 24 小时黑名单 —— 它们不计入 `failure_threshold` 熔断次数，只会应用一段较短的冷却时间（`transient_cooldown`，默认 `60s`）。如果某些节点经常被限流，可以给 429 单独设置更长的冷却时间，让节点池在数小时内跳过该节点，而不是仅仅一分钟：
    ```yaml
    pool:
      transient_cooldown: 60s   # 超时 / 连接重置 / 503
      rate_limit_cooldown: 2h   # HTTP 429 限流 —— 2 小时内避开该节点
    ```
    - 处于冷却中的节点只是被跳过，不会进入黑名单 —— 冷却到期后会自动重新加入节点池，无需手动干预。
    - 如果池内**所有**节点同时处于冷却状态，请求会直接失败，直到最早的冷却到期为止，而不会把请求交给已知被限流的节点。因此设置 `rate_limit_cooldown` 时需要考虑节点池规模 —— 在小节点池上配置过长的冷却时间，意味着真实的服务中断窗口。
    - 如果需要立即恢复某个节点，手动释放（WebUI 的「解封」按钮，或 `POST /api/nodes/{tag}/release`）会立即清除冷却状态。

## 更新日志

详见 [CHANGELOG.md](CHANGELOG.md)。

## 开发验证

使用 **Go 1.26.10 或更新版本**（以 `go.mod` 为准），fixture 测试使用 Node.js 24；Docker builder 固定为 Go 1.26.10。发布工作流要求以下检查通过后才构建/发布镜像，实际运行结果以 GitHub Actions 为准：

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
FEATURE_TAGS='with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api'
go test -count=1 -tags "$FEATURE_TAGS" ./...
sh -n entrypoint.sh
node --test scripts/test-ui.mjs scripts/test-entrypoint.mjs
```

运行时修改应使用临时目录、合成节点和本机回环服务补回归测试，避免操作真实订阅。`scripts/test-ui-browser.mjs` 提供可选浏览器验证（需 Playwright 和 Chrome）。

详见 [全面审查报告与迭代路线图](docs/project-review-2026-10-11.md)，包含本轮已解决问题、性能实测、升级注意事项及待办。

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=jasonwong1991/easy_proxies&type=Date)](https://star-history.com/#jasonwong1991/easy_proxies&Date)

## 致谢

本项目基于 [sing-box](https://github.com/SagerNet/sing-box) 构建 —— 底层所有协议实现、传输层与拨号逻辑都由 sing-box 提供。特别感谢 SagerNet 团队及所有贡献者的卓越工作。

## 许可证

MIT License
