package pool

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"
	"easy_proxies/internal/routing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	// Type is the outbound type name exposed to sing-box.
	Type = "pool"
	// Tag is the default outbound tag used by builder.
	Tag = "proxy-pool"

	modeSequential = "sequential"
	modeRandom     = "random"
	modeBalance    = "balance"
	modeLatency    = "latency"
)

// Options controls pool outbound behaviour.
type Options struct {
	Mode              string
	Members           []string
	FailureThreshold  int
	BlacklistDuration time.Duration
	// TransientCooldown is how long a member is parked after a transient failure
	// (timeout / connection reset / 503). Defaults to 60s when <= 0.
	TransientCooldown time.Duration
	// RateLimitCooldown is the cooldown applied on HTTP 429. Quota windows are
	// usually much longer than a network blip, so this is tuned separately.
	// Falls back to the effective TransientCooldown when <= 0.
	RateLimitCooldown time.Duration
	// RetryEnabled toggles automatic fail-over on dial failure.
	RetryEnabled bool
	// BlacklistFailedPin blacklists a node immediately when a named session's
	// pinned dial fails on a permanent fault, instead of waiting for the
	// failure threshold. nil means enabled (the default).
	BlacklistFailedPin *bool
	// RetryAttempts is the maximum total dial attempts (including the first).
	// Multi-member pools pick a different member per retry; single-member pools retry the same member.
	RetryAttempts int
	Metadata      map[string]MemberMeta
	// Sticky pins each client (by source IP) to a single member, only
	// re-selecting when the pinned member becomes unavailable. Pool/hybrid entry only.
	Sticky bool
	// SessionRouting enables explicit named sessions supplied by the inbound.
	// Unnamed requests retain the pool's normal scheduling behaviour.
	SessionRouting bool
}

// MemberMeta carries optional descriptive information for monitoring UI.
type MemberMeta struct {
	Identity      string // Stable opaque node identity, independent of its tag/name.
	Name          string
	URI           string
	Mode          string
	ListenAddress string
	Port          uint16
	Region        string // GeoIP region code: "jp", "kr", "us", "hk", "tw", "other"
	Country       string // Full country name from GeoIP
}

// Register wires the pool outbound into the registry.
func Register(registry *outbound.Registry) {
	outbound.Register[Options](registry, Type, newPool)
}

type memberState struct {
	outbound adapter.Outbound
	tag      string
	entry    *monitor.EntryHandle
	shared   *sharedMemberState
}

type poolOutbound struct {
	outbound.Adapter
	ctx            context.Context
	logger         singlog.ContextLogger
	manager        adapter.OutboundManager
	options        Options
	mode           string
	members        []*memberState
	membersByID    map[string][]*memberState
	mu             sync.Mutex
	rrCounter      atomic.Uint32
	rng            *rand.Rand
	rngMu          sync.Mutex // protects rng for random mode
	monitor        *monitor.Manager
	candidatesPool sync.Pool
	sticky         bool
	stickyMu       sync.Mutex        // protects stickyMap
	stickyMap      map[string]string // sticky key (client source IP) -> member tag
	sessionStore   *routing.Store
}

func newPool(ctx context.Context, _ adapter.Router, logger singlog.ContextLogger, tag string, options Options) (adapter.Outbound, error) {
	if len(options.Members) == 0 {
		return nil, E.New("pool requires at least one member")
	}
	manager := service.FromContext[adapter.OutboundManager](ctx)
	if manager == nil {
		return nil, E.New("missing outbound manager in context")
	}
	monitorMgr := monitor.FromContext(ctx)
	normalized := normalizeOptions(options)
	memberCount := len(normalized.Members)
	p := &poolOutbound{
		Adapter: outbound.NewAdapter(Type, tag, []string{N.NetworkTCP, N.NetworkUDP}, normalized.Members),
		ctx:     ctx,
		logger:  logger,
		manager: manager,
		options: normalized,
		mode:    normalized.Mode,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor: monitorMgr,
		sticky:  normalized.Sticky,
		candidatesPool: sync.Pool{
			New: func() any {
				return make([]*memberState, 0, memberCount)
			},
		},
	}
	if p.sticky {
		p.stickyMap = make(map[string]string)
	}
	if normalized.SessionRouting {
		p.sessionStore = routing.StoreFromContext(ctx)
		if p.sessionStore == nil {
			p.sessionStore = routing.NewStore(0, 0)
		}
	}

	// Register nodes immediately if monitor is available
	if monitorMgr != nil {
		if memberCount > 1 {
			logger.Info("registering ", memberCount, " nodes to monitor")
		}
		registeredCount := 0
		for _, memberTag := range normalized.Members {
			// Acquire shared state for this tag (creates if not exists)
			state := acquireSharedState(memberTag)

			meta := normalized.Metadata[memberTag]
			info := monitor.NodeInfo{
				Tag:           memberTag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := monitorMgr.Register(info)
			if entry != nil {
				// Attach entry to shared state so all pool instances share it
				state.attachEntry(entry)
				registeredCount++
				// Set probe, release, and blacklist functions immediately
				entry.SetRelease(p.makeReleaseByTagFunc(memberTag))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(memberTag))
				if probeFn := p.makeProbeByTagFunc(memberTag); probeFn != nil {
					entry.SetProbe(probeFn)
				}
			} else {
				logger.Warn("failed to register node: ", memberTag)
			}
		}
		if memberCount > 1 {
			logger.Info("registered ", registeredCount, " nodes to monitor")
		}
	} else {
		logger.Warn("monitor manager is nil, skipping node registration")
	}

	// Register this pool outbound in the dialer registry for GeoIP router
	registerDialer(tag, p)

	return p, nil
}

func normalizeOptions(options Options) Options {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 3
	}
	if options.BlacklistDuration <= 0 {
		options.BlacklistDuration = 24 * time.Hour
	}
	if options.TransientCooldown <= 0 {
		options.TransientCooldown = defaultTransientCooldown
	}
	if options.RateLimitCooldown <= 0 {
		options.RateLimitCooldown = options.TransientCooldown
	}
	if options.RetryAttempts <= 0 {
		options.RetryAttempts = 3
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	case modeLatency:
		options.Mode = modeLatency
	default:
		options.Mode = modeSequential
	}
	return options
}

func (p *poolOutbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	p.mu.Lock()
	err := p.initializeMembersLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// Initial health checks are driven centrally by the monitor's probeAllNodes
	// (StartPeriodicHealthCheck on boot, ProbeAllNow on reload), bounded by the
	// configured probe concurrency. A per-pool startup probe re-probed the same
	// nodes and, with one pool per node, required a process-wide semaphore to
	// avoid fd exhaustion; routing all probing through the monitor removes both.
	return nil
}

// initializeMembersLocked must be called with p.mu held
func (p *poolOutbound) initializeMembersLocked() error {
	if len(p.members) > 0 {
		return nil // Already initialized
	}

	members := make([]*memberState, 0, len(p.options.Members))
	for _, tag := range p.options.Members {
		detour, loaded := p.manager.Outbound(tag)
		if !loaded {
			return E.New("pool member not found: ", tag)
		}

		// Acquire shared state (creates if not exists, reuses if already created)
		state := acquireSharedState(tag)

		member := &memberState{
			outbound: detour,
			tag:      tag,
			shared:   state,
			entry:    state.entryHandle(),
		}

		// Connect to existing monitor entry if available
		if p.monitor != nil {
			meta := p.options.Metadata[tag]
			info := monitor.NodeInfo{
				Tag:           tag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := p.monitor.Register(info)
			if entry != nil {
				state.attachEntry(entry)
				member.entry = entry
				entry.SetRelease(p.makeReleaseFunc(member))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(member.tag))
				if probe := p.makeProbeFunc(member); probe != nil {
					entry.SetProbe(probe)
				}
			}
		}
		members = append(members, member)
	}
	p.members = members
	p.indexMembersLocked()
	p.logger.Info("pool initialized with ", len(members), " members")

	return nil
}

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	lease, binding, err := p.acquireSession(ctx)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		defer lease.Release()
	}
	maxAttempts := p.maxAttempts()
	stickyKey := p.stickyKeyFromCtx(ctx)
	if lease != nil {
		stickyKey = ""
	}
	singleMember := len(p.options.Members) <= 1
	var tried map[string]bool
	if !singleMember && maxAttempts > 1 {
		tried = make(map[string]bool, maxAttempts)
	}
	var lastErr error
	pinned := binding.NodeID
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var member *memberState
		if lease != nil {
			member, err = p.pickMemberForSession(network, tried, pinned)
		} else {
			member, err = p.pickMemberFiltered(network, tried, stickyKey)
		}
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w (after %d attempt(s); last: %v)", err, attempt-1, lastErr)
			}
			return nil, err
		}
		p.incActive(member)
		conn, dialErr := member.outbound.DialContext(ctx, network, destination)
		if dialErr != nil {
			p.decActive(member)
			// A caller cancellation is not evidence of an unhealthy upstream.
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if lease != nil && pinned != "" && p.memberIdentity(member) == pinned {
				p.recordPinFailure(member, dialErr)
			} else {
				p.recordFailure(member, dialErr)
			}
			lastErr = dialErr
			if tried != nil {
				tried[member.tag] = true
			}
			if lease != nil {
				p.dropFailedPin(&pinned, &tried, &maxAttempts, attempt, member, singleMember)
			}
			if attempt < maxAttempts {
				p.logger.Warn("dial via ", member.tag, " failed (attempt ", attempt, "/", maxAttempts, "), retrying: ", dialErr)
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}
		if err := ctx.Err(); err != nil {
			_ = conn.Close()
			p.decActive(member)
			return nil, err
		}
		if attempt > 1 {
			p.logger.Info("dial succeeded via ", member.tag, " after ", attempt, " attempts")
		}
		p.recordSuccess(member)
		if lease != nil && network == N.NetworkTCP {
			meta := p.options.Metadata[member.tag]
			reason := "node_unavailable"
			if lastErr != nil {
				reason = "dial_failed"
			}
			lease.Bind(p.memberIdentity(member), member.tag, meta.Name, reason)
		}
		return p.wrapConn(conn, member), nil
	}
	if lastErr == nil {
		lastErr = E.New("no healthy proxy available")
	}
	return nil, fmt.Errorf("dial failed after %d attempts: %w", maxAttempts, lastErr)
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	lease, binding, err := p.acquireSession(ctx)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		defer lease.Release()
	}
	maxAttempts := p.maxAttempts()
	stickyKey := p.stickyKeyFromCtx(ctx)
	if lease != nil {
		stickyKey = ""
	}
	singleMember := len(p.options.Members) <= 1
	var tried map[string]bool
	if !singleMember && maxAttempts > 1 {
		tried = make(map[string]bool, maxAttempts)
	}
	var lastErr error
	pinned := binding.NodeID
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var member *memberState
		if lease != nil {
			// UDP may use the TCP pin if supported, but capability filtering and
			// packet retries must never migrate the primary TCP binding.
			member, err = p.pickMemberForSession(N.NetworkUDP, tried, pinned)
		} else {
			member, err = p.pickMemberFiltered(N.NetworkUDP, tried, stickyKey)
		}
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w (after %d attempt(s); last: %v)", err, attempt-1, lastErr)
			}
			return nil, err
		}
		p.incActive(member)
		conn, listenErr := member.outbound.ListenPacket(ctx, destination)
		if listenErr != nil {
			p.decActive(member)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// The pin is a TCP binding reused opportunistically for UDP, so a
			// packet failure is not evidence that its TCP dial path is dead and
			// must not blacklist the node.
			p.recordFailure(member, listenErr)
			lastErr = listenErr
			if tried != nil {
				tried[member.tag] = true
			}
			if lease != nil {
				p.dropFailedPin(&pinned, &tried, &maxAttempts, attempt, member, singleMember)
			}
			if attempt < maxAttempts {
				p.logger.Warn("listen-packet via ", member.tag, " failed (attempt ", attempt, "/", maxAttempts, "), retrying: ", listenErr)
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}
		if err := ctx.Err(); err != nil {
			_ = conn.Close()
			p.decActive(member)
			return nil, err
		}
		if attempt > 1 {
			p.logger.Info("listen-packet succeeded via ", member.tag, " after ", attempt, " attempts")
		}
		p.recordSuccess(member)
		return p.wrapPacketConn(conn, member), nil
	}
	if lastErr == nil {
		lastErr = E.New("no healthy proxy available")
	}
	return nil, fmt.Errorf("listen-packet failed after %d attempts: %w", maxAttempts, lastErr)
}

func (p *poolOutbound) acquireSession(ctx context.Context) (*routing.Lease, routing.Binding, error) {
	if err := ctx.Err(); err != nil {
		return nil, routing.Binding{}, err
	}
	key := routing.SessionFromContext(ctx)
	if !p.options.SessionRouting || key == "" {
		return nil, routing.Binding{}, nil
	}
	p.mu.Lock()
	if p.sessionStore == nil {
		p.sessionStore = routing.NewStore(0, 0)
	}
	store := p.sessionStore
	p.mu.Unlock()
	lease, err := store.Acquire(ctx, key)
	if err != nil {
		return nil, routing.Binding{}, err
	}
	binding, _ := lease.Binding()
	return lease, binding, nil
}

func (p *poolOutbound) memberIdentity(member *memberState) string {
	if identity := p.options.Metadata[member.tag].Identity; identity != "" {
		return identity
	}
	return member.tag
}

// dropFailedPin forgets a named session's pin when the failure came from the
// pinned node itself, and buys the current request one extra selection so a
// healthy pool is not reported as unreachable merely because the stale pin was
// tried first. Later requests start from a fresh selection, and a successful
// re-route commits the new pin through Lease.Bind.
func (p *poolOutbound) dropFailedPin(pinned *string, tried *map[string]bool, maxAttempts *int, attempt int, member *memberState, singleMember bool) {
	if *pinned == "" || p.memberIdentity(member) != *pinned {
		return
	}
	*pinned = ""
	if singleMember {
		// A single-member pool has nothing to re-route to; the binding simply
		// stops being preferred so the next request re-selects.
		return
	}
	if *tried == nil {
		*tried = make(map[string]bool, *maxAttempts+1)
	}
	(*tried)[member.tag] = true
	if *maxAttempts <= attempt {
		*maxAttempts = attempt + 1
	}
}

// maxAttempts returns the configured retry budget (>=1).
func (p *poolOutbound) maxAttempts() int {
	if !p.options.RetryEnabled {
		return 1
	}
	if p.options.RetryAttempts < 1 {
		return 1
	}
	return p.options.RetryAttempts
}

// pickMemberFiltered selects a healthy member, optionally excluding tags in `tried`.
// When `tried` is nil or every healthy member has been tried, falls back to picking
// any healthy member (ensuring single-member pools retry the same node).
func (p *poolOutbound) pickMemberFiltered(network string, tried map[string]bool, stickyKey string) (*memberState, error) {
	return p.pickMemberFor(network, tried, stickyKey, "")
}

func (p *poolOutbound) pickMemberForSession(network string, tried map[string]bool, nodeID string) (*memberState, error) {
	return p.pickMemberFor(network, tried, "", nodeID)
}

func (p *poolOutbound) pickMemberFor(network string, tried map[string]bool, stickyKey, nodeID string) (*memberState, error) {
	now := time.Now()

	p.mu.Lock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
	}
	// Most named-session traffic already has a successful pin. Resolve it in
	// O(1) rather than scanning and locking every node in a large pool.
	if nodeID != "" {
		if p.membersByID == nil {
			p.indexMembersLocked()
		}
		for _, member := range p.membersByID[nodeID] {
			if !tried[member.tag] && (network == "" || common.Contains(member.outbound.Network(), network)) &&
				(member.shared == nil || !member.shared.isBlacklisted(now)) {
				p.mu.Unlock()
				return member, nil
			}
		}
	}
	candidates := p.getCandidateBuffer()
	candidates = p.availableMembersLocked(now, network, candidates)
	p.mu.Unlock()

	if len(candidates) == 0 {
		p.mu.Lock()
		if p.releaseIfAllBlacklistedLocked(now) {
			candidates = p.availableMembersLocked(now, network, candidates[:0])
		}
		p.mu.Unlock()
	}

	if len(candidates) == 0 {
		p.putCandidateBuffer(candidates)
		return nil, E.New("no healthy proxy available")
	}

	// Filter out members already tried in this request.
	if len(tried) > 0 {
		filtered := candidates[:0]
		for _, m := range candidates {
			if !tried[m.tag] {
				filtered = append(filtered, m)
			}
		}
		// If filtering left nothing, fall back to all candidates so we still
		// dial something (matches per-node single-member retry semantics).
		if len(filtered) > 0 {
			candidates = filtered
		}
	}

	var member *memberState
	if nodeID != "" {
		for _, candidate := range candidates {
			if p.memberIdentity(candidate) == nodeID {
				member = candidate
				break
			}
		}
	}
	if member == nil {
		member = p.selectMember(candidates, stickyKey)
	}
	p.putCandidateBuffer(candidates)
	return member, nil
}

func (p *poolOutbound) pickMember(network string) (*memberState, error) {
	now := time.Now()
	candidates := p.getCandidateBuffer()

	p.mu.Lock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			p.putCandidateBuffer(candidates)
			return nil, err
		}
	}
	candidates = p.availableMembersLocked(now, network, candidates)
	p.mu.Unlock()

	if len(candidates) == 0 {
		p.mu.Lock()
		if p.releaseIfAllBlacklistedLocked(now) {
			candidates = p.availableMembersLocked(now, network, candidates)
		}
		p.mu.Unlock()
	}

	if len(candidates) == 0 {
		p.putCandidateBuffer(candidates)
		return nil, E.New("no healthy proxy available")
	}

	member := p.selectMember(candidates, "")
	p.putCandidateBuffer(candidates)
	return member, nil
}

func (p *poolOutbound) availableMembersLocked(now time.Time, network string, buf []*memberState) []*memberState {
	result := buf[:0]
	for _, member := range p.members {
		// Check blacklist via shared state (auto-clears if expired)
		if member.shared != nil && member.shared.isBlacklisted(now) {
			// Log blacklisted nodes for debugging
			remaining := member.shared.blacklistRemaining(now)
			if remaining > 0 {
				p.logger.Debug("skipping blacklisted node: ", member.tag, ", remaining: ", remaining.Round(time.Second))
			}
			continue
		}
		if network != "" && !common.Contains(member.outbound.Network(), network) {
			continue
		}
		result = append(result, member)
	}
	return result
}

func (p *poolOutbound) releaseIfAllBlacklistedLocked(now time.Time) bool {
	if len(p.members) == 0 {
		return false
	}
	// Check if all members are blacklisted
	for _, member := range p.members {
		if member.shared == nil || !member.shared.isBlacklisted(now) {
			return false
		}
	}
	// All banned. Clear the permanent blacklists so the pool can recover, but
	// leave members still serving a transient / rate-limit cooldown parked:
	// releasing them would hand the request straight back to a node we know is
	// rate-limited, which is precisely what the cooldown exists to prevent.
	released := 0
	for _, member := range p.members {
		if member.shared != nil && member.shared.releaseIfPermanent(now) {
			released++
		}
	}
	if released == 0 {
		p.logger.Debug("all upstream proxies are on cooldown, leaving them parked")
		return false
	}
	p.logger.Warn("all upstream proxies were blacklisted, released ", released, "/", len(p.members), " for retry; any still on cooldown were left parked")
	return true
}

const stickyFallbackKey = "_global_"

// stickyKeyFromCtx returns the sticky key (client source IP) for this request,
// or "" when stickiness is disabled. Falls back to a shared global key when the
// source address cannot be determined, so such requests still pin together.
func (p *poolOutbound) stickyKeyFromCtx(ctx context.Context) string {
	if !p.sticky {
		return ""
	}
	if md := adapter.ContextFrom(ctx); md != nil && md.Source.IsValid() {
		return md.Source.AddrString()
	}
	return stickyFallbackKey
}

// selectMember picks a member, honouring stickiness when stickyKey is non-empty.
func (p *poolOutbound) selectMember(candidates []*memberState, stickyKey string) *memberState {
	if stickyKey != "" {
		return p.selectSticky(candidates, stickyKey)
	}
	return p.selectByMode(candidates)
}

// selectSticky returns the member pinned to stickyKey if it is still among the
// candidates; otherwise it selects a fresh member (by mode) and pins it. The
// pin is permanent until the member drops out of the candidate set
// (blacklisted/removed), at which point a new member is chosen and pinned.
func (p *poolOutbound) selectSticky(candidates []*memberState, stickyKey string) *memberState {
	p.stickyMu.Lock()
	defer p.stickyMu.Unlock()
	if tag, ok := p.stickyMap[stickyKey]; ok {
		for _, member := range candidates {
			if member.tag == tag {
				return member
			}
		}
		// Pinned member is no longer available: drop the stale pin and re-select.
		delete(p.stickyMap, stickyKey)
	}
	member := p.selectByMode(candidates)
	if member != nil {
		p.stickyMap[stickyKey] = member.tag
	}
	return member
}

// selectByMode applies the configured scheduling strategy.
func (p *poolOutbound) selectByMode(candidates []*memberState) *memberState {
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		idx := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		return candidates[idx]
	case modeBalance:
		// Rotate only among equally least-loaded members. Rotating the start
		// of the full list would bias ties that follow a busier member.
		var minActive int32
		var selected *memberState
		tied := 0
		for _, member := range candidates {
			var active int32
			if member.shared != nil {
				active = member.shared.activeCount()
			}
			if tied == 0 || active < minActive {
				minActive, tied, selected = active, 1, member
			} else if active == minActive {
				tied++
			}
		}
		choice := int(p.rrCounter.Add(1)-1) % tied
		for _, member := range candidates {
			var active int32
			if member.shared != nil {
				active = member.shared.activeCount()
			}
			if active == minActive {
				if choice == 0 {
					return member
				}
				choice--
			}
		}
		// Counts can change between passes; retain the observed minimum rather
		// than returning nil or selecting an unrelated busier candidate.
		return selected
	case modeLatency:
		// Pick the candidate with the lowest measured latency.
		// Candidates without a latency reading (never probed) are deprioritized;
		// if all are unmeasured, fall back to round-robin.
		var selected *memberState
		var minLatency time.Duration
		hasMeasured := false
		for _, member := range candidates {
			if member.entry == nil {
				continue
			}
			latency := member.entry.LastLatency()
			if latency <= 0 {
				continue
			}
			if !hasMeasured || latency < minLatency {
				selected = member
				minLatency = latency
				hasMeasured = true
			}
		}
		if selected != nil {
			return selected
		}
		// Fallback: no measurements yet — round-robin.
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		return candidates[idx]
	default:
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		return candidates[idx]
	}
}

// blacklistFailedPin reports whether a failed session pin blacklists its node
// immediately. Unset options keep the protection enabled.
func (p *poolOutbound) blacklistFailedPin() bool {
	return p.options.BlacklistFailedPin == nil || *p.options.BlacklistFailedPin
}

// failurePolicy snapshots the pool's failure-handling tunables.
func (p *poolOutbound) failurePolicy() failurePolicy {
	return failurePolicy{
		Threshold:         p.options.FailureThreshold,
		BlacklistDuration: p.options.BlacklistDuration,
		TransientCooldown: p.options.TransientCooldown,
		RateLimitCooldown: p.options.RateLimitCooldown,
	}
}

func (p *poolOutbound) recordFailure(member *memberState, cause error) {
	if member.shared == nil {
		p.logger.Warn("proxy ", member.tag, " failure (no shared state): ", cause)
		return
	}
	policy := p.failurePolicy()
	failures, blacklisted, until, kind := member.shared.recordFailure(cause, policy)
	switch {
	case kind == faultRateLimit:
		// Upstream quota exhausted (429): park the node for the rate-limit
		// cooldown, which is typically far longer than a network blip.
		cooldown := policy.cooldownFor(faultRateLimit)
		p.logger.Warn("proxy ", member.tag, " rate-limited (429), cooling down for ", cooldown, " until ", until.Format("15:04:05"), ": ", cause)
		log.Printf("[pool] %s rate-limited (429), cooldown %s until %s: %v", member.tag, cooldown, until.Format("15:04:05"), cause)
	case kind == faultTransient:
		// Transient (timeout / reset / 503): short cooldown, not counted toward
		// the blacklist. The node is retried automatically once it ends.
		cooldown := policy.cooldownFor(faultTransient)
		p.logger.Warn("proxy ", member.tag, " transient failure, cooling down for ", cooldown, " until ", until.Format("15:04:05"), ": ", cause)
		log.Printf("[pool] %s transient failure, cooldown %s until %s: %v", member.tag, cooldown, until.Format("15:04:05"), cause)
	case blacklisted:
		// Report the real remaining span, not the configured duration: an
		// already-running longer ban wins over a fresh one, so `until` can be
		// further out than BlacklistDuration.
		remaining := time.Until(until).Round(time.Second)
		p.logger.Warn("proxy ", member.tag, " blacklisted for ", remaining, ": ", cause)
		log.Printf("⚠️  [pool] %s BLACKLISTED for %s (until %s): %v", member.tag, remaining, until.Format("15:04:05"), cause)
		log.Printf("    To release immediately, use WebUI or: POST /api/nodes/%s/release", member.tag)
	default:
		p.logger.Warn("proxy ", member.tag, " failure ", failures, "/", p.options.FailureThreshold, ": ", cause)
		log.Printf("[pool] %s failure %d/%d: %v", member.tag, failures, p.options.FailureThreshold, cause)
	}
}

// recordPinFailure records a dial failure that came through a named session's
// pinned node. The binding is proven unusable, so a real transport fault
// blacklists that node immediately instead of waiting for the failure
// threshold; a node that only answers 429/timeout keeps its configured
// cooldown, because those say nothing about the rest of the pool and expire on
// their own.
func (p *poolOutbound) recordPinFailure(member *memberState, cause error) {
	if member.shared == nil || !p.blacklistFailedPin() || classifyFailure(cause) != faultPermanent {
		p.recordFailure(member, cause)
		return
	}
	until := member.shared.blacklistNow(cause, p.failurePolicy())
	remaining := time.Until(until).Round(time.Second)
	p.logger.Warn("proxy ", member.tag, " blacklisted for ", remaining, " after a session pin failed to dial: ", cause)
	log.Printf("⚠️  [pool] %s BLACKLISTED for %s (session pin dial failure, until %s): %v", member.tag, remaining, until.Format("15:04:05"), cause)
	log.Printf("    To release immediately, use WebUI or: POST /api/nodes/%s/release", member.tag)
}

func (p *poolOutbound) recordSuccess(member *memberState) {
	if member.shared != nil {
		member.shared.recordSuccess()
	}
}

func (p *poolOutbound) wrapConn(conn net.Conn, member *memberState) net.Conn {
	return &trackedConn{Conn: conn, release: func() {
		p.decActive(member)
	}}
}

func (p *poolOutbound) wrapPacketConn(conn net.PacketConn, member *memberState) net.PacketConn {
	return &trackedPacketConn{PacketConn: conn, release: func() {
		p.decActive(member)
	}}
}

func (p *poolOutbound) makeReleaseFunc(member *memberState) func() {
	return func() {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
}

// upgradeProbeConn optionally upgrades a plain TCP connection to TLS with
// strict certificate verification. When useTLS is false the connection is
// returned unchanged. host is used as the TLS SNI. A handshake failure
// (e.g. self-signed or hijacked certificate) is returned as an error so the
// caller can record it and eventually blacklist the node.
func upgradeProbeConn(ctx context.Context, conn net.Conn, host string, useTLS bool) (net.Conn, error) {
	if !useTLS {
		return conn, nil
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: false,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		// Return the original connection so the caller's deferred Close keeps
		// working; the failed handshake did not take over its lifetime.
		return conn, fmt.Errorf("tls handshake: %w", err)
	}
	return tlsConn, nil
}

const maxProbeHeaderBytes = 64 * 1024

// httpProbe validates an HTTP response without reading its body. A socket that
// emits one byte, a proxy authentication failure, or a quota/error response is
// not a successful health check. Header size and deadlines bound the parser.
func httpProbe(conn net.Conn, host string) (time.Duration, error) {
	req := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: easy-proxies-health\r\n\r\n", host)
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := io.WriteString(conn, req); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	response, err := http.ReadResponse(bufio.NewReader(io.LimitReader(conn, maxProbeHeaderBytes)), &http.Request{Method: http.MethodGet})
	if err != nil {
		return 0, fmt.Errorf("read HTTP response: %w", err)
	}
	// The caller owns and closes conn. Closing response.Body here could drain
	// an unbounded or stalled body; only the headers matter to this probe.
	if response.StatusCode < 200 || response.StatusCode >= 400 {
		return 0, fmt.Errorf("HTTP probe returned status %d", response.StatusCode)
	}
	return time.Since(start), nil
}

// probeMember dials the probe destination through a member and measures latency.
// A context watchdog force-closes the underlying connection when ctx is cancelled
// or its deadline fires. This is essential: some sing-box outbound connection
// types do not honor SetDeadline, so without the watchdog a probe against a node
// that accepts TCP but never sends an HTTP response would block on Read / the TLS
// handshake forever, leaking goroutines and hanging the whole batch probe
// (wg.Wait would never return, freezing the WebUI at "N/M").
func (p *poolOutbound) probeMember(ctx context.Context, member *memberState, destination M.Socksaddr, host string, useTLS bool) (time.Duration, error) {
	start := time.Now()
	rawConn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		if member.entry != nil && !errors.Is(ctx.Err(), context.Canceled) {
			member.entry.RecordFailure(err)
		}
		return 0, err
	}
	// Watchdog: close the raw connection as soon as ctx is done so any blocked
	// Read/Write/handshake below is unblocked even when SetDeadline is a no-op.
	probeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = rawConn.Close()
		case <-probeDone:
		}
	}()
	defer close(probeDone)
	defer rawConn.Close()

	conn := rawConn
	// Strict mode: upgrade to TLS and verify the certificate chain so that
	// nodes whose exit hijacks TLS (self-signed certs) fail the probe.
	if conn, err = upgradeProbeConn(ctx, conn, host, useTLS); err != nil {
		if member.entry != nil && !errors.Is(ctx.Err(), context.Canceled) {
			member.entry.RecordFailure(err)
		}
		return 0, err
	}

	// Perform HTTP probe to measure actual latency (TTFB).
	if _, err = httpProbe(conn, destination.AddrString()); err != nil {
		if member.entry != nil && !errors.Is(ctx.Err(), context.Canceled) {
			member.entry.RecordFailure(err)
		}
		return 0, err
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	duration := time.Since(start)
	if member.entry != nil {
		member.entry.RecordSuccessWithLatency(duration)
	}
	// Clear a permanent pool blacklist on successful probe — a node that passes
	// the health check should be available for selection immediately (fixes #8,
	// #9). A live transient / rate-limit cooldown is deliberately preserved: the
	// probe targets a different host than the failing request did, so passing it
	// says nothing about an exhausted upstream quota.
	if member.shared != nil {
		member.shared.releaseIfPermanent(time.Now())
	}
	return duration, nil
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	destination, host, useTLS, ok := p.monitor.DestinationForProbe()
	if !ok {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		return p.probeMember(ctx, member, destination, host, useTLS)
	}
}

// makeProbeByTagFunc creates a probe function that works before member initialization
func (p *poolOutbound) makeProbeByTagFunc(tag string) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	destination, host, useTLS, ok := p.monitor.DestinationForProbe()
	if !ok {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		// Ensure members are initialized
		p.mu.Lock()
		if len(p.members) == 0 {
			if err := p.initializeMembersLocked(); err != nil {
				p.mu.Unlock()
				return 0, err
			}
		}

		// Find the member by tag
		var member *memberState
		for _, m := range p.members {
			if m.tag == tag {
				member = m
				break
			}
		}
		p.mu.Unlock()

		if member == nil {
			return 0, E.New("member not found: ", tag)
		}
		return p.probeMember(ctx, member, destination, host, useTLS)
	}
}

// makeReleaseByTagFunc creates a release function that works before member initialization
func (p *poolOutbound) makeReleaseByTagFunc(tag string) func() {
	return func() {
		releaseSharedMember(tag)
	}
}

// makeBlacklistByTagFunc creates a blacklist function for manual ban via API
func (p *poolOutbound) makeBlacklistByTagFunc(tag string) func(time.Duration) {
	return func(duration time.Duration) {
		blacklistSharedMember(tag, duration)
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (p *poolOutbound) incActive(member *memberState) {
	if member.shared != nil {
		member.shared.incActive()
	}
}

func (p *poolOutbound) decActive(member *memberState) {
	if member.shared != nil {
		member.shared.decActive()
	}
}

func (p *poolOutbound) getCandidateBuffer() []*memberState {
	if buf := p.candidatesPool.Get(); buf != nil {
		return buf.([]*memberState)
	}
	return make([]*memberState, 0, len(p.options.Members))
}

func (p *poolOutbound) putCandidateBuffer(buf []*memberState) {
	if buf == nil {
		return
	}
	const maxCached = 4096
	if cap(buf) > maxCached {
		return
	}
	p.candidatesPool.Put(buf[:0])
}

// indexMembersLocked builds a stable identity index once per pool lifecycle.
// Duplicate subscription entries retain their original candidate order.
func (p *poolOutbound) indexMembersLocked() {
	p.membersByID = make(map[string][]*memberState, len(p.members))
	for _, member := range p.members {
		id := p.memberIdentity(member)
		p.membersByID[id] = append(p.membersByID[id], member)
	}
}

// Close releases the global registry reference without touching a replacement
// pool or the manager-owned named-session store.
func (p *poolOutbound) Close() error {
	unregisterDialer(p.Tag(), p)
	return nil
}
