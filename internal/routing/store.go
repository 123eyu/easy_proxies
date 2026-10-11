package routing

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"time"
)

const (
	DefaultMaxBindings = 10000
	DefaultIdleTTL     = 24 * time.Hour
)

// Binding describes a named session's last successfully dialed upstream.
// NodeID is a stable, opaque identity, never a proxy URI or its credentials.
type Binding struct {
	ID          string    `json:"id"`
	NodeID      string    `json:"node_id"`
	NodeTag     string    `json:"node_tag"`
	NodeName    string    `json:"node_name"`
	CreatedAt   time.Time `json:"created_at"`
	LastSeen    time.Time `json:"last_seen"`
	ExpiresAt   time.Time `json:"expires_at"`
	SwitchCount uint64    `json:"switch_count"`
	LastReason  string    `json:"last_reason"`
}

type bindingEntry struct {
	binding Binding
	element *list.Element
}

type sessionGate struct {
	token   chan struct{}
	refs    int
	version uint64
}

// Store bounds the binding cache with idle expiration and LRU eviction. It is
// owned by the manager, so config reloads retain bindings but process restarts
// do not. Gates exist only while a session has in-flight or waiting dials.
type Store struct {
	mu      sync.Mutex
	entries map[string]*bindingEntry
	lru     list.List
	gates   map[string]*sessionGate
	max     int
	ttl     time.Duration
	epoch   uint64
	now     func() time.Time
}

func NewStore(max int, ttl time.Duration) *Store {
	s := &Store{
		entries: make(map[string]*bindingEntry),
		gates:   make(map[string]*sessionGate),
		now:     time.Now,
	}
	s.Configure(max, ttl)
	return s
}

// Configure updates cache limits without resetting live bindings.
func (s *Store) Configure(max int, ttl time.Duration) {
	if max <= 0 {
		max = DefaultMaxBindings
	}
	if ttl <= 0 {
		ttl = DefaultIdleTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	// Changing the limits must not resurrect sessions that already expired
	// under the previous policy but have not been lazily pruned yet.
	s.pruneLocked(now)
	s.max, s.ttl = max, ttl
	for _, entry := range s.entries {
		entry.binding.ExpiresAt = entry.binding.LastSeen.Add(ttl)
	}
	s.pruneLocked(now)
	s.enforceCapacityLocked()
}

// List returns a stable snapshot without extending idle expiration.
func (s *Store) List() []Binding {
	s.mu.Lock()
	s.pruneLocked(s.now())
	bindings := make([]Binding, 0, len(s.entries))
	for _, entry := range s.entries {
		bindings = append(bindings, entry.binding)
	}
	s.mu.Unlock()
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].ID < bindings[j].ID })
	return bindings
}

// Delete also invalidates an in-flight lease, so an administrative unbind is
// not immediately undone by a dial that began before the deletion.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(s.now())
	if gate := s.gates[key]; gate != nil {
		gate.version++
	}
	entry, exists := s.entries[key]
	if exists {
		s.removeLocked(key, entry)
	}
	return exists
}

func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*bindingEntry)
	s.lru.Init()
	s.epoch++
}

// Lease serializes dial/commit for one key, not the entire pool. Waiting for
// another dial is context-cancellable, and different keys proceed in parallel.
type Lease struct {
	store    *Store
	key      string
	gate     *sessionGate
	version  uint64
	epoch    uint64
	once     sync.Once
	released bool // protected by store.mu
}

func (s *Store) Acquire(ctx context.Context, key string) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	gate := s.gates[key]
	if gate == nil {
		gate = &sessionGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		s.gates[key] = gate
	}
	gate.refs++
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.releaseReference(key, gate)
		return nil, ctx.Err()
	case <-gate.token:
	}
	if err := ctx.Err(); err != nil {
		s.releaseReference(key, gate)
		gate.token <- struct{}{}
		return nil, err
	}
	s.mu.Lock()
	lease := &Lease{store: s, key: key, gate: gate, version: gate.version, epoch: s.epoch}
	s.mu.Unlock()
	return lease, nil
}

func (s *Store) releaseReference(key string, gate *sessionGate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gate.refs--
	if gate.refs == 0 && s.gates[key] == gate {
		delete(s.gates, key)
	}
}

func (l *Lease) Release() {
	l.once.Do(func() {
		l.store.mu.Lock()
		l.released = true
		l.store.mu.Unlock()
		l.store.releaseReference(l.key, l.gate)
		l.gate.token <- struct{}{}
	})
}

// Binding reads and touches the current pin, extending its idle lifetime.
func (l *Lease) Binding() (Binding, bool) {
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released || l.epoch != s.epoch || l.version != l.gate.version {
		return Binding{}, false
	}
	now := s.now()
	entry, exists := s.entries[l.key]
	if !exists {
		return Binding{}, false
	}
	if !now.Before(entry.binding.ExpiresAt) {
		s.removeLocked(l.key, entry)
		return Binding{}, false
	}
	entry.binding.LastSeen = now
	entry.binding.ExpiresAt = now.Add(s.ttl)
	s.lru.MoveToFront(entry.element)
	return entry.binding, true
}

// Bind commits only after a successful dial. Updating a renamed node with the
// same identity refreshes its display metadata without counting a switch.
func (l *Lease) Bind(nodeID, nodeTag, nodeName, reason string) {
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released || l.epoch != s.epoch || l.version != l.gate.version {
		return
	}
	now := s.now()
	s.pruneLocked(now)
	entry := s.entries[l.key]
	if entry == nil {
		entry = &bindingEntry{binding: Binding{ID: l.key, CreatedAt: now, LastReason: "initial"}}
		entry.element = s.lru.PushFront(l.key)
		s.entries[l.key] = entry
	} else if entry.binding.NodeID != nodeID {
		entry.binding.SwitchCount++
		entry.binding.LastReason = reason
		if reason == "" {
			entry.binding.LastReason = "node_unavailable"
		}
	}
	entry.binding.NodeID = nodeID
	entry.binding.NodeTag = nodeTag
	entry.binding.NodeName = nodeName
	entry.binding.LastSeen = now
	entry.binding.ExpiresAt = now.Add(s.ttl)
	s.lru.MoveToFront(entry.element)
	s.enforceCapacityLocked()
}

func (s *Store) pruneLocked(now time.Time) {
	// All entries share the idle TTL, so the least recently touched entry is
	// also the first to expire. Avoid scanning the whole cache on each dial.
	for tail := s.lru.Back(); tail != nil; tail = s.lru.Back() {
		key := tail.Value.(string)
		entry := s.entries[key]
		if now.Before(entry.binding.ExpiresAt) {
			return
		}
		s.removeLocked(key, entry)
	}
}

func (s *Store) enforceCapacityLocked() {
	for len(s.entries) > s.max {
		key := s.lru.Back().Value.(string)
		s.removeLocked(key, s.entries[key])
	}
}

func (s *Store) removeLocked(key string, entry *bindingEntry) {
	delete(s.entries, key)
	s.lru.Remove(entry.element)
}
