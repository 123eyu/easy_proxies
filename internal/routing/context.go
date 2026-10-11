package routing

import "context"

type storeContextKey struct{}
type sessionContextKey struct{}

// WithStore shares named-session bindings across outbound/config reloads.
func WithStore(ctx context.Context, store *Store) context.Context {
	return context.WithValue(ctx, storeContextKey{}, store)
}

func StoreFromContext(ctx context.Context) *Store {
	store, _ := ctx.Value(storeContextKey{}).(*Store)
	return store
}

// WithSession associates an opaque session identifier with a proxy request.
// Authentication is handled independently by the inbound that sets this value.
func WithSession(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, key)
}

func SessionFromContext(ctx context.Context) string {
	key, _ := ctx.Value(sessionContextKey{}).(string)
	return key
}
