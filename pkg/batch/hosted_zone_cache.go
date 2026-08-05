package batch

import (
	"sync"
	"time"

	"github.com/go-logr/logr"
)

// HostedZoneCache caches GetHostedZone domain lookups to avoid repeated
// API calls. Each reconcile currently calls GetHostedZone to resolve the
// zone's domain name — this cache eliminates that overhead.
type HostedZoneCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	log     logr.Logger
}

type cacheEntry struct {
	domain    string
	expiresAt time.Time
}

// NewHostedZoneCache creates a cache with the given TTL.
func NewHostedZoneCache(ttl time.Duration, log logr.Logger) *HostedZoneCache {
	return &HostedZoneCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
		log:     log.WithName("hz-cache"),
	}
}

// Get returns the cached domain for a hosted zone ID, or ("", false) if not cached.
func (c *HostedZoneCache) Get(hostedZoneID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[hostedZoneID]
	if !exists {
		return "", false
	}
	if time.Now().After(entry.expiresAt) {
		return "", false
	}
	return entry.domain, true
}

// Put stores the domain for a hosted zone ID.
func (c *HostedZoneCache) Put(hostedZoneID, domain string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[hostedZoneID] = &cacheEntry{
		domain:    domain,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.log.V(2).Info("cached hosted zone domain",
		"hostedZoneID", hostedZoneID,
		"domain", domain,
	)
}

// Invalidate removes a specific entry from the cache.
func (c *HostedZoneCache) Invalidate(hostedZoneID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, hostedZoneID)
}

// Len returns the number of entries currently in the cache.
func (c *HostedZoneCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
