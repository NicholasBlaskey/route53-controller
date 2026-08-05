package record_set

import (
	"context"
	"sync"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
)

// zoneCache caches hosted zone domain names by zone ID.
// GetHostedZone is called on every reconcile to resolve the zone domain,
// but the domain never changes for a given zone ID. Caching it eliminates
// one API call per reconcile.
type zoneCache struct {
	mu      sync.RWMutex
	domains map[string]string // zoneID → domain name (e.g. "example.com.")
}

var globalZoneCache = &zoneCache{
	domains: make(map[string]string),
}

// get returns the cached domain for a zone ID, or empty string if not cached.
func (zc *zoneCache) get(zoneID string) (string, bool) {
	zc.mu.RLock()
	defer zc.mu.RUnlock()
	domain, ok := zc.domains[zoneID]
	return domain, ok
}

// set stores the domain for a zone ID.
func (zc *zoneCache) set(zoneID string, domain string) {
	zc.mu.Lock()
	defer zc.mu.Unlock()
	zc.domains[zoneID] = domain
}

// getHostedZoneDomainCached wraps getHostedZoneDomain with caching.
// It checks the cache first and only calls the API on a miss.
func (rm *resourceManager) getHostedZoneDomainCached(
	ctx context.Context,
	zoneID string,
) (string, error) {
	if domain, ok := globalZoneCache.get(zoneID); ok {
		return domain, nil
	}

	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getHostedZoneDomainCached (cache miss)")
	var err error
	defer func() { exit(err) }()

	// Rate limit the API call (only on cache miss, which is rare)
	if rm.rateLimiter != nil {
		if err := rm.rateLimiter.Wait(ctx); err != nil {
			return "", err
		}
	}

	input := &svcsdk.GetHostedZoneInput{
		Id: &zoneID,
	}
	resp, err := rm.sdkapi.GetHostedZone(ctx, input)
	rm.metrics.RecordAPICall("READ_ONE", "GetHostedZone", err)
	if err != nil {
		return "", err
	}

	domain := *resp.HostedZone.Name
	globalZoneCache.set(zoneID, domain)
	return domain, nil
}
