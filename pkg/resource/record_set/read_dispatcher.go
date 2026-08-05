package record_set

import (
	"context"
	"sync"
	"time"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/go-logr/logr"
)

// ReadDispatcher batches ListResourceRecordSets calls per hosted zone.
// Instead of each reconciler calling the API individually, they enqueue
// a read request and block. Every flushInterval, one ListResourceRecordSets
// call per zone is made and the result is delivered to all waiting reconcilers.
//
// This is the same pattern as BatchDispatcher for writes.
type ReadDispatcher struct {
	mu            sync.Mutex
	zones         map[string]*zoneReadBatch
	sdkapi        *svcsdk.Client
	metrics       *ackmetrics.Metrics
	log           logr.Logger
	flushInterval time.Duration

	// lastResults stores the most recent fetch result per zone.
	// Used to serve reconcilers that arrive between flush cycles
	// (e.g. the second reconcile pass that checks if record exists).
	lastResults   map[string]*readResult
	lastResultsMu sync.RWMutex
}

type zoneReadBatch struct {
	hostedZoneID string
	waiters      []chan readResult
	timer        *time.Timer
}

type readResult struct {
	records   []svcsdktypes.ResourceRecordSet
	err       error
	fetchedAt time.Time
}

// NewReadDispatcher creates a ReadDispatcher with the given flush interval.
func NewReadDispatcher(
	sdkapi *svcsdk.Client,
	metrics *ackmetrics.Metrics,
	log logr.Logger,
	flushInterval time.Duration,
) *ReadDispatcher {
	rd := &ReadDispatcher{
		zones:         make(map[string]*zoneReadBatch),
		sdkapi:        sdkapi,
		metrics:       metrics,
		log:           log.WithName("read-dispatcher"),
		flushInterval: flushInterval,
		lastResults:   make(map[string]*readResult),
	}
	globalReadDispatcher = rd
	return rd
}

// globalReadDispatcher is set when the ReadDispatcher is created.
// Used by the BatchDispatcher to inject written records into the read cache.
var globalReadDispatcher *ReadDispatcher

// List enqueues a read request for the given zone and blocks until the
// batch is flushed. If a recent result exists (< flushInterval old), it's
// returned immediately without waiting for the next flush.
func (rd *ReadDispatcher) List(
	ctx context.Context,
	hostedZoneID string,
) ([]svcsdktypes.ResourceRecordSet, error) {
	// Fast path: return recent result if still fresh
	rd.lastResultsMu.RLock()
	if last, ok := rd.lastResults[hostedZoneID]; ok && time.Since(last.fetchedAt) < rd.flushInterval {
		rd.lastResultsMu.RUnlock()
		return last.records, last.err
	}
	rd.lastResultsMu.RUnlock()

	// Enqueue and wait
	resultCh := make(chan readResult, 1)

	rd.mu.Lock()
	zrb, ok := rd.zones[hostedZoneID]
	if !ok {
		zrb = &zoneReadBatch{hostedZoneID: hostedZoneID}
		rd.zones[hostedZoneID] = zrb
	}
	zrb.waiters = append(zrb.waiters, resultCh)
	isFirst := len(zrb.waiters) == 1

	// Start timer on first waiter. On the very first fetch for a zone
	// (no lastResults exist), flush immediately — don't make everyone wait.
	if isFirst {
		rd.lastResultsMu.RLock()
		_, hasLast := rd.lastResults[hostedZoneID]
		rd.lastResultsMu.RUnlock()

		if !hasLast {
			// First ever fetch — flush immediately
			rd.mu.Unlock()
			rd.flushZone(hostedZoneID)
		} else {
			// Subsequent fetches — use flush interval
			zrb.timer = time.AfterFunc(rd.flushInterval, func() {
				rd.flushZone(hostedZoneID)
			})
			rd.mu.Unlock()
		}
	} else {
		rd.mu.Unlock()
	}

	// Block until result
	select {
	case res := <-resultCh:
		return res.records, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// flushZone fetches all records for a zone and delivers the result to all waiters.
func (rd *ReadDispatcher) flushZone(hostedZoneID string) {
	rd.mu.Lock()
	zrb, ok := rd.zones[hostedZoneID]
	if !ok || len(zrb.waiters) == 0 {
		rd.mu.Unlock()
		return
	}
	waiters := zrb.waiters
	zrb.waiters = nil
	if zrb.timer != nil {
		zrb.timer.Stop()
		zrb.timer = nil
	}
	rd.mu.Unlock()

	// Fetch
	records, err := rd.fetchAllRecords(context.Background(), hostedZoneID)

	rd.log.V(1).Info("read flush",
		"zone", hostedZoneID,
		"waiters", len(waiters),
		"records", len(records),
		"error", err,
	)

	// Store as last result for fast-path
	result := &readResult{records: records, err: err, fetchedAt: time.Now()}
	rd.lastResultsMu.Lock()
	rd.lastResults[hostedZoneID] = result
	rd.lastResultsMu.Unlock()

	// Deliver to all waiters
	for _, ch := range waiters {
		ch <- readResult{records: records, err: err, fetchedAt: time.Now()}
	}
}

// fetchAllRecords paginates through ListResourceRecordSets to get all records in a zone.
func (rd *ReadDispatcher) fetchAllRecords(
	ctx context.Context,
	hostedZoneID string,
) ([]svcsdktypes.ResourceRecordSet, error) {
	var allRecords []svcsdktypes.ResourceRecordSet

	input := &svcsdk.ListResourceRecordSetsInput{
		HostedZoneId: &hostedZoneID,
	}

	for {
		resp, err := rd.sdkapi.ListResourceRecordSets(ctx, input)
		if rd.metrics != nil {
			rd.metrics.RecordAPICall("READ_MANY", "ListResourceRecordSets", err)
		}
		if err != nil {
			return nil, err
		}

		allRecords = append(allRecords, resp.ResourceRecordSets...)

		if !resp.IsTruncated {
			break
		}
		input.StartRecordName = resp.NextRecordName
		if resp.NextRecordType != "" {
			input.StartRecordType = resp.NextRecordType
		}
		input.StartRecordIdentifier = resp.NextRecordIdentifier
	}

	return allRecords, nil
}

// UpsertRecords updates the last-result cache to reflect a successful write.
// This ensures the next fast-path read sees the newly created/updated/deleted records.
func (rd *ReadDispatcher) UpsertRecords(hostedZoneID string, changes []svcsdktypes.Change) {
	rd.lastResultsMu.Lock()
	defer rd.lastResultsMu.Unlock()

	last, ok := rd.lastResults[hostedZoneID]
	if !ok || last == nil {
		return
	}

	for _, change := range changes {
		rrs := change.ResourceRecordSet
		if rrs == nil {
			continue
		}

		switch change.Action {
		case svcsdktypes.ChangeActionCreate, svcsdktypes.ChangeActionUpsert:
			found := false
			for i, existing := range last.records {
				if recordsMatch(&existing, rrs) {
					last.records[i] = *rrs
					found = true
					break
				}
			}
			if !found {
				last.records = append(last.records, *rrs)
			}

		case svcsdktypes.ChangeActionDelete:
			for i, existing := range last.records {
				if recordsMatch(&existing, rrs) {
					last.records = append(last.records[:i], last.records[i+1:]...)
					break
				}
			}
		}
	}
}

// recordsMatch returns true if two record sets represent the same DNS record
// (same name, type, and set identifier).
func recordsMatch(a, b *svcsdktypes.ResourceRecordSet) bool {
	if a.Name == nil || b.Name == nil {
		return false
	}
	if *a.Name != *b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	// For routing-policy records, SetIdentifier distinguishes them
	if a.SetIdentifier != nil && b.SetIdentifier != nil {
		return *a.SetIdentifier == *b.SetIdentifier
	}
	return a.SetIdentifier == nil && b.SetIdentifier == nil
}
