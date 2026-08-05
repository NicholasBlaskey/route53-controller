// Package batch provides a batching aggregator for Route53 ChangeResourceRecordSets
// API calls. Instead of making one API call per RecordSet CR reconcile, the aggregator
// collects changes by hosted zone and flushes them in bulk, dramatically reducing
// API call count and improving throughput.
package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/go-logr/logr"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
)

// Route53API is the subset of the Route53 SDK client that the aggregator uses.
type Route53API interface {
	ChangeResourceRecordSets(ctx context.Context, input *route53.ChangeResourceRecordSetsInput, opts ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error)
}

// Config holds configuration for the BatchAggregator.
type Config struct {
	// MaxWait is the maximum time to buffer changes before flushing.
	// Lower values reduce latency; higher values allow larger batches.
	// Default: 200ms.
	MaxWait time.Duration

	// MaxBatchSize is the maximum number of changes to include in a single
	// ChangeResourceRecordSets call. Route53 limit is 1000.
	// Default: 500.
	MaxBatchSize int
}

// DefaultConfig returns sensible default configuration.
func DefaultConfig() Config {
	return Config{
		MaxWait:      200 * time.Millisecond,
		MaxBatchSize: 500,
	}
}

// RecordChange represents a single change submitted by a reconciler.
type RecordChange struct {
	// HostedZoneID is the Route53 hosted zone this change targets.
	HostedZoneID string
	// Action is the change action (CREATE, UPSERT, DELETE).
	Action svcsdktypes.ChangeAction
	// RecordSet is the fully-constructed ResourceRecordSet for the change.
	RecordSet *svcsdktypes.ResourceRecordSet
}

// ChangeResult is returned to the caller after the batch is flushed.
type ChangeResult struct {
	// ChangeInfo is the Route53 response for the batch. All changes in a
	// successful batch share the same ChangeInfo.
	ChangeInfo *svcsdktypes.ChangeInfo
	// Err is non-nil if the batch API call failed.
	Err error
}

// pendingChange wraps a RecordChange with its result channel.
type pendingChange struct {
	change     RecordChange
	resultChan chan<- ChangeResult
}

// zoneBatch holds the pending changes for a single hosted zone.
type zoneBatch struct {
	changes []*pendingChange
	timer   *time.Timer
}

// Aggregator collects RecordSet changes by hosted zone and flushes them
// in batches to minimize Route53 API calls.
type Aggregator struct {
	mu      sync.Mutex
	zones   map[string]*zoneBatch
	cfg     Config
	client  Route53API
	metrics *ackmetrics.Metrics
	log     logr.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewAggregator creates a new BatchAggregator.
func NewAggregator(client Route53API, metrics *ackmetrics.Metrics, log logr.Logger, cfg Config) *Aggregator {
	if cfg.MaxWait == 0 {
		cfg.MaxWait = DefaultConfig().MaxWait
	}
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = DefaultConfig().MaxBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Aggregator{
		zones:   make(map[string]*zoneBatch),
		cfg:     cfg,
		client:  client,
		metrics: metrics,
		log:     log.WithName("batch-aggregator"),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Submit adds a change to the aggregator and blocks until the batch containing
// it is flushed. The caller receives the result of the batch API call.
// If ctx is cancelled before the flush, the change is removed and a context error returned.
func (a *Aggregator) Submit(ctx context.Context, change RecordChange) ChangeResult {
	resultChan := make(chan ChangeResult, 1)

	a.mu.Lock()
	zb, exists := a.zones[change.HostedZoneID]
	if !exists {
		zb = &zoneBatch{
			changes: make([]*pendingChange, 0, a.cfg.MaxBatchSize),
		}
		a.zones[change.HostedZoneID] = zb
	}

	pc := &pendingChange{
		change:     change,
		resultChan: resultChan,
	}
	zb.changes = append(zb.changes, pc)

	// Start the flush timer on first item for this zone
	if !exists || zb.timer == nil {
		zb.timer = time.AfterFunc(a.cfg.MaxWait, func() {
			a.flushZone(change.HostedZoneID)
		})
	}

	// Flush immediately if we've hit max batch size
	if len(zb.changes) >= a.cfg.MaxBatchSize {
		// Stop timer since we're flushing now
		zb.timer.Stop()
		changes := zb.changes
		zb.changes = nil
		zb.timer = nil
		delete(a.zones, change.HostedZoneID)
		a.mu.Unlock()

		a.executeFlush(change.HostedZoneID, changes)
	} else {
		a.mu.Unlock()
	}

	// Block until result arrives or context is cancelled
	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		// Context cancelled — try to remove our pending change
		a.removePending(change.HostedZoneID, pc)
		return ChangeResult{Err: ctx.Err()}
	}
}

// flushZone is called by the timer to flush all pending changes for a zone.
func (a *Aggregator) flushZone(hostedZoneID string) {
	a.mu.Lock()
	zb, exists := a.zones[hostedZoneID]
	if !exists || len(zb.changes) == 0 {
		a.mu.Unlock()
		return
	}

	changes := zb.changes
	zb.changes = nil
	zb.timer = nil
	delete(a.zones, hostedZoneID)
	a.mu.Unlock()

	a.executeFlush(hostedZoneID, changes)
}

// executeFlush makes the actual Route53 API call with the batched changes.
func (a *Aggregator) executeFlush(hostedZoneID string, pending []*pendingChange) {
	changes := make([]svcsdktypes.Change, 0, len(pending))
	for _, pc := range pending {
		changes = append(changes, svcsdktypes.Change{
			Action:            pc.change.Action,
			ResourceRecordSet: pc.change.RecordSet,
		})
	}

	input := &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: &hostedZoneID,
		ChangeBatch: &svcsdktypes.ChangeBatch{
			Changes: changes,
		},
	}

	a.log.V(1).Info("flushing batch",
		"hostedZoneID", hostedZoneID,
		"changeCount", len(changes),
	)

	resp, err := a.client.ChangeResourceRecordSets(a.ctx, input)
	if a.metrics != nil {
		a.metrics.RecordAPICall("BATCH", "ChangeResourceRecordSets", err)
	}

	if err != nil {
		a.log.V(0).Info("batch failed, falling back to individual submissions",
			"hostedZoneID", hostedZoneID,
			"changeCount", len(changes),
			"error", err.Error(),
		)
		// Fallback: try each change individually
		a.fallbackIndividual(hostedZoneID, pending)
		return
	}

	// Success — fan out result to all waiters
	result := ChangeResult{
		ChangeInfo: resp.ChangeInfo,
	}
	for _, pc := range pending {
		pc.resultChan <- result
	}
}

// fallbackIndividual submits each change individually when the batch fails.
// This handles the case where one bad change poisons the whole batch.
func (a *Aggregator) fallbackIndividual(hostedZoneID string, pending []*pendingChange) {
	for _, pc := range pending {
		input := &route53.ChangeResourceRecordSetsInput{
			HostedZoneId: &hostedZoneID,
			ChangeBatch: &svcsdktypes.ChangeBatch{
				Changes: []svcsdktypes.Change{
					{
						Action:            pc.change.Action,
						ResourceRecordSet: pc.change.RecordSet,
					},
				},
			},
		}

		resp, err := a.client.ChangeResourceRecordSets(a.ctx, input)
		if a.metrics != nil {
			a.metrics.RecordAPICall("BATCH_FALLBACK", "ChangeResourceRecordSets", err)
		}

		if err != nil {
			a.log.V(0).Info("individual fallback failed",
				"hostedZoneID", hostedZoneID,
				"recordName", safeRecordName(pc.change.RecordSet),
				"error", err.Error(),
			)
			pc.resultChan <- ChangeResult{Err: fmt.Errorf("batch and individual submission failed: %w", err)}
		} else {
			pc.resultChan <- ChangeResult{ChangeInfo: resp.ChangeInfo}
		}
	}
}

// removePending removes a specific pending change from its zone batch.
// Used when a caller's context is cancelled before the batch flushes.
func (a *Aggregator) removePending(hostedZoneID string, target *pendingChange) {
	a.mu.Lock()
	defer a.mu.Unlock()

	zb, exists := a.zones[hostedZoneID]
	if !exists {
		return
	}

	for i, pc := range zb.changes {
		if pc == target {
			zb.changes = append(zb.changes[:i], zb.changes[i+1:]...)
			break
		}
	}

	// If zone batch is now empty, clean up
	if len(zb.changes) == 0 {
		if zb.timer != nil {
			zb.timer.Stop()
		}
		delete(a.zones, hostedZoneID)
	}
}

// Stop shuts down the aggregator, flushing any remaining pending changes.
func (a *Aggregator) Stop() {
	a.mu.Lock()
	// Flush all remaining zones
	zones := make(map[string]*zoneBatch, len(a.zones))
	for k, v := range a.zones {
		zones[k] = v
	}
	a.zones = make(map[string]*zoneBatch)
	a.mu.Unlock()

	for hostedZoneID, zb := range zones {
		if zb.timer != nil {
			zb.timer.Stop()
		}
		if len(zb.changes) > 0 {
			a.executeFlush(hostedZoneID, zb.changes)
		}
	}

	a.cancel()
}

// Pending returns the total number of changes currently buffered (for metrics/testing).
func (a *Aggregator) Pending() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	total := 0
	for _, zb := range a.zones {
		total += len(zb.changes)
	}
	return total
}

func safeRecordName(rs *svcsdktypes.ResourceRecordSet) string {
	if rs == nil || rs.Name == nil {
		return "<nil>"
	}
	return *rs.Name
}
