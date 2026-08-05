package record_set

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	smithy "github.com/aws/smithy-go"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
)

// BatchConfig holds configuration for the BatchDispatcher.
type BatchConfig struct {
	// MaxBatchSize is the maximum number of changes per ChangeResourceRecordSets call.
	// Route53 allows up to 1000.
	MaxBatchSize int

	// FlushInterval is how long to wait after the first enqueued item before
	// flushing the batch. Lower values reduce latency for single-record changes;
	// higher values improve batching efficiency for bulk operations.
	FlushInterval time.Duration

	// RateLimit is the maximum Route53 API calls per second.
	// Route53 enforces 5 req/s account-wide; we default to 4 to leave headroom.
	RateLimit float64

	// MaxBisectDepth limits how deep bisection retry goes. This prevents runaway
	// recursion if many records in a batch are invalid.
	MaxBisectDepth int

	// MaxPriorRequestRetries limits how many times a batch retries when
	// Route53 returns PriorRequestNotComplete. Each retry sleeps 2 seconds,
	// so 15 retries = 30 seconds max wait.
	MaxPriorRequestRetries int
}

// DefaultBatchConfig returns sensible defaults for batch configuration.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxBatchSize:           1000,
		FlushInterval:          1 * time.Second,
		RateLimit:              4.5, // shared across ALL Route53 API calls (reads + writes + GetChange)
		MaxBisectDepth:         10,
		MaxPriorRequestRetries: 15, // 15 retries × 2s = 30s max wait
	}
}

// batchResult is the result delivered to a reconciler after its change has been
// processed as part of a batch.
type batchResult struct {
	changeInfo *svcsdktypes.ChangeInfo
	err        error
}

// pendingChange represents a single change waiting to be batched.
type pendingChange struct {
	action    svcsdktypes.ChangeAction
	recordSet *svcsdktypes.ResourceRecordSet
	resultCh  chan batchResult
}

// zoneBatch accumulates pending changes for a single hosted zone.
type zoneBatch struct {
	mu           sync.Mutex
	hostedZoneID string
	pending      []pendingChange
	timer        *time.Timer
	dispatcher   *BatchDispatcher
}

// BatchDispatcher collects individual Route53 changes and flushes them as
// batched ChangeResourceRecordSets calls. It sits between the per-CR
// reconcilers and the Route53 API.
type BatchDispatcher struct {
	mu          sync.Mutex
	zones       map[string]*zoneBatch
	sdkapi      *svcsdk.Client
	metrics     *ackmetrics.Metrics
	log         logr.Logger
	config      BatchConfig
	rateLimiter *rate.Limiter

	// executeFunc is the function used to execute a batch. It defaults to
	// executeBatch but can be overridden for testing.
	executeFunc func(hostedZoneID string, pending []pendingChange, depth int, priorRetries int)

	// shutdownCh signals graceful shutdown — flush all pending batches.
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewBatchDispatcher creates a new BatchDispatcher with the given configuration.
func NewBatchDispatcher(
	sdkapi *svcsdk.Client,
	metrics *ackmetrics.Metrics,
	log logr.Logger,
	config BatchConfig,
) *BatchDispatcher {
	return NewBatchDispatcherWithLimiter(sdkapi, metrics, log, config, nil)
}

// NewBatchDispatcherWithLimiter creates a new BatchDispatcher with a shared rate limiter.
// If limiter is nil, a new limiter is created from config.RateLimit.
func NewBatchDispatcherWithLimiter(
	sdkapi *svcsdk.Client,
	metrics *ackmetrics.Metrics,
	log logr.Logger,
	config BatchConfig,
	limiter *rate.Limiter,
) *BatchDispatcher {
	if limiter == nil {
		limiter = rate.NewLimiter(rate.Limit(config.RateLimit), 1)
	}
	bd := &BatchDispatcher{
		zones:       make(map[string]*zoneBatch),
		sdkapi:      sdkapi,
		metrics:     metrics,
		log:         log.WithName("batch-dispatcher"),
		config:      config,
		rateLimiter: limiter,
		shutdownCh:  make(chan struct{}),
	}
	bd.executeFunc = bd.executeBatch
	return bd
}

// Enqueue adds a change to the batch for the given hosted zone. It blocks
// until the batch containing this change is flushed and a result is available.
// The context can be used to cancel waiting (e.g. reconciler timeout).
func (bd *BatchDispatcher) Enqueue(
	ctx context.Context,
	hostedZoneID string,
	action svcsdktypes.ChangeAction,
	recordSet *svcsdktypes.ResourceRecordSet,
) batchResult {
	resultCh := make(chan batchResult, 1)

	change := pendingChange{
		action:    action,
		recordSet: recordSet,
		resultCh:  resultCh,
	}

	zb := bd.getOrCreateZoneBatch(hostedZoneID)
	zb.mu.Lock()
	zb.pending = append(zb.pending, change)
	pendingCount := len(zb.pending)

	// Start timer on first item
	if pendingCount == 1 {
		zb.timer = time.AfterFunc(bd.config.FlushInterval, func() {
			bd.flushZone(hostedZoneID)
		})
	}

	// Flush immediately if we hit the batch size limit
	if pendingCount >= bd.config.MaxBatchSize {
		if zb.timer != nil {
			zb.timer.Stop()
		}
		zb.mu.Unlock()
		bd.flushZone(hostedZoneID)
	} else {
		zb.mu.Unlock()
	}

	// Block until result or context cancellation.
	// NOTE: If context is cancelled, the change remains in the pending batch.
	// The batch will still flush and the Route53 call may still succeed.
	// This is acceptable because:
	// - For UPSERT/CREATE: the record gets created in Route53; the reconciler
	//   will retry, sdkFind will find it exists, and skip re-creation.
	// - For DELETE: the record gets deleted; next reconcile finds NotFound.
	// The alternative (removing the item from the batch) is racy and complex.
	select {
	case res := <-resultCh:
		return res
	case <-ctx.Done():
		return batchResult{err: ctx.Err()}
	}
}

// Shutdown gracefully flushes all pending batches and waits for completion.
func (bd *BatchDispatcher) Shutdown(ctx context.Context) {
	close(bd.shutdownCh)

	// Flush all zones
	bd.mu.Lock()
	zoneIDs := make([]string, 0, len(bd.zones))
	for id := range bd.zones {
		zoneIDs = append(zoneIDs, id)
	}
	bd.mu.Unlock()

	for _, id := range zoneIDs {
		bd.flushZone(id)
	}

	bd.wg.Wait()
}

// getOrCreateZoneBatch returns the zoneBatch for the given zone, creating one if needed.
func (bd *BatchDispatcher) getOrCreateZoneBatch(hostedZoneID string) *zoneBatch {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	zb, ok := bd.zones[hostedZoneID]
	if !ok {
		zb = &zoneBatch{
			hostedZoneID: hostedZoneID,
			dispatcher:   bd,
		}
		bd.zones[hostedZoneID] = zb
	}
	return zb
}

// flushZone drains the pending changes for a zone and sends them to Route53.
func (bd *BatchDispatcher) flushZone(hostedZoneID string) {
	bd.mu.Lock()
	zb, ok := bd.zones[hostedZoneID]
	bd.mu.Unlock()
	if !ok {
		return
	}

	zb.mu.Lock()
	if len(zb.pending) == 0 {
		zb.mu.Unlock()
		return
	}
	pending := zb.pending
	zb.pending = nil
	if zb.timer != nil {
		zb.timer.Stop()
		zb.timer = nil
	}
	zb.mu.Unlock()

	bd.wg.Add(1)
	go func() {
		defer bd.wg.Done()
		bd.executeFunc(hostedZoneID, pending, 0, 0)
	}()
}

// executeBatch sends a batch of changes to Route53 and delivers results.
func (bd *BatchDispatcher) executeBatch(hostedZoneID string, pending []pendingChange, depth int, priorRetries int) {
	if len(pending) == 0 {
		return
	}

	// Rate limit
	ctx := context.Background()
	if err := bd.rateLimiter.Wait(ctx); err != nil {
		bd.deliverError(pending, err)
		return
	}

	// Build ChangeBatch
	changes := make([]svcsdktypes.Change, len(pending))
	for i, p := range pending {
		changes[i] = svcsdktypes.Change{
			Action:            p.action,
			ResourceRecordSet: p.recordSet,
		}
	}

	input := &svcsdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(hostedZoneID),
		ChangeBatch:  &svcsdktypes.ChangeBatch{Changes: changes},
	}

	resp, err := bd.sdkapi.ChangeResourceRecordSets(ctx, input)
	bd.metrics.RecordAPICall("BATCH", "ChangeResourceRecordSets", err)

	bd.log.V(1).Info("batch flush",
		"zone", hostedZoneID,
		"size", len(pending),
		"error", err,
	)

	if err == nil {
		// Success — update the read dispatcher's last-result so subsequent
		// sdkFind calls see the newly written records immediately.
		if globalReadDispatcher != nil {
			globalReadDispatcher.UpsertRecords(hostedZoneID, changes)
		}
		// Deliver result to all waiters
		for _, p := range pending {
			p.resultCh <- batchResult{changeInfo: resp.ChangeInfo}
		}
		return
	}

	// Check if this is an InvalidChangeBatch error (all-or-none rejection)
	if bd.isInvalidChangeBatch(err) && len(pending) > 1 && depth < bd.config.MaxBisectDepth {
		bd.log.Info("batch rejected, bisecting",
			"zone", hostedZoneID,
			"size", len(pending),
			"depth", depth,
		)
		bd.bisectAndRetry(hostedZoneID, pending, depth)
		return
	}

	// Check for PriorRequestNotComplete — retry the whole batch after a delay
	if bd.isPriorRequestNotComplete(err) && priorRetries < bd.config.MaxPriorRequestRetries {
		bd.log.Info("PriorRequestNotComplete, retrying after delay",
			"zone", hostedZoneID,
			"size", len(pending),
			"retry", priorRetries+1,
		)
		time.Sleep(2 * time.Second)
		bd.executeBatch(hostedZoneID, pending, depth, priorRetries+1)
		return
	}

	// Unrecoverable error — deliver to all waiters
	bd.deliverError(pending, err)
}

// bisectAndRetry splits a failed batch in half and retries each half independently.
// This isolates invalid records so valid ones can still succeed.
func (bd *BatchDispatcher) bisectAndRetry(hostedZoneID string, pending []pendingChange, depth int) {
	if len(pending) == 1 {
		// Single record is invalid — deliver error
		bd.deliverError(pending, fmt.Errorf("InvalidChangeBatch: record %s (%s) is invalid",
			aws.ToString(pending[0].recordSet.Name),
			string(pending[0].action)))
		return
	}

	mid := len(pending) / 2
	left := pending[:mid]
	right := pending[mid:]

	// Execute both halves (sequentially to avoid exceeding rate limit)
	bd.executeBatch(hostedZoneID, left, depth+1, 0)
	bd.executeBatch(hostedZoneID, right, depth+1, 0)
}

// deliverError sends an error result to all pending changes.
func (bd *BatchDispatcher) deliverError(pending []pendingChange, err error) {
	for _, p := range pending {
		p.resultCh <- batchResult{err: err}
	}
}

// isInvalidChangeBatch checks if the error is an InvalidChangeBatch error
// from Route53 (meaning one or more records in the batch are invalid).
func (bd *BatchDispatcher) isInvalidChangeBatch(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "InvalidChangeBatch"
	}
	return false
}

// isPriorRequestNotComplete checks if the error indicates a prior change
// to the same hosted zone hasn't finished propagating yet.
func (bd *BatchDispatcher) isPriorRequestNotComplete(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "PriorRequestNotComplete"
	}
	return false
}
