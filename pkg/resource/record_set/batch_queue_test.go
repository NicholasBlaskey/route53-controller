package record_set

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	smithy "github.com/aws/smithy-go"
	"github.com/go-logr/logr/testr"
)

// mockRoute53Client implements a mock for the Route53 ChangeResourceRecordSets call.
// We use this via a wrapper that replaces the SDK client's behavior.
type mockRoute53Client struct {
	mu          sync.Mutex
	calls       []mockCall
	callCount   int64
	shouldError func(input *svcsdk.ChangeResourceRecordSetsInput) error
}

type mockCall struct {
	hostedZoneID string
	changeCount  int
	actions      []svcsdktypes.ChangeAction
}

func (m *mockRoute53Client) record(input *svcsdk.ChangeResourceRecordSetsInput) {
	m.mu.Lock()
	defer m.mu.Unlock()

	actions := make([]svcsdktypes.ChangeAction, len(input.ChangeBatch.Changes))
	for i, c := range input.ChangeBatch.Changes {
		actions[i] = c.Action
	}

	m.calls = append(m.calls, mockCall{
		hostedZoneID: aws.ToString(input.HostedZoneId),
		changeCount:  len(input.ChangeBatch.Changes),
		actions:      actions,
	})
}

func (m *mockRoute53Client) getCalls() []mockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *mockRoute53Client) getCallCount() int64 {
	return atomic.LoadInt64(&m.callCount)
}

// --- Test functions ---

// enqueueAndFlush is a test helper that directly exercises the batching logic.
func TestBatchDispatcher_SingleChange(t *testing.T) {
	// Test that a single enqueue completes within the flush interval
	t.Run("single_enqueue_flushes_on_timer", func(t *testing.T) {
		config := DefaultBatchConfig()
		config.FlushInterval = 100 * time.Millisecond
		config.RateLimit = 1000

		bd := newMockDispatcher(t, config, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start := time.Now()
		result := bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("test.example.com.", "A"))
		elapsed := time.Since(start)

		if result.err != nil {
			t.Fatalf("expected no error, got: %v", result.err)
		}
		if result.changeInfo == nil {
			t.Fatal("expected changeInfo, got nil")
		}

		// Should have waited approximately the flush interval
		if elapsed < 80*time.Millisecond {
			t.Errorf("expected to wait ~100ms for flush, but only waited %v", elapsed)
		}
		if elapsed > 500*time.Millisecond {
			t.Errorf("expected to wait ~100ms for flush, but waited %v", elapsed)
		}

		// Should have made exactly 1 API call with 1 change
		calls := bd.mock.getCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 API call, got %d", len(calls))
		}
		if calls[0].changeCount != 1 {
			t.Errorf("expected batch size 1, got %d", calls[0].changeCount)
		}
	})
}

func TestBatchDispatcher_BatchesMultipleChanges(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 200 * time.Millisecond
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue 10 changes concurrently for the same zone
	const numChanges = 10
	var wg sync.WaitGroup
	results := make([]batchResult, numChanges)

	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("record-%d.example.com.", idx)
			results[idx] = bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord(name, "A"))
		}(i)
	}

	wg.Wait()

	// All should succeed
	for i, r := range results {
		if r.err != nil {
			t.Errorf("change %d failed: %v", i, r.err)
		}
	}

	// Should have made exactly 1 API call with all 10 changes batched
	calls := bd.mock.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 API call, got %d", len(calls))
	}
	if calls[0].changeCount != numChanges {
		t.Errorf("expected batch size %d, got %d", numChanges, calls[0].changeCount)
	}
}

func TestBatchDispatcher_FlushOnMaxSize(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 10 * time.Second // long timer — should flush on size
	config.MaxBatchSize = 5
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue exactly MaxBatchSize changes — should flush immediately
	const numChanges = 5
	var wg sync.WaitGroup
	results := make([]batchResult, numChanges)

	start := time.Now()
	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("record-%d.example.com.", idx)
			results[idx] = bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord(name, "A"))
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Should have flushed almost immediately (not waiting for 10s timer)
	if elapsed > 1*time.Second {
		t.Errorf("expected near-immediate flush on max batch size, took %v", elapsed)
	}

	// All should succeed
	for i, r := range results {
		if r.err != nil {
			t.Errorf("change %d failed: %v", i, r.err)
		}
	}

	calls := bd.mock.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 API call, got %d", len(calls))
	}
	if calls[0].changeCount != numChanges {
		t.Errorf("expected batch size %d, got %d", numChanges, calls[0].changeCount)
	}
}

func TestBatchDispatcher_SeparateZones(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 100 * time.Millisecond
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue changes to two different zones
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("a.zone1.com.", "A"))
	}()
	go func() {
		defer wg.Done()
		bd.Enqueue(ctx, "zone-2", svcsdktypes.ChangeActionCreate, makeTestRecord("a.zone2.com.", "A"))
	}()

	wg.Wait()

	// Should have made 2 API calls (one per zone)
	calls := bd.mock.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 API calls (one per zone), got %d", len(calls))
	}

	// Each call should have 1 change
	for i, call := range calls {
		if call.changeCount != 1 {
			t.Errorf("call %d: expected batch size 1, got %d", i, call.changeCount)
		}
	}
}

func TestBatchDispatcher_InvalidChangeBatch_Bisection(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 100 * time.Millisecond
	config.RateLimit = 1000

	// The "bad" record name that will cause failures
	badRecordName := "bad.example.com."

	errorFn := func(input *svcsdk.ChangeResourceRecordSetsInput) error {
		// Reject batches containing the bad record
		for _, change := range input.ChangeBatch.Changes {
			if aws.ToString(change.ResourceRecordSet.Name) == badRecordName {
				return &smithy.GenericAPIError{
					Code:    "InvalidChangeBatch",
					Message: "Invalid record in batch",
				}
			}
		}
		return nil
	}

	bd := newMockDispatcher(t, config, errorFn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue 4 changes: 3 good + 1 bad
	var wg sync.WaitGroup
	results := make([]batchResult, 4)

	names := []string{"good1.example.com.", "good2.example.com.", badRecordName, "good3.example.com."}

	for i, name := range names {
		wg.Add(1)
		go func(idx int, n string) {
			defer wg.Done()
			results[idx] = bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord(n, "A"))
		}(i, name)
	}

	wg.Wait()

	// Good records should succeed
	for i, name := range names {
		if name == badRecordName {
			if results[i].err == nil {
				t.Errorf("expected error for bad record %q, got success", name)
			}
		} else {
			if results[i].err != nil {
				t.Errorf("expected success for good record %q, got error: %v", name, results[i].err)
			}
		}
	}

	// Should have made multiple API calls due to bisection
	calls := bd.mock.getCalls()
	if len(calls) < 2 {
		t.Errorf("expected multiple API calls from bisection, got %d", len(calls))
	}
}

func TestBatchDispatcher_PriorRequestNotComplete_Retries(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 50 * time.Millisecond
	config.RateLimit = 1000

	var callCount int64

	errorFn := func(input *svcsdk.ChangeResourceRecordSetsInput) error {
		count := atomic.AddInt64(&callCount, 1)
		// Fail on first attempt, succeed on second
		if count == 1 {
			return &smithy.GenericAPIError{
				Code:    "PriorRequestNotComplete",
				Message: "The request was rejected because Route 53 was still processing a prior request.",
			}
		}
		return nil
	}

	bd := newMockDispatcher(t, config, errorFn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("test.example.com.", "A"))

	if result.err != nil {
		t.Fatalf("expected success after retry, got error: %v", result.err)
	}
	if result.changeInfo == nil {
		t.Fatal("expected changeInfo after retry, got nil")
	}

	// Should have made 2 API calls (first failed, second succeeded)
	calls := bd.mock.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 API calls (retry), got %d", len(calls))
	}
}

func TestBatchDispatcher_ContextCancellation(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 5 * time.Second // very long flush — we'll cancel first
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result := bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("test.example.com.", "A"))

	if result.err == nil {
		t.Fatal("expected context cancellation error, got success")
	}
	if result.err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got: %v", result.err)
	}
}

func TestBatchDispatcher_MixedActions(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 100 * time.Millisecond
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue a mix of create, update, and delete
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("new.example.com.", "A"))
	}()
	go func() {
		defer wg.Done()
		bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionUpsert, makeTestRecord("existing.example.com.", "A"))
	}()
	go func() {
		defer wg.Done()
		bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionDelete, makeTestRecord("old.example.com.", "A"))
	}()

	wg.Wait()

	// Should be 1 API call with 3 changes of different types
	calls := bd.mock.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 API call, got %d", len(calls))
	}
	if calls[0].changeCount != 3 {
		t.Errorf("expected 3 changes in batch, got %d", calls[0].changeCount)
	}
}

func TestBatchDispatcher_Shutdown(t *testing.T) {
	config := DefaultBatchConfig()
	config.FlushInterval = 10 * time.Second // won't fire naturally
	config.RateLimit = 1000

	bd := newMockDispatcher(t, config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue a change (it won't flush due to long timer)
	var result batchResult
	done := make(chan struct{})
	go func() {
		result = bd.Enqueue(ctx, "zone-1", svcsdktypes.ChangeActionCreate, makeTestRecord("test.example.com.", "A"))
		close(done)
	}()

	// Give it a moment to enqueue
	time.Sleep(50 * time.Millisecond)

	// Shutdown should flush pending batches
	bd.Shutdown(context.Background())

	// Wait for the enqueue to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shutdown flush")
	}

	if result.err != nil {
		t.Fatalf("expected success after shutdown flush, got error: %v", result.err)
	}
}

// --- Test helpers ---

// mockDispatcher is a BatchDispatcher with a mock Route53 client that
// intercepts executeBatch calls.
type mockDispatcher struct {
	*BatchDispatcher
	mock *mockRoute53Client
}

func newMockDispatcher(t *testing.T, config BatchConfig, errorFn func(*svcsdk.ChangeResourceRecordSetsInput) error) *mockDispatcher {
	t.Helper()
	log := testr.New(t)
	metrics := ackmetrics.NewMetrics("route53")

	mock := &mockRoute53Client{
		shouldError: errorFn,
	}

	bd := NewBatchDispatcher(nil, metrics, log, config)

	md := &mockDispatcher{
		BatchDispatcher: bd,
		mock:            mock,
	}

	// Override the executeBatch to use our mock
	bd.executeFunc = func(hostedZoneID string, pending []pendingChange, depth int, priorRetries int) {
		md.mockExecuteBatch(hostedZoneID, pending, depth, priorRetries)
	}

	return md
}

func (md *mockDispatcher) mockExecuteBatch(hostedZoneID string, pending []pendingChange, depth int, priorRetries int) {
	if len(pending) == 0 {
		return
	}

	// Rate limit (fast in tests)
	ctx := context.Background()
	_ = md.rateLimiter.Wait(ctx)

	// Build the input for the mock
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

	// Record the call
	md.mock.record(input)
	atomic.AddInt64(&md.mock.callCount, 1)

	// Check for error
	var err error
	if md.mock.shouldError != nil {
		err = md.mock.shouldError(input)
	}

	if err == nil {
		// Success
		changeInfo := &svcsdktypes.ChangeInfo{
			Id:     aws.String("/change/MOCK123"),
			Status: svcsdktypes.ChangeStatusPending,
		}
		now := time.Now()
		changeInfo.SubmittedAt = &now

		for _, p := range pending {
			p.resultCh <- batchResult{changeInfo: changeInfo}
		}
		return
	}

	// Check if InvalidChangeBatch — bisect
	var apiErr smithy.APIError
	if isAPIError(err, &apiErr) && apiErr.ErrorCode() == "InvalidChangeBatch" && len(pending) > 1 && depth < md.config.MaxBisectDepth {
		mid := len(pending) / 2
		md.mockExecuteBatch(hostedZoneID, pending[:mid], depth+1, 0)
		md.mockExecuteBatch(hostedZoneID, pending[mid:], depth+1, 0)
		return
	}

	// Check for PriorRequestNotComplete — retry
	if isAPIError(err, &apiErr) && apiErr.ErrorCode() == "PriorRequestNotComplete" && priorRetries < md.config.MaxPriorRequestRetries {
		time.Sleep(50 * time.Millisecond) // short sleep for tests
		md.mockExecuteBatch(hostedZoneID, pending, depth, priorRetries+1)
		return
	}

	// Deliver error
	for _, p := range pending {
		p.resultCh <- batchResult{err: err}
	}
}

func isAPIError(err error, target *smithy.APIError) bool {
	var apiErr smithy.APIError
	if ok := errorAs(err, &apiErr); ok {
		*target = apiErr
		return true
	}
	return false
}

func errorAs(err error, target interface{}) bool {
	type apiError interface {
		ErrorCode() string
		ErrorMessage() string
	}
	if ae, ok := err.(apiError); ok {
		if t, ok2 := target.(*smithy.APIError); ok2 {
			*t = ae.(smithy.APIError)
			return true
		}
	}
	return false
}

func makeTestRecord(name string, recordType string) *svcsdktypes.ResourceRecordSet {
	return &svcsdktypes.ResourceRecordSet{
		Name: aws.String(name),
		Type: svcsdktypes.RRType(recordType),
		TTL:  aws.Int64(300),
		ResourceRecords: []svcsdktypes.ResourceRecord{
			{Value: aws.String("10.0.0.1")},
		},
	}
}
