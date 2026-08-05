package batch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
)

func testLogger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		// discard
	}, funcr.Options{Verbosity: 10})
}

// mockRoute53Client implements Route53API for testing.
type mockRoute53Client struct {
	mu        sync.Mutex
	calls     int
	callDelay time.Duration
	err       error
	// changeInfoFn allows customizing the response per call
	changeInfoFn func(input *route53.ChangeResourceRecordSetsInput) (*svcsdktypes.ChangeInfo, error)
}

func (m *mockRoute53Client) ChangeResourceRecordSets(ctx context.Context, input *route53.ChangeResourceRecordSetsInput, opts ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()

	if m.callDelay > 0 {
		time.Sleep(m.callDelay)
	}

	if m.changeInfoFn != nil {
		info, err := m.changeInfoFn(input)
		if err != nil {
			return nil, err
		}
		return &route53.ChangeResourceRecordSetsOutput{
			ChangeInfo: info,
		}, nil
	}

	if m.err != nil {
		return nil, m.err
	}

	now := time.Now()
	return &route53.ChangeResourceRecordSetsOutput{
		ChangeInfo: &svcsdktypes.ChangeInfo{
			Id:          aws.String("/change/BATCH123"),
			Status:      svcsdktypes.ChangeStatusPending,
			SubmittedAt: &now,
		},
	}, nil
}

func (m *mockRoute53Client) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestAggregator_BatchesByZone(t *testing.T) {
	client := &mockRoute53Client{}
	cfg := Config{
		MaxWait:      50 * time.Millisecond,
		MaxBatchSize: 100,
	}
	agg := NewAggregator(client, nil, testLogger(), cfg)
	defer agg.Stop()

	const numChanges = 10
	const hostedZoneID = "Z123456"

	var wg sync.WaitGroup
	results := make([]ChangeResult, numChanges)

	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = agg.Submit(context.Background(), RecordChange{
				HostedZoneID: hostedZoneID,
				Action:       svcsdktypes.ChangeActionUpsert,
				RecordSet: &svcsdktypes.ResourceRecordSet{
					Name: aws.String(fmt.Sprintf("record%d.example.com.", idx)),
					Type: svcsdktypes.RRTypeA,
					TTL:  aws.Int64(300),
					ResourceRecords: []svcsdktypes.ResourceRecord{
						{Value: aws.String("1.2.3.4")},
					},
				},
			})
		}(i)
	}

	wg.Wait()

	// All should have succeeded
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("change %d failed: %v", i, r.Err)
		}
		if r.ChangeInfo == nil {
			t.Errorf("change %d has nil ChangeInfo", i)
		}
	}

	// Should have made exactly 1 API call (all same zone, within max wait)
	calls := client.CallCount()
	if calls != 1 {
		t.Errorf("expected 1 API call, got %d", calls)
	}
}

func TestAggregator_SeparatesZones(t *testing.T) {
	client := &mockRoute53Client{}
	cfg := Config{
		MaxWait:      50 * time.Millisecond,
		MaxBatchSize: 100,
	}
	agg := NewAggregator(client, nil, testLogger(), cfg)
	defer agg.Stop()

	var wg sync.WaitGroup

	zones := []string{"Z111", "Z222", "Z333"}
	for _, zone := range zones {
		wg.Add(1)
		go func(z string) {
			defer wg.Done()
			result := agg.Submit(context.Background(), RecordChange{
				HostedZoneID: z,
				Action:       svcsdktypes.ChangeActionUpsert,
				RecordSet: &svcsdktypes.ResourceRecordSet{
					Name: aws.String("test.example.com."),
					Type: svcsdktypes.RRTypeA,
					TTL:  aws.Int64(300),
					ResourceRecords: []svcsdktypes.ResourceRecord{
						{Value: aws.String("1.2.3.4")},
					},
				},
			})
			if result.Err != nil {
				t.Errorf("zone %s failed: %v", z, result.Err)
			}
		}(zone)
	}

	wg.Wait()

	// Should have made 3 API calls (one per zone)
	calls := client.CallCount()
	if calls != 3 {
		t.Errorf("expected 3 API calls, got %d", calls)
	}
}

func TestAggregator_MaxBatchFlush(t *testing.T) {
	client := &mockRoute53Client{}
	cfg := Config{
		MaxWait:      5 * time.Second, // long wait — should flush on batch size
		MaxBatchSize: 5,
	}
	agg := NewAggregator(client, nil, testLogger(), cfg)
	defer agg.Stop()

	var wg sync.WaitGroup
	const numChanges = 5
	const hostedZoneID = "Z999"

	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result := agg.Submit(context.Background(), RecordChange{
				HostedZoneID: hostedZoneID,
				Action:       svcsdktypes.ChangeActionCreate,
				RecordSet: &svcsdktypes.ResourceRecordSet{
					Name: aws.String(fmt.Sprintf("r%d.example.com.", idx)),
					Type: svcsdktypes.RRTypeA,
					TTL:  aws.Int64(300),
					ResourceRecords: []svcsdktypes.ResourceRecord{
						{Value: aws.String("10.0.0.1")},
					},
				},
			})
			if result.Err != nil {
				t.Errorf("change %d failed: %v", idx, result.Err)
			}
		}(i)
	}

	wg.Wait()

	calls := client.CallCount()
	if calls != 1 {
		t.Errorf("expected 1 API call (batch size triggered), got %d", calls)
	}
}

func TestAggregator_FallbackOnBatchError(t *testing.T) {
	callCount := &atomic.Int32{}
	client := &mockRoute53Client{
		changeInfoFn: func(input *route53.ChangeResourceRecordSetsInput) (*svcsdktypes.ChangeInfo, error) {
			n := callCount.Add(1)
			// First call (the batch) fails
			if n == 1 {
				return nil, fmt.Errorf("InvalidChangeBatch: batch error")
			}
			// Individual fallbacks succeed
			now := time.Now()
			return &svcsdktypes.ChangeInfo{
				Id:          aws.String(fmt.Sprintf("/change/FALLBACK%d", n)),
				Status:      svcsdktypes.ChangeStatusPending,
				SubmittedAt: &now,
			}, nil
		},
	}

	cfg := Config{
		MaxWait:      50 * time.Millisecond,
		MaxBatchSize: 100,
	}
	agg := NewAggregator(client, nil, testLogger(), cfg)
	defer agg.Stop()

	var wg sync.WaitGroup
	const numChanges = 3
	results := make([]ChangeResult, numChanges)

	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = agg.Submit(context.Background(), RecordChange{
				HostedZoneID: "ZFAIL",
				Action:       svcsdktypes.ChangeActionUpsert,
				RecordSet: &svcsdktypes.ResourceRecordSet{
					Name: aws.String(fmt.Sprintf("r%d.fail.com.", idx)),
					Type: svcsdktypes.RRTypeA,
					TTL:  aws.Int64(300),
					ResourceRecords: []svcsdktypes.ResourceRecord{
						{Value: aws.String("1.1.1.1")},
					},
				},
			})
		}(i)
	}

	wg.Wait()

	// All should succeed via fallback
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("change %d should have succeeded via fallback, got: %v", i, r.Err)
		}
		if r.ChangeInfo == nil {
			t.Errorf("change %d has nil ChangeInfo after fallback", i)
		}
	}

	// 1 batch call + 3 individual fallbacks = 4 total
	totalCalls := int(callCount.Load())
	if totalCalls != 4 {
		t.Errorf("expected 4 API calls (1 batch + 3 fallback), got %d", totalCalls)
	}
}

func TestAggregator_ContextCancellation(t *testing.T) {
	client := &mockRoute53Client{
		callDelay: 5 * time.Second, // intentionally slow
	}
	cfg := Config{
		MaxWait:      5 * time.Second,
		MaxBatchSize: 100,
	}
	agg := NewAggregator(client, nil, testLogger(), cfg)
	defer agg.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result := agg.Submit(ctx, RecordChange{
		HostedZoneID: "ZSLOW",
		Action:       svcsdktypes.ChangeActionUpsert,
		RecordSet: &svcsdktypes.ResourceRecordSet{
			Name: aws.String("slow.example.com."),
			Type: svcsdktypes.RRTypeA,
			TTL:  aws.Int64(300),
			ResourceRecords: []svcsdktypes.ResourceRecord{
				{Value: aws.String("1.2.3.4")},
			},
		},
	})

	if result.Err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestHostedZoneCache(t *testing.T) {
	cache := NewHostedZoneCache(100*time.Millisecond, testLogger())

	// Miss
	if _, ok := cache.Get("Z1"); ok {
		t.Error("expected cache miss")
	}

	// Put and hit
	cache.Put("Z1", "example.com.")
	domain, ok := cache.Get("Z1")
	if !ok || domain != "example.com." {
		t.Errorf("expected cache hit with 'example.com.', got %q, ok=%v", domain, ok)
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)
	if _, ok := cache.Get("Z1"); ok {
		t.Error("expected cache miss after TTL expiry")
	}

	// Invalidate
	cache.Put("Z2", "test.com.")
	cache.Invalidate("Z2")
	if _, ok := cache.Get("Z2"); ok {
		t.Error("expected cache miss after invalidate")
	}
}
