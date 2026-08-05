# Queue-Level Batching Design: Verdict & Recommended Approach

## Executive Summary

**Queue-level batching (Option 2) is NOT viable** as originally conceived — you cannot make controller-runtime's work queue hand multiple items to a single `Reconcile()` call without forking it. However, a **hybrid approach** achieves the same goal without forking: use a **batch aggregator goroutine** that sits between the standard reconciler and Route53, collecting items by hosted zone and flushing them in bulk.

This is effectively "Approach B" from CONTEXT.md refined into something that works within controller-runtime's constraints.

---

## Why Pure Queue-Level Batching Is Not Viable

### Barrier 1: `processNextWorkItem()` is hard-coded to single-item dequeue

In `controller-runtime@v0.23.0/pkg/internal/controller/controller.go` (line ~290):

```go
func (c *Controller[request]) processNextWorkItem(ctx context.Context) bool {
    obj, priority, shutdown := c.Queue.GetWithPriority()
    // ... processes exactly ONE item
    c.reconcileHandler(ctx, obj, priority)
    return true
}
```

There's no extension point, callback, or interface to override this. Each worker goroutine pulls exactly one item and calls exactly one `Reconcile()`.

### Barrier 2: The `Reconciler` interface is `1 request → 1 result`

```go
type Reconciler interface {
    Reconcile(ctx context.Context, req Request) (Result, error)
}
```

There is no `ReconcileBatch([]Request)` variant. The contract between queue and reconciler is fundamentally one-to-one.

### Barrier 3: The queue has no "peek" or "drain N items" API

The `PriorityQueue` interface exposes:
- `Add/AddWithOpts` — enqueue
- `GetWithPriority()` — dequeue exactly one (blocking)
- `Done(item)` — mark complete
- `Forget(item)` — clear rate limiter

There's no `PeekN()`, `DrainByKey()`, or `GetBatch()`. Implementing these would require forking the priority queue.

### Barrier 4: ACK runtime hardcodes the reconciler binding

In `ack-runtime@v0.61.0/pkg/runtime/reconciler.go` line 110:

```go
return ctrlrt.NewControllerManagedBy(mgr).
    For(rd.EmptyRuntimeObject()).
    WithEventFilter(predicate.GenerationChangedPredicate{}).
    WithOptions(ctrlrtcontroller.Options{
        MaxConcurrentReconciles: maxConcurrentReconciles,
    }).Complete(r)  // r is the single-item reconciler
```

The generated code calls `.Complete(r)` with the standard reconciler. A custom queue would need this call to use a different reconciler, requiring forking ACK runtime.

### Conclusion

A pure "replace the queue" or "make the queue batch" approach requires forking **both** controller-runtime and ACK runtime. That's too much blast radius for a POC.

---

## Recommended Design: Batch Aggregator Pattern

### Core Idea

Keep controller-runtime's standard 1:1 dequeue→reconcile flow intact. Instead of making the queue batch, make the **reconciler itself** delegate to a shared batch aggregator that collects work by hosted zone and flushes periodically.

```
┌──────────────────────────────────────────────────────────────┐
│  controller-runtime (unmodified)                              │
│                                                               │
│  Worker 1 ──Get()──→ Reconcile(req1) ──┐                     │
│  Worker 2 ──Get()──→ Reconcile(req2) ──┼──→ BatchAggregator  │
│  Worker N ──Get()──→ Reconcile(reqN) ──┘         │            │
│                                                   ▼            │
│                                          flush per zone       │
│                                          every 200ms          │
│                                                   │            │
│                                                   ▼            │
│                                    ChangeResourceRecordSets   │
│                                    (up to 500 changes/call)   │
└──────────────────────────────────────────────────────────────┘
```

### Architecture

```go
// pkg/batch/aggregator.go

type RecordChange struct {
    Request    reconcile.Request       // Original k8s request
    Action     route53types.ChangeAction // CREATE, UPSERT, DELETE
    RecordSet  *route53types.ResourceRecordSet
    ResultChan chan<- ChangeResult      // Reconciler blocks on this
}

type ChangeResult struct {
    ChangeInfo *route53types.ChangeInfo
    Err        error
}

type BatchAggregator struct {
    mu          sync.Mutex
    pending     map[string][]RecordChange  // key: hostedZoneID
    flushTimer  *time.Timer
    maxBatch    int           // max changes per flush (Route53 limit: 1000)
    maxWait     time.Duration // max time to wait before flushing (e.g. 200ms)
    sdkClient   *route53.Client
    metrics     *ackmetrics.Metrics
}
```

### Flow

1. **Standard reconcile fires** — controller-runtime dequeues one RecordSet CR and calls `Reconcile()`.

2. **Reconciler builds the change** — reads the CR spec, determines the action (CREATE/UPSERT/DELETE), constructs the `ResourceRecordSet` struct. This includes the `GetHostedZone` call to resolve the domain (which we can cache).

3. **Reconciler submits to aggregator** — instead of calling `ChangeResourceRecordSets` directly, it sends a `RecordChange` to the `BatchAggregator` and blocks on the result channel.

4. **Aggregator collects by zone** — groups changes by `hostedZoneID`. Starts a flush timer on first item for a zone.

5. **Aggregator flushes** — when either:
   - `maxWait` (200ms) expires, OR
   - `maxBatch` (500) items accumulated for a zone
   
   It issues ONE `ChangeResourceRecordSets` call with all accumulated changes for that zone.

6. **Results fan back out** — the aggregator sends success/failure to each `RecordChange.ResultChan`. The individual reconcilers unblock and update their CR status.

### Why This Works

| Concern | Answer |
|---------|--------|
| Doesn't require forking controller-runtime | ✅ Standard queue, standard reconciler interface |
| Doesn't break ACK code generation | ✅ Change is in hooks.go (custom code), not generated sdk.go |
| Respects Route53's 5 req/s limit | ✅ One call per zone per flush window replaces N calls |
| Handles errors per-CR | ✅ Route53 returns per-change errors; aggregator routes them back |
| Works with any MaxConcurrentReconciles | ✅ More workers = more items buffered per window = bigger batches |

### Modifications Needed

#### 1. New package: `pkg/batch/`

```
pkg/batch/
├── aggregator.go        # Core batching logic
├── aggregator_test.go   # Unit tests
└── hosted_zone_cache.go # Cache GetHostedZone results
```

#### 2. Modified: `pkg/resource/record_set/hooks.go`

Replace the direct `ChangeResourceRecordSets` calls in `customUpdateRecordSet` (and similar create/delete hooks) with a call to the batch aggregator:

```go
func (rm *resourceManager) customUpdateRecordSet(
    ctx context.Context,
    desired *resource,
    latest *resource,
    delta *ackcompare.Delta,
) (updated *resource, err error) {
    ko := desired.ko.DeepCopy()

    recordSet, err := rm.newResourceRecordSet(ctx, desired)
    if err != nil {
        return nil, err
    }

    // Submit to batch aggregator instead of calling API directly
    result := rm.batchAggregator.Submit(ctx, batch.RecordChange{
        HostedZoneID: aws.ToString(desired.ko.Spec.HostedZoneID),
        Action:       svcsdktypes.ChangeActionUpsert,
        RecordSet:    recordSet,
    })

    if result.Err != nil {
        ko.Status.ID = nil
        ko.Status.Status = nil
        ko.Status.SubmittedAt = nil
        return &resource{ko}, result.Err
    }

    // Update status from the shared ChangeInfo
    ko.Status.ID = result.ChangeInfo.Id
    ko.Status.Status = aws.String(string(result.ChangeInfo.Status))
    if result.ChangeInfo.SubmittedAt != nil {
        ko.Status.SubmittedAt = &metav1.Time{Time: *result.ChangeInfo.SubmittedAt}
    }

    rm.setStatusDefaults(ko)
    return &resource{ko}, nil
}
```

#### 3. Modified: `pkg/resource/record_set/manager.go` or `manager_factory.go`

Inject the shared `BatchAggregator` into the resource manager at construction time. The aggregator is a singleton shared across all record_set reconciler workers.

#### 4. Modified: `cmd/controller/main.go`

Initialize the `BatchAggregator` before binding controllers. Pass it to the resource manager factory.

### Hosted Zone Domain Cache

Currently every reconcile calls `GetHostedZone` to resolve the domain name. This costs 1 API call per reconcile. Add a simple in-memory cache:

```go
type HostedZoneCache struct {
    mu    sync.RWMutex
    zones map[string]string // hostedZoneID → domain
    ttl   time.Duration
}
```

This alone cuts API calls from ~3-4 per reconcile to ~2 (just `ChangeResourceRecordSets` + `GetChange`). With batching, N records in the same zone use 1 `ChangeResourceRecordSets` + 1 `GetChange` = 2 calls total instead of 2N.

### Eliminating `GetChange` (syncStatus)

The `syncStatus()` call adds another API call per reconcile. For the batch case, we can:
1. Skip it entirely — Route53 changes propagate in <60s typically
2. OR do one `GetChange` per batch (not per CR) and share the result

For maximum throughput, option 1 is best: set status to PENDING, use a requeue-after to check later.

### Expected Performance

| Scenario | API calls | Effective ops/sec |
|----------|-----------|-------------------|
| Baseline (1 CR = 3-4 calls) | 3-4 per CR | ~1.5 |
| Batched, 50 CRs/zone, 200ms window | 2 per batch of ~50 | **~40-50** |
| Batched, conservative (10/batch) | 2 per batch of 10 | **~15-20** |

With `MaxConcurrentReconciles=10` and a 200ms flush window, we'd accumulate ~10 items per flush. That's 2 API calls for 10 records = 5 records/sec per API call. Well within the 5 req/s limit and delivering **>10 ops/sec**.

With `MaxConcurrentReconciles=50`, 200ms window can accumulate 50 items → **~50 ops/sec** from just 2 API calls.

### Configuration Knobs

```go
type AggregatorConfig struct {
    MaxWait       time.Duration // Default: 200ms
    MaxBatchSize  int           // Default: 500 (Route53 limit is 1000)
    MaxConcurrent int           // Max concurrent flush calls across all zones
}
```

### Error Handling

Route53's `ChangeResourceRecordSets` is **all-or-nothing** per call. If one change in the batch is invalid, the entire batch fails. Strategies:

1. **Optimistic batch**: Submit all. On failure, fall back to individual submissions for each change in the failed batch. This gives best-case batch performance with worst-case graceful degradation.

2. **Validation pre-filter**: Check for obvious issues (missing fields, etc.) before including in batch.

3. **Conflict detection**: Two CRs targeting the same record name + type in the same batch is a conflict. The aggregator can detect this and serialize them.

Recommended: Strategy 1 (optimistic + fallback).

### Race Condition Analysis

**Concern**: Two reconciles for the same CR land in the same batch.

**Answer**: controller-runtime's workqueue already de-duplicates. The same NamespacedName can't be in-flight twice — `Get()` removes it from the queue, and it's only re-added after `Done()`. So this can't happen.

**Concern**: Two CRs targeting the same DNS record (name + type) but different Kubernetes objects.

**Answer**: Route53 UPSERT is idempotent on name+type+SetIdentifier. If they have different SetIdentifiers, they're different records and batch fine. If they have the SAME name+type+SetIdentifier, they're logically the same record managed by two CRs (a user error). The batch will apply the last one — same as sequential reconciliation.

---

## Implementation Plan

### Phase 1: Hosted Zone Cache (Quick Win)
- Add in-memory cache for `GetHostedZone` results
- Cuts baseline API usage by 25-33%
- No architectural changes needed

### Phase 2: Batch Aggregator Core
- Implement `pkg/batch/aggregator.go`
- Wire into hooks.go for create/update/delete
- Inject via manager factory

### Phase 3: Eliminate Per-CR syncStatus
- Return PENDING status immediately
- Requeue-after 30s to check `GetChange` once
- Optionally batch `GetChange` calls too

### Phase 4: Benchmark & Tune
- Run the test harness with different configurations
- Tune `maxWait`, `maxBatch`, `MaxConcurrentReconciles`
- Verify >8 ops/sec target

---

## Blast Radius

| Component | Modified? | Notes |
|-----------|-----------|-------|
| controller-runtime | ❌ No | Used as-is |
| ACK runtime | ❌ No | Used as-is |
| Generated code (sdk.go) | ❌ No | Untouched |
| hooks.go | ✅ Yes | Route53 controller-specific custom code |
| manager_factory.go | ✅ Yes | Inject aggregator |
| cmd/controller/main.go | ✅ Yes | Initialize aggregator |
| New pkg/batch/ | ✅ Yes | New code, fully isolated |

This is **Route53-controller only**. No changes to shared ACK infrastructure.
