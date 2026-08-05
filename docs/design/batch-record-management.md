# Design: Batched ChangeResourceRecordSets for route53-controller

**Issue:** [aws-controllers-k8s/community#2966](https://github.com/aws-controllers-k8s/community/issues/2966)  
**Status:** Draft  
**Date:** 2026-07-27  

## Problem

As the number of hosted zones managed by the route53-controller grows, customers hit a hard scaling ceiling imposed by the Route53 API's account-level rate quota.

### 1. Account-wide rate quota is low and shared

Route53 enforces a hard limit of **5 API requests per second per AWS account** across *all* Route53 API actions — both reads (`ListResourceRecordSets`, `GetHostedZone`, `GetChange`) and writes (`ChangeResourceRecordSets`). Exceeding this returns HTTP 400 with `Code: Throttling`. ([source: AWS docs — "Maximums on API requests"](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/DNSLimitations.html#limits-api-requests))

Additionally, Route53 rejects a `ChangeResourceRecordSets` request for a hosted zone if a prior change to that same zone is still in-progress (`PriorRequestNotComplete`), effectively serializing writes per zone.

The route53-controller currently reconciles each `RecordSet` CR as its own individual API call (1 change per call). For a zone with N records, this means N serialized `ChangeResourceRecordSets` calls *plus* N `GetHostedZone` + N `ListResourceRecordSets` read calls for drift detection. A private hosted zone with ~1,450 records therefore generates ~4,350 API calls per full reconciliation cycle. At 5 req/s, that's ~14.5 minutes of solid API usage just for one zone — assuming zero throttling or retries.

### 2. Shared quota starves other consumers

The 5 req/s budget is shared across the entire AWS account. When the controller is churning through reconciliation of a large zone, it consumes the majority of the available API budget. Other actors in the same account — notably **external-dns**, Terraform, or other controllers — get throttled by proxy. This turns a single-controller scaling problem into an account-wide availability issue.

### 3. Route53 supports bulk operations but the controller doesn't use them

The Route53 API natively supports batching:

- **Writes:** `ChangeResourceRecordSets` accepts up to **1,000 `ResourceRecord` elements** per `ChangeBatch` (UPSERT counts each element twice toward this limit). A single API call can atomically create, update, or delete up to 1,000 records. ([source](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/DNSLimitations.html#limits-api-requests-changeresourcerecordsets))

  ```json
  {
    "HostedZoneId": "Z1234567890",
    "ChangeBatch": {
      "Changes": [
        { "Action": "UPSERT", "ResourceRecordSet": { ... } },
        { "Action": "CREATE", "ResourceRecordSet": { ... } },
        { "Action": "DELETE", "ResourceRecordSet": { ... } }
      ]
    }
  }
  ```

- **Reads:** `ListResourceRecordSets` returns up to 300 records per call and supports pagination, allowing a single pass to read an entire zone.

However, integrating batch semantics into the ACK reconciliation model is non-trivial. ACK reconciles each Custom Resource independently through its own reconciler loop. There is no built-in concept of "group these CRs and act on them together." The controller would need a custom batching layer that sits between the per-CR reconcilers and the Route53 API — collecting individual changes and coalescing them into fewer, larger API calls.

### Impact Summary

| Metric | Current (1,450-record zone) | With batching (1,000/batch) |
|--------|----------------------------|-----------------------------|
| Write API calls per full reconcile | ~1,450 | ~2 |
| Read API calls per full reconcile | ~2,900 | ~5 (paginated list) |
| Time to reconcile at 5 req/s (no errors) | ~14.5 min | ~1.5 seconds |
| Throttling risk to other account consumers | High | Negligible |

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Kubernetes API Server                                           │
│                                                                  │
│  RecordSet CR (1)   RecordSet CR (2)   ...   RecordSet CR (N)   │
└──────┬───────────────────┬────────────────────────┬─────────────┘
       │                   │                        │
       ▼                   ▼                        ▼
┌──────────────┐   ┌──────────────┐        ┌──────────────┐
│ Reconciler   │   │ Reconciler   │  ...   │ Reconciler   │
│ (1 CR)       │   │ (1 CR)       │        │ (1 CR)       │
└──────┬───────┘   └──────┬───────┘        └──────┬───────┘
       │                   │                        │
       ▼                   ▼                        ▼
  ChangeRRSets(1)    ChangeRRSets(1)         ChangeRRSets(1)
  (1 change)         (1 change)              (1 change)
```

Key code path:
- `generator.yaml` ignores `ChangeResourceRecordSetsInput.ChangeBatch` (the generated code doesn't set it)
- Hook templates (`sdk_create_post_build_request.go.tpl`, `sdk_delete_post_build_request.go.tpl`) inject a single-element batch via `newChangeBatch()`
- `customUpdateRecordSet()` in `hooks.go` also builds a single-element batch with `UPSERT`

Each reconcile loop for a single RecordSet CR:
1. Calls `GetHostedZone` (to resolve domain name)
2. Calls `ListResourceRecordSets` (to find current state)
3. Calls `ChangeResourceRecordSets` with 1 change (create/update/delete)

## Constraints & Requirements

| # | Requirement |
|---|---|
| R1 | Reduce Route53 API calls when many RecordSets in the same hosted zone change concurrently |
| R2 | Maintain per-CR status reporting (each RecordSet CR must reflect its own sync status) |
| R3 | Preserve the existing single-CR UX — users who apply one RecordSet at a time should see no behavioral change |
| R4 | Handle partial failures gracefully (Route53 ChangeBatch is all-or-none; if one record is invalid, entire batch fails) |
| R5 | Stay within Route53 limits: ≤1,000 changes per batch, ≤5 req/s per account |
| R6 | Backward-compatible: existing RecordSet CRs continue to work without modification |
| R7 | Interoperate with the ACK runtime reconciliation model (each CR has its own reconciler loop) |

## Options

---

### Option 1: API-Level Batching via Work Queue (Not Viable)

One way to solve this is to add a work queue at the API level. Every time the controller needs to submit a create, update, or delete, instead of calling Route53 immediately it pushes the change onto a per-hosted-zone queue. The queue waits a short window (e.g. 5 seconds) for additional requests to arrive. If multiple changes come in during that window, they get combined into a single `ChangeResourceRecordSets` call with multiple entries in the `ChangeBatch`.

The queue would flush when either the time window expires or the batch hits the 1,000-change Route53 limit. Each reconciler blocks until the batch it belongs to is flushed, then receives the shared result to update its CR's status.

This is not viable because it creates a dependency between CRs that should be independent. Route53's `ChangeBatch` has all-or-none semantics: if one record in the batch is invalid, the entire batch fails. One bad CR poisons the batch for every other CR that happened to land in the same window. You can work around this with bisection retry (split the batch in half, retry each half, recurse until you isolate the bad record), but this adds significant complexity for what should be a straightforward reconcile. Beyond the failure semantics, the approach also requires bolting a coordination layer (goroutines, locking, graceful shutdown, leader election awareness) onto a framework specifically designed to avoid coordination between CRs. It works, but it's gross.

---

### Option 2: Work Queue Level Batching (Not Viable)

Another way to solve this is at the controller-runtime work queue level. The reconciler queue has a list of RecordSet CRs waiting to be reconciled. In theory, we could inspect the queue, find items that belong to the same hosted zone, pull them out, and replace them with a single "bulk reconcile" task that handles all of them in one `ChangeResourceRecordSets` call.

This is not viable for two reasons. First, it has the same dependency problem as Option 1: independent CRs become coupled through the batch, and one bad record poisons the group. Second, controller-runtime's work queue treats each item as independent and opaque. There's no supported API to peek ahead, group items, or merge them. You'd be forking the reconciler machinery and breaking ACK's 1:1 CR-to-reconcile assumption throughout the generated code.

---

### Option 3: New "RecordSetGroup" CRD

**Concept:** Introduce a new Custom Resource (e.g., `RecordSetGroup`) that represents multiple record sets in a single object. The reconciler for this CRD builds a multi-element `ChangeBatch` directly.

```yaml
apiVersion: route53.services.k8s.aws/v1alpha1
kind: RecordSetGroup
metadata:
  name: my-zone-records
spec:
  hostedZoneID: Z1234567890
  recordSets:
    - name: app1.example.com
      recordType: A
      ttl: 300
      resourceRecords:
        - value: "1.2.3.4"
    - name: app2.example.com
      recordType: CNAME
      ttl: 300
      resourceRecords:
        - value: "app1.example.com"
    # ... up to 1000 records
```

**Implementation:**
- Define a new CRD `RecordSetGroup` with `spec.hostedZoneID` and `spec.recordSets[]`
- The reconciler diffs current vs desired record sets, builds a `ChangeBatch` with CREATE/DELETE/UPSERT actions as needed
- Single `ChangeResourceRecordSets` call per reconcile

**Pros:**
- Simple, explicit UX — user controls what goes in a batch
- Clean mapping to Route53 API semantics
- No concurrent goroutine complexity
- Easier all-or-none failure handling (user sees it at the group level)
- Similar to Terraform's `aws_route53_records_exclusive` pattern

**Cons:**
- New CRD to maintain; increased API surface
- Users must migrate from individual RecordSet CRs to RecordSetGroup (or use both, which is confusing)
- Loses fine-grained per-record status/conditions (the group succeeds or fails as a whole)
- Large CRs (1000 records in one object) are unwieldy in `kubectl`, etcd (1MB object size limit), and GitOps diffs
- Doesn't help existing users who already have hundreds of individual RecordSet CRs
- Reconciliation granularity: any change to any record in the group triggers a full diff/reconcile of all records

---

### Option 4: Controller-Side Coalescing with WorkQueue Deduplication

**Concept:** Leverage the controller-runtime work queue's rate-limiting and batching capabilities. Instead of processing each RecordSet CR event immediately, the controller accumulates keys for the same hosted zone and processes them together.

**Implementation:**
- Override the default reconciler to use a custom work queue that groups items by `hostedZoneID`
- When the queue drains items for a given zone, it processes up to 1000 of them in one `ChangeResourceRecordSets` call
- Uses controller-runtime's `MaxConcurrentReconciles` set to 1 per zone (or a custom multi-item reconciler)

**Sketch:**
```go
// Custom reconciler that pulls multiple items from the queue
func (r *RecordSetReconciler) ReconcileBatch(ctx context.Context, zone string, items []reconcile.Request) {
    // 1. Read all RecordSet CRs for this zone
    // 2. Diff against Route53 current state
    // 3. Build ChangeBatch with all needed changes
    // 4. Call ChangeResourceRecordSets
    // 5. Update status on each CR
}
```

**Pros:**
- Works with existing RecordSet CRs (no migration)
- Conceptually simpler than Option A's goroutine pool — leverages existing queue mechanics
- Natural rate limiting through queue drain interval

**Cons:**
- Requires deep customization of controller-runtime reconciler pattern (ACK's code-generated reconciler assumes 1:1 CR:reconcile)
- Conflicts with ACK runtime's standard reconciliation model — ACK doesn't support "batch reconcile" out of the box
- Complex interaction with ACK's drift detection, requeue, and status management
- Essentially requires forking/patching the ACK runtime for this controller

---

### Option 5: Rate-Limiting + Parallel Workers (Minimal Change)

**Concept:** Don't batch at the API level. Instead, add client-side rate limiting to stay within the 5 req/s budget, and allow parallel reconciliation to improve throughput within that budget.

**Implementation:**
- Add a per-account rate limiter (e.g., `golang.org/x/time/rate` at 4 req/s to leave headroom)
- Set `MaxConcurrentReconciles` appropriately
- Optionally add jitter to resync intervals to prevent thundering herd

**Pros:**
- Minimal code change — just wrap the SDK client with a rate limiter
- No new CRDs, no architectural changes
- Prevents throttling errors

**Cons:**
- Does NOT reduce the total number of API calls (still N calls for N records)
- 1,450 records at 5 req/s = ~290 seconds (nearly 5 minutes) for a full zone reconcile
- Doesn't help with the fundamental scaling problem
- Still wastes API budget vs. batching (1 call with 1000 changes uses the same 1 request as 1 call with 1 change)

---

## Comparison Matrix

| Criterion | A: Debounced Queue | B: RecordSetGroup CRD | C: WorkQueue Coalescing | D: Rate Limit Only |
|-----------|-------------------|----------------------|------------------------|-------------------|
| API call reduction | ✅ Excellent (N→1) | ✅ Excellent (N→1) | ✅ Excellent (N→1) | ❌ None |
| Backward compatible | ✅ Yes | ⚠️ New CRD, existing CRs unchanged | ⚠️ Deep runtime changes | ✅ Yes |
| Per-CR status | ✅ Preserved | ⚠️ Group-level only | ✅ Preserved | ✅ Preserved |
| Implementation complexity | ⚠️ Medium-High | ⚠️ Medium | ❌ High (runtime fork) | ✅ Low |
| ACK runtime compatibility | ⚠️ Custom layer | ✅ Standard ACK pattern | ❌ Requires runtime changes | ✅ Fully compatible |
| User migration needed | ✅ None | ❌ Opt-in to new CRD | ✅ None | ✅ None |
| Failure isolation | ⚠️ Needs bisect retry | ⚠️ Whole group fails | ⚠️ Needs bisect retry | ✅ Per-CR |
| etcd/object size concerns | ✅ None | ❌ Large objects | ✅ None | ✅ None |

## Recommendation

**Option A (Debounced Queue)** is recommended as the primary approach, optionally combined with **Option D** (rate limiting) as a safety net.

**Rationale:**
- It provides the best API efficiency while maintaining backward compatibility with existing RecordSet CRs
- Users don't need to change anything — the batching is transparent
- Per-CR status reporting is preserved
- The failure bisection strategy handles the all-or-none semantics cleanly
- Rate limiting (Option D) is trivial to add as a complementary safeguard

Option B (RecordSetGroup) could be offered as a future enhancement for users who prefer explicit batch control, but it shouldn't be the only solution since it requires user migration and doesn't help existing deployments.

## Detailed Design for Option A

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ACK Runtime                                │
│  ┌───────────┐  ┌───────────┐       ┌───────────┐          │
│  │RecordSet  │  │RecordSet  │  ...  │RecordSet  │          │
│  │Reconciler │  │Reconciler │       │Reconciler │          │
│  └─────┬─────┘  └─────┬─────┘       └─────┬─────┘          │
└────────┼───────────────┼─────────────────────┼──────────────┘
         │               │                     │
         ▼               ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│              BatchDispatcher                                  │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ zoneBatches map[string]*zoneBatch                    │    │
│  │                                                      │    │
│  │  zone-A: [change1, change2, change3...]  timer: 1s  │    │
│  │  zone-B: [change4, change5...]           timer: 1s  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  Flush triggers:                                             │
│   • Batch size reaches 1000                                  │
│   • Timer fires (configurable, default 1s)                   │
│   • Graceful shutdown signal                                 │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                  Route53 API (batched)
```

### Key Components

#### 1. `BatchDispatcher`

```go
type BatchDispatcher struct {
    mu          sync.Mutex
    zones       map[string]*zoneBatch
    sdkapi      *svcsdk.Client
    metrics     *ackmetrics.Metrics
    maxBatch    int           // default 1000
    flushDelay  time.Duration // default 1s
    rateLimiter *rate.Limiter // 4 req/s
}

type zoneBatch struct {
    hostedZoneID string
    pending      []pendingChange
    timer        *time.Timer
    mu           sync.Mutex
}

type pendingChange struct {
    action    svcsdktypes.ChangeAction
    recordSet *svcsdktypes.ResourceRecordSet
    resultCh  chan batchResult
}

type batchResult struct {
    changeInfo *svcsdktypes.ChangeInfo
    err        error
}
```

#### 2. Integration with Reconciler

The `sdkCreate`, `sdkUpdate`, and `sdkDelete` methods are modified to enqueue changes rather than calling the API directly:

```go
func (rm *resourceManager) sdkCreate(ctx context.Context, desired *resource) (*resource, error) {
    recordSet, err := rm.newResourceRecordSet(ctx, desired)
    if err != nil {
        return nil, err
    }
    
    result := rm.batchDispatcher.Enqueue(
        ctx,
        *desired.ko.Spec.HostedZoneID,
        svcsdktypes.ChangeActionCreate,
        recordSet,
    )
    
    // Block until batch is flushed
    select {
    case res := <-result:
        if res.err != nil {
            return nil, res.err
        }
        // Update status from ChangeInfo...
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

#### 3. Flush Logic

```go
func (bd *BatchDispatcher) flush(zone *zoneBatch) {
    zone.mu.Lock()
    pending := zone.pending
    zone.pending = nil
    zone.mu.Unlock()
    
    if len(pending) == 0 {
        return
    }
    
    // Rate limit
    bd.rateLimiter.Wait(context.Background())
    
    // Build ChangeBatch
    changes := make([]svcsdktypes.Change, len(pending))
    for i, p := range pending {
        changes[i] = svcsdktypes.Change{
            Action:            p.action,
            ResourceRecordSet: p.recordSet,
        }
    }
    
    input := &svcsdk.ChangeResourceRecordSetsInput{
        HostedZoneId: &zone.hostedZoneID,
        ChangeBatch:  &svcsdktypes.ChangeBatch{Changes: changes},
    }
    
    resp, err := bd.sdkapi.ChangeResourceRecordSets(ctx, input)
    
    if err != nil {
        // If InvalidChangeBatch, bisect and retry
        if isInvalidChangeBatch(err) {
            bd.bisectAndRetry(zone.hostedZoneID, pending)
            return
        }
        // Otherwise, signal all pending with the error
        for _, p := range pending {
            p.resultCh <- batchResult{err: err}
        }
        return
    }
    
    // Signal all pending with success
    for _, p := range pending {
        p.resultCh <- batchResult{changeInfo: resp.ChangeInfo}
    }
}
```

#### 4. Failure Bisection

When a batch fails with `InvalidChangeBatch`, the dispatcher splits it in half and retries each half. This recursively isolates the offending record(s):

```go
func (bd *BatchDispatcher) bisectAndRetry(zoneID string, pending []pendingChange) {
    if len(pending) == 1 {
        // Individual record is bad — signal terminal error
        pending[0].resultCh <- batchResult{err: terminalError}
        return
    }
    
    mid := len(pending) / 2
    bd.submitSubBatch(zoneID, pending[:mid])
    bd.submitSubBatch(zoneID, pending[mid:])
}
```

### Configuration

New Helm values / controller flags:

```yaml
batch:
  enabled: true          # Feature gate — allows disabling for rollback
  maxSize: 1000          # Max changes per API call (Route53 limit)
  flushInterval: "1s"    # Time to wait before flushing a partial batch
  rateLimitPerSecond: 4  # Account-wide Route53 API rate budget
```

### Observability

- **Metric:** `ack_route53_batch_size` (histogram) — changes per flush
- **Metric:** `ack_route53_batch_flush_duration` (histogram)  
- **Metric:** `ack_route53_batch_bisect_total` (counter) — how often bisection happens
- **Log:** batch flush events at Debug level with zone ID and batch size

### Rollout Plan

1. **Phase 1:** Implement Option D (rate limiter) — immediate relief, minimal risk
2. **Phase 2:** Implement batch dispatcher behind a feature gate (`--enable-batch-reconcile`)
3. **Phase 3:** Enable by default after soak testing; keep feature gate for escape hatch

### Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Batch failure poisons many CRs | Bisection isolates bad records; each CR gets its own error |
| Controller crash with in-flight batch | Changes are re-queued on next reconcile (CRs still show "not synced") |
| Shared ChangeInfo ID across CRs | Acceptable — all records in the batch truly share the same change |
| Leader election failover | New leader re-reconciles all CRs; batch queue starts empty |
| Memory pressure from large queues | Bounded by maxSize (1000) × number of active zones |

## Open Questions

1. **Should the batch dispatcher be per-controller-instance or use a shared lock?** — Per-instance is simpler; with leader election only one instance reconciles at a time anyway.

2. **What should the default flush interval be?** 1 second gives good batching for bulk operations; higher values batch more but add latency for single-record changes.

3. **Should we support mixed actions in a single batch (CREATE + DELETE + UPSERT)?** — Route53 supports this. We should too, to handle concurrent create/delete/update of different records in the same zone.

4. **How does this interact with `syncStatus` / the PENDING→INSYNC flow?** — All CRs in a batch share the same ChangeInfo.ID. The existing `syncStatus` polling with `GetChange` works — one `GetChange` call tells us the status of the entire batch.

5. **Should Option B (RecordSetGroup CRD) be pursued as a complementary feature?** — It has value for "infrastructure-as-code" use cases where users want to declare an entire zone atomically. Could be a follow-up.
