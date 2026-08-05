# Fork Changes Required for Queue-Level Batching

This POC requires local forks of two dependencies. This document describes
the exact changes made. The forks are NOT included in this repository — see
the `replace` directives in `go.mod` for the expected paths.

## controller-runtime (sigs.k8s.io/controller-runtime v0.23.0)

### New file: `pkg/reconcile/batch.go`

Adds the `BatchReconciler` interface:

```go
type BatchReconciler = TypedBatchReconciler[Request]

type TypedBatchReconciler[request comparable] interface {
    ReconcileBatch(ctx context.Context, requests []request) ([]TypedBatchResult[request], error)
    GroupKey(request) string
}

type TypedBatchResult[request comparable] struct {
    Request request
    Result  Result
    Err     error
}

type BatchConfig struct {
    MaxBatchSize int           // Default: 500
    MaxWait      time.Duration // Default: 200ms
}
```

### Modified: `pkg/internal/controller/controller.go`

1. Added `BatchDo` and `BatchConfig` fields to `Options` and `Controller` structs.
2. Modified `New()` to pass through batch fields.
3. Modified `Start()`: when `BatchDo != nil`, launches a **coordinator goroutine** + worker pool instead of standard per-item workers.
4. Added `batchCoordinator()`: single goroutine that drains the queue, groups items by `GroupKey`, and dispatches batches to workers via channel. Uses "drain until empty + 100ms grace" strategy.
5. Added `executeBatch()`: worker goroutine that calls `ReconcileBatch` and handles per-item results (requeue, forget, error).

### Modified: `pkg/controller/controller.go`

1. Added `BatchReconciler` and `BatchConfig` to public `TypedOptions`.
2. Relaxed `NewTypedUnmanaged` to accept `BatchReconciler` as alternative to `Reconciler`.
3. Passes `BatchReconciler`/`BatchConfig` through to internal controller options.

## ACK Runtime (github.com/aws-controllers-k8s/runtime v0.61.0)

### New file: `pkg/types/batch_reconciler_provider.go`

```go
type BatchReconcilerProvider interface {
    GetBatchReconciler(mgr ctrlrt.Manager) (reconcile.BatchReconciler, reconcile.BatchConfig)
}
```

### Modified: `pkg/types/aws_resource_reconciler.go`

Added `SetBatchReconciler(reconcile.BatchReconciler, reconcile.BatchConfig)` to the `AWSResourceReconciler` interface.

### Modified: `pkg/runtime/reconciler.go`

1. Added `batchReconciler` and `batchConfig` fields to `resourceReconciler`.
2. Added `import "sigs.k8s.io/controller-runtime/pkg/reconcile"`.
3. Modified `BindControllerManager`: when `batchReconciler != nil`, passes it via `ctrlrtcontroller.Options{BatchReconciler: ..., BatchConfig: ...}`.
4. Added `SetBatchReconciler()` method.

### Modified: `pkg/runtime/service_controller.go`

In `BindControllerManager`, after `NewReconciler()` and before `rec.BindControllerManager(mgr)`:
- Type-asserts the resource manager factory to `BatchReconcilerProvider`.
- If it implements the interface, calls `GetBatchReconciler(mgr)` and `rec.SetBatchReconciler(...)`.

### Modified: `go.mod`

Added `replace sigs.k8s.io/controller-runtime v0.23.0 => ../controller-runtime` to point at the local controller-runtime fork.

---

## How to reproduce the forks

```bash
# Copy from Go module cache
cp -r ~/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.23.0 forks/controller-runtime
cp -r ~/go/pkg/mod/github.com/aws-controllers-k8s/runtime@v0.61.0 forks/ack-runtime
chmod -R u+w forks/

# Apply changes described above, then:
cd forks/ack-runtime && echo 'replace sigs.k8s.io/controller-runtime v0.23.0 => ../controller-runtime' >> go.mod
```
