package record_set

import (
	"context"
	"sync"
	"time"

	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
)

// changeStatusDispatcher batches GetChange calls by change ID.
// All records in a batched write share the same change ID. Instead of
// N reconcilers each calling GetChange, one call per change ID per flush
// interval serves all of them.
type changeStatusDispatcher struct {
	mu            sync.Mutex
	pending       map[string]*changeWaiters // changeID → waiters
	lastResults   map[string]*changeResult  // changeID → cached result
	lastResultsMu sync.RWMutex
	sdkapi        *svcsdk.Client
	flushInterval time.Duration
}

type changeWaiters struct {
	waiters []chan changeResult
	timer   *time.Timer
}

type changeResult struct {
	status string
	err    error
}

var globalChangeDispatcher *changeStatusDispatcher

func initChangeDispatcher(sdkapi *svcsdk.Client, flushInterval time.Duration) {
	globalChangeDispatcher = &changeStatusDispatcher{
		pending:       make(map[string]*changeWaiters),
		lastResults:   make(map[string]*changeResult),
		sdkapi:        sdkapi,
		flushInterval: flushInterval,
	}
}

// GetStatus returns the propagation status for a change ID.
// INSYNC results are returned immediately from cache (terminal state).
// PENDING results are queued — one GetChange call per change ID per flush interval.
func (d *changeStatusDispatcher) GetStatus(ctx context.Context, changeID string) (string, error) {
	// Fast path: INSYNC is permanent
	d.lastResultsMu.RLock()
	if last, ok := d.lastResults[changeID]; ok && last.status == string(svcsdktypes.ChangeStatusInsync) {
		d.lastResultsMu.RUnlock()
		return last.status, last.err
	}
	// Also serve recent PENDING from cache (within flush interval)
	if last, ok := d.lastResults[changeID]; ok && last.err == nil {
		d.lastResultsMu.RUnlock()
		return last.status, nil
	}
	d.lastResultsMu.RUnlock()

	// Enqueue and wait
	resultCh := make(chan changeResult, 1)

	d.mu.Lock()
	cw, ok := d.pending[changeID]
	if !ok {
		cw = &changeWaiters{}
		d.pending[changeID] = cw
	}
	cw.waiters = append(cw.waiters, resultCh)

	// Start timer on first waiter
	if len(cw.waiters) == 1 {
		cid := changeID
		cw.timer = time.AfterFunc(d.flushInterval, func() {
			d.flush(cid)
		})
	}
	d.mu.Unlock()

	select {
	case res := <-resultCh:
		return res.status, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (d *changeStatusDispatcher) flush(changeID string) {
	d.mu.Lock()
	cw, ok := d.pending[changeID]
	if !ok || len(cw.waiters) == 0 {
		d.mu.Unlock()
		return
	}
	waiters := cw.waiters
	cw.waiters = nil
	if cw.timer != nil {
		cw.timer.Stop()
		cw.timer = nil
	}
	delete(d.pending, changeID)
	d.mu.Unlock()

	// One GetChange call
	resp, err := d.sdkapi.GetChange(context.Background(), &svcsdk.GetChangeInput{
		Id: &changeID,
	})

	var status string
	if err == nil {
		status = string(resp.ChangeInfo.Status)
	}

	// Cache result
	result := &changeResult{status: status, err: err}
	d.lastResultsMu.Lock()
	d.lastResults[changeID] = result
	d.lastResultsMu.Unlock()

	// Deliver to all waiters
	for _, ch := range waiters {
		ch <- changeResult{status: status, err: err}
	}
}
