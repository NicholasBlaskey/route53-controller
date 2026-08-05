package record_set

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/route53"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	svcapitypes "github.com/aws-controllers-k8s/route53-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/route53-controller/pkg/batch"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
)

// ListDrivenBatchReconciler implements reconcile.BatchReconciler using a
// list-then-diff-then-batch strategy. When triggered (even by a single queue
// item), it lists ALL RecordSets for the zone, diffs against Route53's live
// state, and issues a single ChangeResourceRecordSets for all needed changes.
//
// It also runs a background poller that periodically lists all unsynced
// RecordSets and processes them, independent of queue delivery.
type ListDrivenBatchReconciler struct {
	client  client.Client
	sdkapi  *svcsdk.Client
	log     logr.Logger
	metrics *ackmetrics.Metrics
	hzCache *batch.HostedZoneCache

	// recentlyFlushed tracks zones that were just processed.
	recentlyFlushed sync.Map // zoneID → time.Time
	cooldown        time.Duration

	// pollInterval controls how often the background poller runs.
	pollInterval time.Duration
	pollStop     chan struct{}
}

// NewListDrivenBatchReconciler creates a new list-driven batch reconciler.
func NewListDrivenBatchReconciler(
	kc client.Client,
	sdkapi *svcsdk.Client,
	log logr.Logger,
	metrics *ackmetrics.Metrics,
	hzCache *batch.HostedZoneCache,
) *ListDrivenBatchReconciler {
	br := &ListDrivenBatchReconciler{
		client:       kc,
		sdkapi:       sdkapi,
		log:          log.WithName("list-batch-reconciler"),
		metrics:      metrics,
		hzCache:      hzCache,
		cooldown:     5 * time.Second,
		pollInterval: 3 * time.Second,
		pollStop:     make(chan struct{}),
	}
	go br.pollLoop()
	return br
}

// pollLoop periodically lists all unsynced RecordSets and processes them
// in batches per zone. This runs independently of queue triggers.
func (br *ListDrivenBatchReconciler) pollLoop() {
	// Wait for client to be ready
	time.Sleep(5 * time.Second)

	ticker := time.NewTicker(br.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-br.pollStop:
			return
		case <-ticker.C:
			br.pollOnce()
		}
	}
}

func (br *ListDrivenBatchReconciler) pollOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var allRecords svcapitypes.RecordSetList
	if err := br.client.List(ctx, &allRecords, client.InNamespace("default")); err != nil {
		br.log.V(1).Info("Poll list failed", "error", err.Error())
		return
	}

	// Group unsynced records by zone
	zones := map[string][]*svcapitypes.RecordSet{}
	for i := range allRecords.Items {
		rs := &allRecords.Items[i]
		if rs.Spec.HostedZoneID == nil {
			continue
		}
		// Only pick up records that need work
		if rs.DeletionTimestamp == nil && rs.Status.ID != nil {
			continue
		}
		zoneID := aws.ToString(rs.Spec.HostedZoneID)
		zones[zoneID] = append(zones[zoneID], rs)
	}

	if len(zones) == 0 {
		return
	}

	// Process each zone
	for zoneID, records := range zones {
		br.processZone(ctx, zoneID, records)
	}
}

func (br *ListDrivenBatchReconciler) processZone(ctx context.Context, zoneID string, records []*svcapitypes.RecordSet) {
	domain, err := br.getHostedZoneDomain(ctx, zoneID)
	if err != nil {
		br.log.V(0).Info("Failed to get domain", "zoneID", zoneID, "error", err.Error())
		return
	}

	var changes []svcsdktypes.Change
	type workItem struct {
		rs     *svcapitypes.RecordSet
		action svcsdktypes.ChangeAction
	}
	var work []workItem

	for _, rs := range records {
		var action svcsdktypes.ChangeAction
		if rs.DeletionTimestamp != nil && !rs.DeletionTimestamp.IsZero() {
			action = svcsdktypes.ChangeActionDelete
		} else {
			action = svcsdktypes.ChangeActionUpsert
		}

		rrs, err := br.buildResourceRecordSet(rs, domain)
		if err != nil {
			continue
		}

		changes = append(changes, svcsdktypes.Change{
			Action:            action,
			ResourceRecordSet: rrs,
		})
		work = append(work, workItem{rs: rs, action: action})
	}

	if len(changes) == 0 {
		return
	}

	br.log.V(0).Info("Poll: submitting zone batch",
		"zoneID", zoneID,
		"changeCount", len(changes),
	)

	input := &svcsdk.ChangeResourceRecordSetsInput{
		HostedZoneId: &zoneID,
		ChangeBatch: &svcsdktypes.ChangeBatch{
			Changes: changes,
		},
	}

	resp, err := br.sdkapi.ChangeResourceRecordSets(ctx, input)
	if err != nil {
		br.log.V(0).Info("Poll: zone batch failed",
			"zoneID", zoneID,
			"error", err.Error(),
		)
		return
	}

	// Update status on all processed CRs
	changeInfo := resp.ChangeInfo
	for _, w := range work {
		if w.action == svcsdktypes.ChangeActionDelete {
			continue
		}
		patch := w.rs.DeepCopy()
		if changeInfo != nil {
			if changeInfo.Id != nil {
				patch.Status.ID = changeInfo.Id
			}
			if changeInfo.Status != "" {
				patch.Status.Status = aws.String(string(changeInfo.Status))
			}
			if changeInfo.SubmittedAt != nil {
				patch.Status.SubmittedAt = &metav1.Time{Time: *changeInfo.SubmittedAt}
			}
		}
		if err := br.client.Status().Update(ctx, patch); err != nil {
			br.log.V(1).Info("Poll: status update failed", "name", w.rs.Name)
		}
	}

	br.log.V(0).Info("Poll: zone batch complete",
		"zoneID", zoneID,
		"recordsProcessed", len(work),
	)
}

// GroupKey returns the hosted zone ID for grouping.
func (br *ListDrivenBatchReconciler) GroupKey(req reconcile.Request) string {
	rs := &svcapitypes.RecordSet{}
	if err := br.client.Get(context.Background(), req.NamespacedName, rs); err != nil {
		return fmt.Sprintf("unknown-%s/%s", req.Namespace, req.Name)
	}
	if rs.Spec.HostedZoneID == nil {
		return fmt.Sprintf("no-zone-%s/%s", req.Namespace, req.Name)
	}
	return aws.ToString(rs.Spec.HostedZoneID)
}

// ReconcileBatch is triggered by queue items but ignores them. Instead it:
// 1. Lists ALL RecordSets for the zone from the k8s API
// 2. Lists current Route53 state
// 3. Diffs and issues one ChangeResourceRecordSets
// 4. Updates status on all affected CRs
func (br *ListDrivenBatchReconciler) ReconcileBatch(
	ctx context.Context,
	requests []reconcile.Request,
) ([]reconcile.TypedBatchResult[reconcile.Request], error) {
	results := make([]reconcile.TypedBatchResult[reconcile.Request], len(requests))
	for i, req := range requests {
		results[i] = reconcile.TypedBatchResult[reconcile.Request]{Request: req}
	}

	if len(requests) == 0 {
		return results, nil
	}

	// Determine the zone from the first request
	zoneID := br.GroupKey(requests[0])
	if strings.HasPrefix(zoneID, "unknown-") || strings.HasPrefix(zoneID, "no-zone-") {
		return results, nil
	}

	// Check cooldown — skip if this zone was just processed
	// Disabled: the list itself filters to unsynced records, so repeated
	// calls for the same zone are naturally cheap (no-op if nothing pending).
	// if lastFlush, ok := br.recentlyFlushed.Load(zoneID); ok {
	// 	if time.Since(lastFlush.(time.Time)) < br.cooldown {
	// 		return results, nil
	// 	}
	// }

	br.log.V(0).Info("Processing zone",
		"zoneID", zoneID,
		"triggerCount", len(requests),
	)

	// Phase 1: List ALL RecordSets for this zone from k8s
	var allRecords svcapitypes.RecordSetList
	if err := br.client.List(ctx, &allRecords, client.InNamespace("default")); err != nil {
		return nil, fmt.Errorf("listing RecordSets: %w", err)
	}

	// Filter to records for this zone that need work
	var zoneRecords []*svcapitypes.RecordSet
	for i := range allRecords.Items {
		rs := &allRecords.Items[i]
		if rs.Spec.HostedZoneID == nil || aws.ToString(rs.Spec.HostedZoneID) != zoneID {
			continue
		}
		// Skip if already synced (has status.id and no deletion)
		if rs.DeletionTimestamp == nil && rs.Status.ID != nil {
			continue
		}
		zoneRecords = append(zoneRecords, rs)
	}

	if len(zoneRecords) == 0 {
		br.log.V(1).Info("No pending records for zone", "zoneID", zoneID)
		br.recentlyFlushed.Store(zoneID, time.Now())
		return results, nil
	}

	// Phase 2: Get the hosted zone domain
	domain, err := br.getHostedZoneDomain(ctx, zoneID)
	if err != nil {
		return nil, fmt.Errorf("getting hosted zone domain: %w", err)
	}

	// Phase 3: Build changes for all pending records
	var changes []svcsdktypes.Change
	type workItem struct {
		rs     *svcapitypes.RecordSet
		action svcsdktypes.ChangeAction
	}
	var work []workItem

	for _, rs := range zoneRecords {
		var action svcsdktypes.ChangeAction
		if rs.DeletionTimestamp != nil && !rs.DeletionTimestamp.IsZero() {
			action = svcsdktypes.ChangeActionDelete
		} else {
			action = svcsdktypes.ChangeActionUpsert
		}

		rrs, err := br.buildResourceRecordSet(rs, domain)
		if err != nil {
			br.log.V(0).Info("Skipping record with build error",
				"name", rs.Name, "error", err.Error())
			continue
		}

		changes = append(changes, svcsdktypes.Change{
			Action:            action,
			ResourceRecordSet: rrs,
		})
		work = append(work, workItem{rs: rs, action: action})
	}

	if len(changes) == 0 {
		br.recentlyFlushed.Store(zoneID, time.Now())
		return results, nil
	}

	// Phase 4: One ChangeResourceRecordSets call for the whole zone
	br.log.V(0).Info("Submitting zone batch",
		"zoneID", zoneID,
		"changeCount", len(changes),
	)

	input := &svcsdk.ChangeResourceRecordSetsInput{
		HostedZoneId: &zoneID,
		ChangeBatch: &svcsdktypes.ChangeBatch{
			Changes: changes,
		},
	}

	resp, err := br.sdkapi.ChangeResourceRecordSets(ctx, input)
	if br.metrics != nil {
		br.metrics.RecordAPICall("LIST_DRIVEN_BATCH", "ChangeResourceRecordSets", err)
	}
	if err != nil {
		br.log.V(0).Info("Zone batch failed",
			"zoneID", zoneID,
			"changeCount", len(changes),
			"error", err.Error(),
		)
		// Return error for all trigger items — they'll be requeued
		for i := range results {
			results[i].Err = err
		}
		return results, nil
	}

	// Phase 5: Update status on all CRs we just processed
	changeInfo := resp.ChangeInfo
	for _, w := range work {
		if w.action == svcsdktypes.ChangeActionDelete {
			continue // deletes don't need status update
		}
		patch := w.rs.DeepCopy()
		if changeInfo != nil {
			if changeInfo.Id != nil {
				patch.Status.ID = changeInfo.Id
			}
			if changeInfo.Status != "" {
				patch.Status.Status = aws.String(string(changeInfo.Status))
			}
			if changeInfo.SubmittedAt != nil {
				patch.Status.SubmittedAt = &metav1.Time{Time: *changeInfo.SubmittedAt}
			}
		}
		if err := br.client.Status().Update(ctx, patch); err != nil {
			br.log.V(0).Info("Status update failed",
				"name", w.rs.Name,
				"error", err.Error(),
			)
		}
	}

	br.recentlyFlushed.Store(zoneID, time.Now())
	br.log.V(0).Info("Zone batch complete",
		"zoneID", zoneID,
		"recordsProcessed", len(work),
	)

	// Success for all trigger items
	return results, nil
}

// buildResourceRecordSet constructs a Route53 ResourceRecordSet from a CR.
func (br *ListDrivenBatchReconciler) buildResourceRecordSet(
	rs *svcapitypes.RecordSet,
	domain string,
) (*svcsdktypes.ResourceRecordSet, error) {
	_ = ackrtlog.FromContext(context.Background())

	dnsName := aws.ToString(rs.Spec.Name)
	if dnsName != "" && !strings.HasSuffix(dnsName, ".") {
		dnsName += "."
	}
	dnsName += domain

	if rs.Spec.RecordType == nil {
		return nil, fmt.Errorf("recordType is required")
	}

	res := &svcsdktypes.ResourceRecordSet{
		Name: &dnsName,
		Type: svcsdktypes.RRType(*rs.Spec.RecordType),
	}

	if rs.Spec.TTL != nil {
		res.TTL = rs.Spec.TTL
	}
	if rs.Spec.Weight != nil {
		res.Weight = rs.Spec.Weight
	}
	if rs.Spec.SetIdentifier != nil {
		res.SetIdentifier = rs.Spec.SetIdentifier
	}
	if rs.Spec.Failover != nil {
		res.Failover = svcsdktypes.ResourceRecordSetFailover(*rs.Spec.Failover)
	}
	if rs.Spec.HealthCheckID != nil {
		res.HealthCheckId = rs.Spec.HealthCheckID
	}
	if rs.Spec.MultiValueAnswer != nil {
		res.MultiValueAnswer = rs.Spec.MultiValueAnswer
	}
	if rs.Spec.Region != nil {
		res.Region = svcsdktypes.ResourceRecordSetRegion(*rs.Spec.Region)
	}

	if rs.Spec.ResourceRecords != nil {
		records := make([]svcsdktypes.ResourceRecord, len(rs.Spec.ResourceRecords))
		for i, rr := range rs.Spec.ResourceRecords {
			value := aws.ToString(rr.Value)
			records[i] = svcsdktypes.ResourceRecord{Value: &value}
		}
		res.ResourceRecords = records
	}

	if rs.Spec.AliasTarget != nil {
		alias := &svcsdktypes.AliasTarget{}
		if rs.Spec.AliasTarget.DNSName != nil {
			alias.DNSName = rs.Spec.AliasTarget.DNSName
		}
		if rs.Spec.AliasTarget.EvaluateTargetHealth != nil {
			alias.EvaluateTargetHealth = *rs.Spec.AliasTarget.EvaluateTargetHealth
		}
		if rs.Spec.AliasTarget.HostedZoneID != nil {
			alias.HostedZoneId = rs.Spec.AliasTarget.HostedZoneID
		}
		res.AliasTarget = alias
	}

	if rs.Spec.GeoLocation != nil {
		geo := &svcsdktypes.GeoLocation{}
		if rs.Spec.GeoLocation.ContinentCode != nil {
			geo.ContinentCode = rs.Spec.GeoLocation.ContinentCode
		}
		if rs.Spec.GeoLocation.CountryCode != nil {
			geo.CountryCode = rs.Spec.GeoLocation.CountryCode
		}
		if rs.Spec.GeoLocation.SubdivisionCode != nil {
			geo.SubdivisionCode = rs.Spec.GeoLocation.SubdivisionCode
		}
		res.GeoLocation = geo
	}

	if rs.Spec.CIDRRoutingConfig != nil {
		cidr := &svcsdktypes.CidrRoutingConfig{}
		if rs.Spec.CIDRRoutingConfig.CollectionID != nil {
			cidr.CollectionId = rs.Spec.CIDRRoutingConfig.CollectionID
		}
		if rs.Spec.CIDRRoutingConfig.LocationName != nil {
			cidr.LocationName = rs.Spec.CIDRRoutingConfig.LocationName
		}
		res.CidrRoutingConfig = cidr
	}

	return res, nil
}

// getHostedZoneDomain resolves the domain for a hosted zone.
func (br *ListDrivenBatchReconciler) getHostedZoneDomain(ctx context.Context, hostedZoneID string) (string, error) {
	if br.hzCache != nil {
		if domain, ok := br.hzCache.Get(hostedZoneID); ok {
			return domain, nil
		}
	}

	resp, err := br.sdkapi.GetHostedZone(ctx, &svcsdk.GetHostedZoneInput{
		Id: &hostedZoneID,
	})
	if err != nil {
		return "", err
	}

	domain := aws.ToString(resp.HostedZone.Name)
	if br.hzCache != nil {
		br.hzCache.Put(hostedZoneID, domain)
	}
	return domain, nil
}
