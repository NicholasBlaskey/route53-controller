package record_set

import (
	"context"
	"fmt"
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

// RecordSetBatchReconciler implements reconcile.BatchReconciler for Route53
// RecordSet resources. It groups items by hosted zone ID and issues a single
// ChangeResourceRecordSets call per zone per batch.
type RecordSetBatchReconciler struct {
	client  client.Client
	sdkapi  *svcsdk.Client
	log     logr.Logger
	metrics *ackmetrics.Metrics
	hzCache *batch.HostedZoneCache
}

// NewRecordSetBatchReconciler creates a new batch reconciler.
func NewRecordSetBatchReconciler(
	kc client.Client,
	sdkapi *svcsdk.Client,
	log logr.Logger,
	metrics *ackmetrics.Metrics,
	hzCache *batch.HostedZoneCache,
) *RecordSetBatchReconciler {
	return &RecordSetBatchReconciler{
		client:  kc,
		sdkapi:  sdkapi,
		log:     log.WithName("batch-reconciler"),
		metrics: metrics,
		hzCache: hzCache,
	}
}

// GroupKey returns the hosted zone ID for a RecordSet request.
// All records in the same hosted zone will be batched together.
func (br *RecordSetBatchReconciler) GroupKey(req reconcile.Request) string {
	// We need to read the CR to get the hosted zone ID. We do a quick
	// client.Get here. This is cheap (from cache) and necessary to group.
	rs := &svcapitypes.RecordSet{}
	if err := br.client.Get(context.Background(), req.NamespacedName, rs); err != nil {
		// If we can't read it (deleted, etc.), return a unique key so it gets
		// its own "batch" of 1 and goes through normal reconcile.
		return fmt.Sprintf("unknown-%s/%s", req.Namespace, req.Name)
	}
	if rs.Spec.HostedZoneID == nil {
		return fmt.Sprintf("no-zone-%s/%s", req.Namespace, req.Name)
	}
	return aws.ToString(rs.Spec.HostedZoneID)
}

// ReconcileBatch processes a batch of RecordSet requests that share the same
// hosted zone. It reads each CR, determines the required action, builds a
// combined ChangeBatch, and issues a single ChangeResourceRecordSets call.
func (br *RecordSetBatchReconciler) ReconcileBatch(
	ctx context.Context,
	requests []reconcile.Request,
) ([]reconcile.TypedBatchResult[reconcile.Request], error) {
	results := make([]reconcile.TypedBatchResult[reconcile.Request], len(requests))

	if len(requests) == 0 {
		return results, nil
	}

	br.log.V(1).Info("ReconcileBatch called",
		"batchSize", len(requests),
	)

	// Phase 1: Read all CRs and determine actions
	type recordWork struct {
		request   reconcile.Request
		rs        *svcapitypes.RecordSet
		action    svcsdktypes.ChangeAction
		recordSet *svcsdktypes.ResourceRecordSet
		err       error
		deleted   bool
		notFound  bool
	}

	work := make([]*recordWork, len(requests))
	var hostedZoneID string

	for i, req := range requests {
		w := &recordWork{request: req}
		work[i] = w

		rs := &svcapitypes.RecordSet{}
		if err := br.client.Get(ctx, req.NamespacedName, rs); err != nil {
			// If not found, skip — resource was deleted and finalizer already handled
			w.notFound = true
			continue
		}
		w.rs = rs

		if hostedZoneID == "" && rs.Spec.HostedZoneID != nil {
			hostedZoneID = aws.ToString(rs.Spec.HostedZoneID)
		}

		// Determine action based on deletion timestamp
		if rs.DeletionTimestamp != nil && !rs.DeletionTimestamp.IsZero() {
			w.action = svcsdktypes.ChangeActionDelete
			w.deleted = true
		} else {
			// UPSERT handles both create and update
			w.action = svcsdktypes.ChangeActionUpsert
		}

		// Build the ResourceRecordSet
		rrs, err := br.buildResourceRecordSet(ctx, rs)
		if err != nil {
			w.err = err
			continue
		}
		w.recordSet = rrs
	}

	if hostedZoneID == "" {
		// No valid hosted zone — return individual errors
		for i, w := range work {
			results[i] = reconcile.TypedBatchResult[reconcile.Request]{
				Request: w.request,
				Result:  reconcile.Result{},
			}
			if w.notFound {
				// Already deleted, nothing to do
				continue
			}
			results[i].Err = fmt.Errorf("no hosted zone ID found")
		}
		return results, nil
	}

	// Phase 2: Build the combined ChangeBatch
	var changes []svcsdktypes.Change
	var validIndices []int // maps change index → work index

	for i, w := range work {
		if w.notFound || w.err != nil || w.recordSet == nil {
			continue
		}
		changes = append(changes, svcsdktypes.Change{
			Action:            w.action,
			ResourceRecordSet: w.recordSet,
		})
		validIndices = append(validIndices, i)
	}

	if len(changes) == 0 {
		// Nothing to submit — fill in results for items with errors
		br.log.V(0).Info("No valid changes to submit",
			"totalWork", len(work),
			"hostedZoneID", hostedZoneID,
		)
		for i, w := range work {
			results[i] = reconcile.TypedBatchResult[reconcile.Request]{
				Request: w.request,
				Err:     w.err,
			}
			if w.err != nil {
				br.log.V(0).Info("Item error", "name", w.request.Name, "error", w.err.Error())
			}
		}
		return results, nil
	}

	// Phase 3: Issue the batch API call
	br.log.V(0).Info("Submitting batch ChangeResourceRecordSets",
		"hostedZoneID", hostedZoneID,
		"changeCount", len(changes),
	)

	input := &svcsdk.ChangeResourceRecordSetsInput{
		HostedZoneId: &hostedZoneID,
		ChangeBatch: &svcsdktypes.ChangeBatch{
			Changes: changes,
		},
	}

	resp, err := br.sdkapi.ChangeResourceRecordSets(ctx, input)
	if br.metrics != nil {
		br.metrics.RecordAPICall("BATCH_QUEUE_LEVEL", "ChangeResourceRecordSets", err)
	}

	if err != nil {
		br.log.V(0).Info("Batch failed, will requeue all",
			"error", err.Error(),
			"changeCount", len(changes),
		)
		// Entire batch failed — return error for all valid items so they get requeued
		for i, w := range work {
			results[i] = reconcile.TypedBatchResult[reconcile.Request]{
				Request: w.request,
			}
			if w.notFound {
				continue
			}
			if w.err != nil {
				results[i].Err = w.err
			} else {
				results[i].Err = err
			}
		}
		return results, nil
	}

	// Phase 4: Success — update status on all CRs
	changeInfo := resp.ChangeInfo
	for i, w := range work {
		results[i] = reconcile.TypedBatchResult[reconcile.Request]{
			Request: w.request,
		}

		if w.notFound || w.err != nil {
			results[i].Err = w.err
			continue
		}

		if w.deleted {
			// Delete succeeded — nothing more to do (finalizer removal is
			// handled by the standard ACK reconciler path)
			continue
		}

		// Update status with ChangeInfo
		if w.rs != nil {
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
				br.log.V(0).Info("Failed to update status after batch",
					"name", w.request.Name,
					"namespace", w.request.Namespace,
					"error", err.Error(),
				)
				// Don't fail the result — the change was applied, status will
				// catch up on next reconcile
			}
		}

		// Requeue after 30s to check sync status
		results[i].Result = reconcile.Result{RequeueAfter: 30 * time.Second}
	}

	return results, nil
}

// buildResourceRecordSet constructs a Route53 ResourceRecordSet from a CR spec.
func (br *RecordSetBatchReconciler) buildResourceRecordSet(
	ctx context.Context,
	rs *svcapitypes.RecordSet,
) (*svcsdktypes.ResourceRecordSet, error) {
	rlog := ackrtlog.FromContext(ctx)
	_ = rlog

	hostedZoneID := aws.ToString(rs.Spec.HostedZoneID)

	// Get the hosted zone domain (from cache or API)
	domain, err := br.getHostedZoneDomain(ctx, hostedZoneID)
	if err != nil {
		return nil, err
	}

	// Construct DNS name
	dnsName := aws.ToString(rs.Spec.Name)
	if dnsName != "" && !hasTrailingDot(dnsName) {
		dnsName += "."
	}
	dnsName += domain

	res := &svcsdktypes.ResourceRecordSet{
		Name: &dnsName,
		Type: svcsdktypes.RRType(aws.ToString(rs.Spec.RecordType)),
	}

	// Optional fields
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

	// Resource records
	if rs.Spec.ResourceRecords != nil {
		records := make([]svcsdktypes.ResourceRecord, len(rs.Spec.ResourceRecords))
		for i, rr := range rs.Spec.ResourceRecords {
			value := aws.ToString(rr.Value)
			records[i] = svcsdktypes.ResourceRecord{Value: &value}
		}
		res.ResourceRecords = records
	}

	// Alias target
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

	// GeoLocation
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

	// CIDR routing config
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

// getHostedZoneDomain resolves the domain for a hosted zone, using cache.
func (br *RecordSetBatchReconciler) getHostedZoneDomain(ctx context.Context, hostedZoneID string) (string, error) {
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

func hasTrailingDot(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '.'
}
