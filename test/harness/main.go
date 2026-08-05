package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
)

var (
	numZones       = flag.Int("zones", 3, "Number of hosted zones to create")
	recordsPerZone = flag.Int("records-per-zone", 50, "Number of records to create per zone")
	testDuration   = flag.Duration("duration", 5*time.Minute, "Maximum test duration")
	namespace      = flag.String("namespace", "default", "Kubernetes namespace for CRs")
	kubecontext    = flag.String("context", "route53-test", "Kubernetes context to use")
	cleanupOnly    = flag.Bool("cleanup-only", false, "Only run cleanup (remove leftover resources)")
)

const (
	testPrefix = "r53-harness"
	lockFile   = "/tmp/r53-harness.lock"
)

type metrics struct {
	createSubmitted int64
	createCompleted int64
	createFailed    int64
	updateSubmitted int64
	updateCompleted int64
	updateFailed    int64
	deleteSubmitted int64
	deleteCompleted int64
	deleteFailed    int64

	mu             sync.Mutex
	reconcileTimes []time.Duration // time from CR creation to status update
}

func (m *metrics) recordReconcile(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileTimes = append(m.reconcileTimes, d)
}

func (m *metrics) summary() {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("\n====== TEST HARNESS RESULTS ======")
	fmt.Printf("Creates: submitted=%d completed=%d failed=%d\n",
		atomic.LoadInt64(&m.createSubmitted),
		atomic.LoadInt64(&m.createCompleted),
		atomic.LoadInt64(&m.createFailed))
	fmt.Printf("Updates: submitted=%d completed=%d failed=%d\n",
		atomic.LoadInt64(&m.updateSubmitted),
		atomic.LoadInt64(&m.updateCompleted),
		atomic.LoadInt64(&m.updateFailed))
	fmt.Printf("Deletes: submitted=%d completed=%d failed=%d\n",
		atomic.LoadInt64(&m.deleteSubmitted),
		atomic.LoadInt64(&m.deleteCompleted),
		atomic.LoadInt64(&m.deleteFailed))

	if len(m.reconcileTimes) > 0 {
		var total time.Duration
		var min, max time.Duration
		min = m.reconcileTimes[0]
		for _, d := range m.reconcileTimes {
			total += d
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}
		avg := total / time.Duration(len(m.reconcileTimes))
		totalSec := total.Seconds()
		opsPerSec := float64(len(m.reconcileTimes)) / totalSec * float64(len(m.reconcileTimes))

		fmt.Printf("\nReconcile latencies (n=%d):\n", len(m.reconcileTimes))
		fmt.Printf("  Min: %v\n", min)
		fmt.Printf("  Max: %v\n", max)
		fmt.Printf("  Avg: %v\n", avg)
		fmt.Printf("  Total wall time for all reconciles: %v\n", total)

		// Calculate actual ops/sec based on first create to last complete
		fmt.Printf("\nThroughput:\n")
		fmt.Printf("  Reconciled records: %d\n", len(m.reconcileTimes))
		fmt.Printf("  Avg reconcile ops/sec: %.2f\n", opsPerSec)
	}
	fmt.Println("==================================")
}

type harness struct {
	dynClient  dynamic.Interface
	r53Client  *route53.Client
	metrics    *metrics
	zoneIDs    []string // Route53 hosted zone IDs created
	zoneDomain map[string]string
}

var recordSetGVR = schema.GroupVersionResource{
	Group:    "route53.services.k8s.aws",
	Version:  "v1alpha1",
	Resource: "recordsets",
}

var hostedZoneGVR = schema.GroupVersionResource{
	Group:    "route53.services.k8s.aws",
	Version:  "v1alpha1",
	Resource: "hostedzones",
}

func main() {
	flag.Parse()

	// Acquire file lock so only one harness instance runs at a time.
	// Other instances will block here until the lock is released.
	lockFd, err := acquireLock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire lock: %v\n", err)
		os.Exit(1)
	}
	defer releaseLock(lockFd)

	ctx, cancel := context.WithTimeout(context.Background(), *testDuration+2*time.Minute) // extra for cleanup
	defer cancel()

	// Handle SIGINT/SIGTERM for cleanup
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, cleaning up...")
		cancel()
	}()

	// Build k8s client
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{CurrentContext: *kubecontext}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	restConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load kubeconfig: %v\n", err)
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create dynamic client: %v\n", err)
		os.Exit(1)
	}

	// Build AWS Route53 client
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load AWS config: %v\n", err)
		os.Exit(1)
	}
	r53Client := route53.NewFromConfig(awsCfg)

	h := &harness{
		dynClient:  dynClient,
		r53Client:  r53Client,
		metrics:    &metrics{},
		zoneDomain: make(map[string]string),
	}

	if *cleanupOnly {
		fmt.Println("Running cleanup only...")
		h.cleanup(ctx)
		return
	}

	fmt.Printf("=== Route53 ACK Controller Test Harness ===\n")
	fmt.Printf("Config: zones=%d, records-per-zone=%d, duration=%v\n", *numZones, *recordsPerZone, *testDuration)
	fmt.Printf("Cluster context: %s, namespace: %s\n\n", *kubecontext, *namespace)

	// Phase 1: Create hosted zones directly in Route53
	fmt.Println("[Phase 1] Creating hosted zones in Route53...")
	if err := h.createHostedZones(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create hosted zones: %v\n", err)
		h.cleanup(ctx)
		os.Exit(1)
	}
	fmt.Printf("  Created %d hosted zones\n", len(h.zoneIDs))

	// Phase 2: Start a watcher for RecordSet status changes
	fmt.Println("[Phase 2] Starting RecordSet status watcher...")
	creationTimes := &sync.Map{} // name -> time.Time
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	go h.watchRecordSets(watchCtx, creationTimes)

	// Phase 3: Create RecordSet CRs
	fmt.Println("[Phase 3] Creating RecordSet CRs...")
	testStart := time.Now()
	h.createRecordSetCRs(ctx, creationTimes)
	createDone := time.Now()
	fmt.Printf("  Submitted %d RecordSet CRs in %v\n",
		atomic.LoadInt64(&h.metrics.createSubmitted), createDone.Sub(testStart))

	// Phase 4: Wait for reconciliation
	fmt.Println("[Phase 4] Waiting for reconciliation...")
	h.waitForReconciliation(ctx, int(atomic.LoadInt64(&h.metrics.createSubmitted)))

	// Phase 5: Update records (change TTL)
	fmt.Println("[Phase 5] Updating RecordSet CRs (TTL change)...")
	updateStart := time.Now()
	h.updateRecordSetCRs(ctx, creationTimes)
	fmt.Printf("  Submitted %d updates in %v\n",
		atomic.LoadInt64(&h.metrics.updateSubmitted), time.Since(updateStart))

	// Wait for update reconciliation
	fmt.Println("[Phase 5b] Waiting for update reconciliation...")
	h.waitForReconciliation(ctx, int(atomic.LoadInt64(&h.metrics.updateSubmitted))+int(atomic.LoadInt64(&h.metrics.createCompleted)))

	totalTestTime := time.Since(testStart)
	fmt.Printf("\nTotal test time: %v\n", totalTestTime)

	// Print metrics
	h.metrics.summary()

	totalOps := atomic.LoadInt64(&h.metrics.createCompleted) + atomic.LoadInt64(&h.metrics.updateCompleted)
	fmt.Printf("\n>>> EFFECTIVE OPS/SEC: %.2f (total_reconciled=%d / wall_time=%.1fs)\n",
		float64(totalOps)/totalTestTime.Seconds(), totalOps, totalTestTime.Seconds())

	// Phase 6: Cleanup
	fmt.Println("\n[Phase 6] Cleaning up...")
	watchCancel()
	h.cleanup(ctx)
}

func (h *harness) createHostedZones(ctx context.Context) error {
	for i := 0; i < *numZones; i++ {
		domain := fmt.Sprintf("%s-%d-%d.harness-test.internal", testPrefix, time.Now().UnixNano(), i)
		callerRef := fmt.Sprintf("%s-%d-%d", testPrefix, time.Now().UnixNano(), i)

		out, err := h.r53Client.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
			Name:            &domain,
			CallerReference: &callerRef,
			HostedZoneConfig: &r53types.HostedZoneConfig{
				Comment:     strPtr(fmt.Sprintf("Test harness zone %d", i)),
				PrivateZone: false,
			},
		})
		if err != nil {
			return fmt.Errorf("creating zone %d: %w", i, err)
		}

		zoneID := *out.HostedZone.Id
		// Strip /hostedzone/ prefix
		if len(zoneID) > 12 && zoneID[:12] == "/hostedzone/" {
			zoneID = zoneID[12:]
		}
		h.zoneIDs = append(h.zoneIDs, zoneID)
		h.zoneDomain[zoneID] = domain
		fmt.Printf("  Zone %d: %s (ID: %s)\n", i, domain, zoneID)
	}
	return nil
}

func (h *harness) createRecordSetCRs(ctx context.Context, creationTimes *sync.Map) {
	for _, zoneID := range h.zoneIDs {
		domain := h.zoneDomain[zoneID]
		for i := 0; i < *recordsPerZone; i++ {
			name := fmt.Sprintf("record-%04d.%s", i, domain)
			crName := fmt.Sprintf("%s-%s-%04d", testPrefix, strings.ToLower(zoneID[:8]), i)
			ip := fmt.Sprintf("10.%d.%d.%d", rand.Intn(255), rand.Intn(255), rand.Intn(255)+1)

			cr := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "route53.services.k8s.aws/v1alpha1",
					"kind":       "RecordSet",
					"metadata": map[string]interface{}{
						"name":      crName,
						"namespace": *namespace,
						"labels": map[string]interface{}{
							"harness": testPrefix,
							"zone-id": strings.ToLower(zoneID[:8]),
						},
					},
					"spec": map[string]interface{}{
						"hostedZoneID": zoneID,
						"name":         name,
						"recordType":   "A",
						"ttl":          int64(300),
						"resourceRecords": []interface{}{
							map[string]interface{}{"value": ip},
						},
					},
				},
			}

			creationTimes.Store(crName, time.Now())
			_, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).Create(ctx, cr, metav1.CreateOptions{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "  WARN: failed to create CR %s: %v\n", crName, err)
				atomic.AddInt64(&h.metrics.createFailed, 1)
				continue
			}
			atomic.AddInt64(&h.metrics.createSubmitted, 1)
		}
	}
}

func (h *harness) updateRecordSetCRs(ctx context.Context, creationTimes *sync.Map) {
	list, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "harness=" + testPrefix,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  WARN: failed to list CRs for update: %v\n", err)
		return
	}

	for _, item := range list.Items {
		name := item.GetName()
		// Update TTL from 300 to 600
		spec, ok := item.Object["spec"].(map[string]interface{})
		if !ok {
			continue
		}
		spec["ttl"] = int64(600)
		item.Object["spec"] = spec

		creationTimes.Store(name+"-update", time.Now())
		_, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).Update(ctx, &item, metav1.UpdateOptions{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "  WARN: failed to update CR %s: %v\n", name, err)
			atomic.AddInt64(&h.metrics.updateFailed, 1)
			continue
		}
		atomic.AddInt64(&h.metrics.updateSubmitted, 1)
	}
}

func (h *harness) watchRecordSets(ctx context.Context, creationTimes *sync.Map) {
	watcher, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: "harness=" + testPrefix,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  WARN: failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// Watcher closed, restart
				watcher, err = h.dynClient.Resource(recordSetGVR).Namespace(*namespace).Watch(ctx, metav1.ListOptions{
					LabelSelector: "harness=" + testPrefix,
				})
				if err != nil {
					return
				}
				continue
			}
			if event.Type == watch.Modified {
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				h.processStatusUpdate(obj, creationTimes)
			}
		}
	}
}

func (h *harness) processStatusUpdate(obj *unstructured.Unstructured, creationTimes *sync.Map) {
	name := obj.GetName()
	status, ok := obj.Object["status"].(map[string]interface{})
	if !ok {
		return
	}

	// Check if status has been set (indicating reconciliation happened)
	conditions, ok := status["conditions"]
	if !ok {
		return
	}
	condList, ok := conditions.([]interface{})
	if !ok || len(condList) == 0 {
		return
	}

	// Check for ACK.ResourceSynced condition
	for _, c := range condList {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _ := cond["type"].(string)
		condStatus, _ := cond["status"].(string)
		if condType == "ACK.ResourceSynced" && condStatus == "True" {
			// Record reconcile time
			if startTime, ok := creationTimes.Load(name); ok {
				d := time.Since(startTime.(time.Time))
				h.metrics.recordReconcile(d)
				atomic.AddInt64(&h.metrics.createCompleted, 1)
				creationTimes.Delete(name)
			} else if startTime, ok := creationTimes.Load(name + "-update"); ok {
				d := time.Since(startTime.(time.Time))
				h.metrics.recordReconcile(d)
				atomic.AddInt64(&h.metrics.updateCompleted, 1)
				creationTimes.Delete(name + "-update")
			}
			return
		}
	}
}

func (h *harness) waitForReconciliation(ctx context.Context, targetCount int) {
	timeout := time.After(*testDuration)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			fmt.Println("  Test duration exceeded, moving on...")
			return
		case <-ticker.C:
			completed := atomic.LoadInt64(&h.metrics.createCompleted) + atomic.LoadInt64(&h.metrics.updateCompleted)
			fmt.Printf("  Progress: %d/%d reconciled\n", completed, targetCount)
			if int(completed) >= targetCount {
				return
			}
		}
	}
}

func (h *harness) cleanup(ctx context.Context) {
	// Use a longer context for cleanup
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Delete all RecordSet CRs
	fmt.Println("  Deleting RecordSet CRs...")
	list, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).List(cleanupCtx, metav1.ListOptions{
		LabelSelector: "harness=" + testPrefix,
	})
	if err == nil {
		for _, item := range list.Items {
			_ = h.dynClient.Resource(recordSetGVR).Namespace(*namespace).Delete(cleanupCtx, item.GetName(), metav1.DeleteOptions{})
		}
		fmt.Printf("  Deleted %d RecordSet CRs\n", len(list.Items))
	}

	// Wait for CRs to be fully deleted (controller needs to delete from Route53 first)
	fmt.Println("  Waiting for CR deletion (controller removing Route53 records)...")
	for i := 0; i < 60; i++ {
		remaining, err := h.dynClient.Resource(recordSetGVR).Namespace(*namespace).List(cleanupCtx, metav1.ListOptions{
			LabelSelector: "harness=" + testPrefix,
		})
		if err != nil || len(remaining.Items) == 0 {
			break
		}
		if i%10 == 0 {
			fmt.Printf("  Waiting... %d CRs remaining\n", len(remaining.Items))
		}
		time.Sleep(2 * time.Second)
	}

	// Delete hosted zones from Route53
	fmt.Println("  Deleting hosted zones from Route53...")
	for _, zoneID := range h.zoneIDs {
		h.deleteHostedZone(cleanupCtx, zoneID)
	}

	// Also clean up any orphaned harness zones
	h.cleanupOrphanedZones(cleanupCtx)

	fmt.Println("  Cleanup complete!")
}

func (h *harness) deleteHostedZone(ctx context.Context, zoneID string) {
	// First delete all records in the zone (except NS and SOA)
	paginator := route53.NewListResourceRecordSetsPaginator(h.r53Client, &route53.ListResourceRecordSetsInput{
		HostedZoneId: &zoneID,
	})

	var changes []r53types.Change
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  WARN: failed to list records for zone %s: %v\n", zoneID, err)
			break
		}
		for _, rrs := range page.ResourceRecordSets {
			if rrs.Type == r53types.RRTypeNs || rrs.Type == r53types.RRTypeSoa {
				continue
			}
			changes = append(changes, r53types.Change{
				Action:            r53types.ChangeActionDelete,
				ResourceRecordSet: &rrs,
			})
		}
	}

	// Delete records in batches of 1000
	for i := 0; i < len(changes); i += 1000 {
		end := i + 1000
		if end > len(changes) {
			end = len(changes)
		}
		batch := changes[i:end]
		_, err := h.r53Client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
			HostedZoneId: &zoneID,
			ChangeBatch: &r53types.ChangeBatch{
				Changes: batch,
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "  WARN: failed to delete records batch for zone %s: %v\n", zoneID, err)
		}
	}

	// Now delete the zone
	_, err := h.r53Client.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{
		Id: &zoneID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  WARN: failed to delete zone %s: %v\n", zoneID, err)
	} else {
		fmt.Printf("  Deleted zone %s\n", zoneID)
	}
}

func (h *harness) cleanupOrphanedZones(ctx context.Context) {
	// Find any zones that match our test prefix
	paginator := route53.NewListHostedZonesPaginator(h.r53Client, &route53.ListHostedZonesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			break
		}
		for _, zone := range page.HostedZones {
			if zone.Name != nil && len(*zone.Name) > len(testPrefix) && (*zone.Name)[:len(testPrefix)] == testPrefix {
				zoneID := *zone.Id
				if len(zoneID) > 12 && zoneID[:12] == "/hostedzone/" {
					zoneID = zoneID[12:]
				}
				fmt.Printf("  Found orphaned harness zone: %s (%s)\n", *zone.Name, zoneID)
				h.deleteHostedZone(ctx, zoneID)
			}
		}
	}
}

func strPtr(s string) *string {
	return &s
}

// printJSON is a helper for debugging
func printJSON(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

// acquireLock obtains an exclusive file lock. If another process holds the lock,
// this blocks until it's released. This prevents two harness instances from
// running concurrently and fighting over the same Route53 API budget and cluster.
func acquireLock() (*os.File, error) {
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file: %w", err)
	}

	fmt.Printf("Acquiring lock (%s)...\n", lockFile)
	// LOCK_EX = exclusive lock, blocks until available
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("acquiring flock: %w", err)
	}
	fmt.Println("Lock acquired.")
	return f, nil
}

// releaseLock releases the file lock and closes the file.
func releaseLock(f *os.File) {
	if f == nil {
		return
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}
