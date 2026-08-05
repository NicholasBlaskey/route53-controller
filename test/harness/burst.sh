#!/bin/bash
#
# Burst Benchmark for Route53 ACK Controller
#
# Pre-loads all CRs with no controller running, then starts the controller
# and measures time to fully reconcile. This isolates controller throughput
# from CR submission rate.
#
# Prerequisites:
#   - AWS credentials with Route53 access for account 585008087740
#   - kubectl context pointing to a cluster with Route53 CRDs installed
#   - Controller binary built: go build -o test/harness/controller-bin ./cmd/controller/
#
# Usage:
#   cd ~/acks/api-level-route53/test/harness
#   CONTEXT=r53-bench ZONES=3 RECORDS=50 SYNCS=50 bash burst.sh
#
# Environment variables:
#   CONTEXT  - kubectl context (required)
#   ZONES    - number of hosted zones to create (default: 3)
#   RECORDS  - records per zone (default: 50)
#   SYNCS    - max concurrent reconciles (default: 50)
#   TIMEOUT  - max wait for sync in seconds (default: 300)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTROLLER_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CONTEXT="${CONTEXT:?Set CONTEXT to your kubectl context}"
ZONES="${ZONES:-3}"
RECORDS="${RECORDS:-50}"
SYNCS="${SYNCS:-50}"
TIMEOUT="${TIMEOUT:-300}"
TOTAL=$((ZONES * RECORDS))

cd "$CONTROLLER_DIR"
unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN

echo "=== Route53 ACK Burst Benchmark ==="
echo "Context: $CONTEXT"
echo "Config:  $ZONES zones × $RECORDS records = $TOTAL total"
echo "Syncs:   $SYNCS concurrent reconciles"
echo ""

# Build if needed
if [ ! -f "$SCRIPT_DIR/controller-bin" ]; then
  echo "[0] Building controller..."
  go build -o "$SCRIPT_DIR/controller-bin" ./cmd/controller/
fi

# Phase 1: Create hosted zones
echo "[1] Creating $ZONES hosted zones in Route53..."
ZONE_IDS=()
ZONE_DOMAINS=()
for i in $(seq 0 $((ZONES-1))); do
  DOMAIN="burst-${RANDOM}-${i}.bench.internal"
  ZID=$(aws route53 create-hosted-zone \
    --name "$DOMAIN" \
    --caller-reference "burst-$$-$i-$(date +%s%N)" \
    --query 'HostedZone.Id' --output text 2>&1 | sed 's|/hostedzone/||')
  ZONE_IDS+=("$ZID")
  ZONE_DOMAINS+=("$DOMAIN")
  echo "  Zone $i: $DOMAIN ($ZID)"
done

# Phase 2: Submit all CRs (no controller running)
echo ""
echo "[2] Submitting $TOTAL CRs (controller NOT running)..."
for i in "${!ZONE_IDS[@]}"; do
  ZID="${ZONE_IDS[$i]}"
  DOMAIN="${ZONE_DOMAINS[$i]}"
  ZLABEL=$(echo "$ZID" | tr '[:upper:]' '[:lower:]' | cut -c1-8)
  for j in $(seq 0 $((RECORDS-1))); do
    CRNAME="burst-${ZLABEL}-$(printf '%04d' $j)"
    RECNAME="r$(printf '%04d' $j).${DOMAIN}"
    IP="10.$((RANDOM%255)).$((RANDOM%255)).$((RANDOM%255+1))"
    cat <<EOF | kubectl --context "$CONTEXT" apply -f - > /dev/null 2>&1 &
apiVersion: route53.services.k8s.aws/v1alpha1
kind: RecordSet
metadata:
  name: $CRNAME
  namespace: default
  labels:
    harness: burst-bench
spec:
  hostedZoneID: "$ZID"
  name: "$RECNAME"
  recordType: "A"
  ttl: 300
  resourceRecords:
    - value: "$IP"
EOF
    # Batch kubectl calls to avoid overwhelming API server
    if (( (j+1) % 50 == 0 )); then wait; fi
  done
done
wait
sleep 3
ACTUAL=$(kubectl --context "$CONTEXT" get recordsets -l harness=burst-bench --no-headers 2>/dev/null | wc -l)
echo "  Verified: $ACTUAL/$TOTAL CRs in cluster"
if [ "$ACTUAL" -lt "$TOTAL" ]; then
  echo "  WARNING: only $ACTUAL of $TOTAL CRs submitted successfully"
fi

# Phase 3: Start controller
echo ""
echo "[3] Starting controller ($SYNCS concurrent syncs)..."
START=$(date +%s)

"$SCRIPT_DIR/controller-bin" \
  --aws-region us-west-2 \
  --enable-leader-election=false \
  --log-level info \
  --healthz-addr ":8091" \
  --metrics-addr ":8092" \
  --reconcile-default-max-concurrent-syncs "$SYNCS" \
  > "$SCRIPT_DIR/controller.log" 2>&1 &
CTRL_PID=$!

for i in $(seq 1 12); do
  sleep 5
  grep -q "Starting Controller" "$SCRIPT_DIR/controller.log" && break
done
if ! grep -q "Starting Controller" "$SCRIPT_DIR/controller.log"; then
  echo "  FAILED TO START"
  tail -5 "$SCRIPT_DIR/controller.log"
  kill $CTRL_PID 2>/dev/null
  exit 1
fi
echo "  Controller running (PID $CTRL_PID)"

# Phase 4: Wait for all synced
echo ""
echo "[4] Waiting for reconciliation..."
LAST_SYNCED=0
STALL_COUNT=0
for i in $(seq 1 $((TIMEOUT / 5))); do
  sleep 5
  SYNCED=$(kubectl --context "$CONTEXT" get recordsets -l harness=burst-bench \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="ACK.ResourceSynced")].status}{"\n"}{end}' 2>/dev/null | grep -c "True" 2>/dev/null || echo 0)
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  echo "  [${ELAPSED}s] $SYNCED/$ACTUAL synced"

  if [ "$SYNCED" -ge "$ACTUAL" ] 2>/dev/null; then
    echo ""
    echo "======================================"
    echo "  ALL $ACTUAL RECORDS SYNCED in ${ELAPSED}s"
    echo "  EFFECTIVE OPS/SEC: $(python3 -c "print(f'{$ACTUAL/$ELAPSED:.1f}')" 2>/dev/null || echo "$ACTUAL/$ELAPSED")"
    echo "======================================"
    break
  fi

  # Detect stalls
  if [ "$SYNCED" = "$LAST_SYNCED" ]; then
    STALL_COUNT=$((STALL_COUNT + 1))
    if [ "$STALL_COUNT" -ge 12 ]; then  # 60s stall
      echo ""
      echo "  STALLED at $SYNCED/$ACTUAL for 60s, stopping."
      break
    fi
  else
    STALL_COUNT=0
  fi
  LAST_SYNCED="$SYNCED"
done

# Stats
echo ""
echo "=== Stats ==="
echo "Throttle errors: $(grep -c 'Throttling' "$SCRIPT_DIR/controller.log" 2>/dev/null || echo 0)"
echo "Records created: $(grep -c 'created new resource' "$SCRIPT_DIR/controller.log" 2>/dev/null || echo 0)"
echo ""
echo "Peak creates/second:"
grep "created new resource" "$SCRIPT_DIR/controller.log" | \
  awk -F'"ts":"' '{print $2}' | awk -F'"' '{print $1}' | cut -c1-19 | sort | uniq -c | sort -rn | head -5
echo ""
echo "Controller log: $SCRIPT_DIR/controller.log"

# Phase 5: Cleanup
echo ""
echo "[5] Cleaning up..."
kill $CTRL_PID 2>/dev/null; wait $CTRL_PID 2>/dev/null

kubectl --context "$CONTEXT" get recordsets -l harness=burst-bench -o name 2>/dev/null | \
  xargs -P20 -I{} kubectl --context "$CONTEXT" patch {} --type=merge -p '{"metadata":{"finalizers":[]}}' > /dev/null 2>&1
kubectl --context "$CONTEXT" delete recordsets -l harness=burst-bench --wait=false > /dev/null 2>&1

for ZID in "${ZONE_IDS[@]}"; do
  aws route53 list-resource-record-sets --hosted-zone-id "$ZID" \
    --query 'ResourceRecordSets[?Type!=`NS`&&Type!=`SOA`]' --output json 2>/dev/null | \
    python3 -c "
import json,sys,subprocess
records=json.load(sys.stdin)
if records:
  for i in range(0,len(records),100):
    batch=[{'Action':'DELETE','ResourceRecordSet':r} for r in records[i:i+100]]
    subprocess.run(['aws','route53','change-resource-record-sets','--hosted-zone-id','$ZID',
      '--change-batch',json.dumps({'Changes':batch})],capture_output=True)
" 2>/dev/null
  aws route53 delete-hosted-zone --id "$ZID" > /dev/null 2>&1
done
echo "Done!"
