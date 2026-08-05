#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTROLLER_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
HARNESS_DIR="$SCRIPT_DIR"

CONTEXT="${KUBE_CONTEXT:-route53-test}"
NAMESPACE="${NAMESPACE:-default}"
ZONES="${ZONES:-3}"
RECORDS="${RECORDS:-50}"
DURATION="${DURATION:-5m}"

echo "=== Route53 ACK Controller Benchmark ==="
echo "Controller: $CONTROLLER_DIR"
echo "Context: $CONTEXT"
echo "Zones: $ZONES, Records/zone: $RECORDS"
echo ""

# Build controller
echo "[1/4] Building controller..."
cd "$CONTROLLER_DIR"
go build -o "$HARNESS_DIR/controller-bin" ./cmd/controller/ 2>&1
echo "  Done."

# Build harness
echo "[2/4] Building harness..."
cd "$HARNESS_DIR"
go build -o "$HARNESS_DIR/harness-bin" . 2>&1
echo "  Done."

# Start controller in background
echo "[3/4] Starting controller..."
cd "$CONTROLLER_DIR"

# Unset IRSA-related env vars so controller uses local AWS creds
unset AWS_WEB_IDENTITY_TOKEN_FILE
unset AWS_ROLE_ARN

"$HARNESS_DIR/controller-bin" \
  --aws-region us-west-2 \
  --enable-leader-election=false \
  --log-level info \
  --healthz-addr ":8091" \
  --metrics-addr ":8092" \
  --reconcile-default-max-concurrent-syncs 2 \
  > "$HARNESS_DIR/controller.log" 2>&1 &
CONTROLLER_PID=$!
echo "  Controller PID: $CONTROLLER_PID"

# Wait for controller to start
sleep 5

if ! kill -0 $CONTROLLER_PID 2>/dev/null; then
  echo "ERROR: Controller failed to start. Logs:"
  tail -30 "$HARNESS_DIR/controller.log"
  exit 1
fi
echo "  Controller is running."

# Run harness
echo "[4/4] Running harness..."
cd "$HARNESS_DIR"
"$HARNESS_DIR/harness-bin" \
  -zones="$ZONES" \
  -records-per-zone="$RECORDS" \
  -duration="$DURATION" \
  -namespace="$NAMESPACE" \
  -context="$CONTEXT"
HARNESS_EXIT=$?

# Stop controller
echo ""
echo "Stopping controller (PID: $CONTROLLER_PID)..."
kill $CONTROLLER_PID 2>/dev/null || true
wait $CONTROLLER_PID 2>/dev/null || true

if [ $HARNESS_EXIT -ne 0 ]; then
  echo "Harness failed. Controller logs (last 50 lines):"
  tail -50 "$HARNESS_DIR/controller.log"
fi

echo ""
echo "Controller logs saved to: $HARNESS_DIR/controller.log"
exit $HARNESS_EXIT
