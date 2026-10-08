#!/bin/bash
#
# Cleanup Fission resources (functions, routes) + labeled ConfigMaps + Kubernetes resources.
# Usage:
#   ./cleanup.sh              # blanket cleanup
#   ./cleanup.sh WF_ID        # targeted cleanup for a workflow id
# Env:
#   NS=default                # namespace (optional)
set -e
set +o pipefail
set +e

NS="${NS:-default}"
WF_ID="$1"

# Helper: wait for pods with a label selector to terminate (max 60s)
wait_pods_terminated() {
  local selector="$1"
  local timeout=60
  local waited=0
  while [ "$waited" -lt "$timeout" ]; do
    local count
    count=$(kubectl -n "$NS" get pods -l "$selector" --no-headers 2>/dev/null | wc -l)
    if [ "$count" -eq 0 ]; then
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  echo "   ⚠️ Some pods still terminating after ${timeout}s (selector: $selector)"
}

if [ -n "$WF_ID" ]; then
  echo "🧹 Performing targeted cleanup for workflow: $WF_ID (namespace: $NS)"
  WF_DIR="workflows/$WF_ID"
  TASKS_JSON="$WF_DIR/tasks.json"

  if [ -f "$TASKS_JSON" ]; then
    TASKS=$(python3 -c "import json; t=json.load(open('$TASKS_JSON'))['tasks']; print(' '.join(t.keys()))")
    for fn in $TASKS; do
      echo " - Deleting function: $fn"
      fission fn delete --name "$fn" >/dev/null 2>&1 || true
      # Also delete Kubernetes deployments directly (Fission newdeploy)
      kubectl -n "$NS" delete deploy -l "functionName=$fn" --ignore-not-found >/dev/null 2>&1 || true
    done
  else
    echo "⚠️ tasks.json not found for $WF_ID, best-effort function cleanup (task*):"
    fission fn list 2>/dev/null | awk 'NR>1 {print $1}' | grep -E '^task' | while read -r fn; do
      echo " - Deleting function: $fn"
      fission fn delete --name "$fn" >/dev/null 2>&1 || true
      kubectl -n "$NS" delete deploy -l "functionName=$fn" --ignore-not-found >/dev/null 2>&1 || true
    done
  fi

  echo " - Deleting route: ${WF_ID}-route"
  fission route delete --name="${WF_ID}-route" >/dev/null 2>&1 || true

  echo " - Deleting ConfigMaps for wf-id=${WF_ID}"
  kubectl -n "$NS" delete configmap -l "app=serverless-wf,wf-id=${WF_ID}" --ignore-not-found >/dev/null 2>&1 || true

  # Wait for task pods to terminate before returning
  echo " - Waiting for pods to terminate..."
  wait_pods_terminated "functionName"

else
  echo "🧹 Performing BLANKET cleanup of ALL workflow-related resources... (namespace: $NS)"

  echo " - Deleting all workflow functions (task*)..."
  fission fn list 2>/dev/null | awk 'NR>1 {print $1}' | grep -E '^task' | while read -r fn; do
    echo "   - $fn"
    fission fn delete --name "$fn" >/dev/null 2>&1 || true
  done

  echo " - Deleting all workflow routes (*-route)..."
  fission route list 2>/dev/null | awk 'NR>1 {print $1}' | grep -E '\-route$' | while read -r rt; do
    echo "   - $rt"
    fission route delete --name "$rt" >/dev/null 2>&1 || true
  done

  echo " - Deleting all labeled ConfigMaps (app=serverless-wf)..."
  kubectl -n "$NS" delete configmap -l "app=serverless-wf" --ignore-not-found >/dev/null 2>&1 || true

  # Direct Kubernetes cleanup for any orphaned newdeploy deployments
  echo " - Deleting orphaned Fission newdeploy deployments..."
  kubectl -n "$NS" delete deploy -l "executorType=newdeploy" --ignore-not-found >/dev/null 2>&1 || true

  # Wait for all function pods to terminate
  echo " - Waiting for pods to terminate..."
  wait_pods_terminated "executorType=newdeploy"
fi

set -e
echo "✅ Cleanup complete."
