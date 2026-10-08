#!/bin/bash
set -euo pipefail
WF_ID="wf-1"
BASELINE="hourglass"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"

echo "Namespace: $NS | Workflow: $WF_ID | Baseline: $BASELINE"

# Robust rollout wait with auto-retry if deployment is recreated
wait_rollout_for_task() {
  local task="$1"
  local timeout_sec=420
  local waited=0
  local dep_name=""
  while [ "$waited" -lt "$timeout_sec" ]; do
    dep_name="$(kubectl -n "$NS" get deployments -l functionName="$task" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
    if [ -n "$dep_name" ]; then
      # Try to wait for rollout; if the object was deleted/recreated, retry
      if kubectl -n "$NS" rollout status "deploy/$dep_name" --timeout=30s 2>/dev/null; then
        # Double-check availability equals desired to avoid race on recreate
        local desired available
        desired="$(kubectl -n "$NS" get deploy "$dep_name" -o jsonpath='{.status.replicas}' 2>/dev/null || echo 0)"
        available="$(kubectl -n "$NS" get deploy "$dep_name" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)"
        if [ -n "$desired" ] && [ -n "$available" ] && [ "$desired" = "$available" ] && [ "$available" != "" ]; then
          return 0
        fi
      fi
    fi
    sleep 3
    waited=$((waited+3))
  done
  echo "⚠️ Timeout waiting for rollout of task '$task' after ${timeout_sec}s"
  return 1
}

echo "Applying ConfigMap wf-1-task1-hourglass-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task1-hourglass-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task1
    baseline: hourglass
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "hourglass"
  TASK_EXEC_TIME: "4.3941"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_EXEC_TIME: "4.0"
  CRITICAL_PATH_TIME: "4.0"
  CKPT_STRATEGY: "hourglass"
  HOURGLASS_DEADLINE_FACTOR: "1.5"
  HOURGLASS_SAFE_SLACK: "0.3"
  HOURGLASS_RISKY_SLACK: "0.1"
  HOURGLASS_CONFIG: "SPOT_AGGRESSIVE"
CMEOF

echo "Creating function task1 (min=1, max=11) for baseline hourglass..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task1-hourglass-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-1-task2-hourglass-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task2-hourglass-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task2
    baseline: hourglass
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "hourglass"
  TASK_EXEC_TIME: "1.2701"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_EXEC_TIME: "4.0"
  CRITICAL_PATH_TIME: "4.0"
  CKPT_STRATEGY: "hourglass"
  HOURGLASS_DEADLINE_FACTOR: "1.5"
  HOURGLASS_SAFE_SLACK: "0.3"
  HOURGLASS_RISKY_SLACK: "0.1"
  HOURGLASS_CONFIG: "SPOT_AGGRESSIVE"
CMEOF

echo "Creating function task2 (min=1, max=11) for baseline hourglass..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task2-hourglass-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-1-task3-hourglass-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3-hourglass-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3
    baseline: hourglass
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "hourglass"
  TASK_EXEC_TIME: "1.621"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_EXEC_TIME: "4.0"
  CRITICAL_PATH_TIME: "4.0"
  CKPT_STRATEGY: "hourglass"
  HOURGLASS_DEADLINE_FACTOR: "1.5"
  HOURGLASS_SAFE_SLACK: "0.3"
  HOURGLASS_RISKY_SLACK: "0.1"
  HOURGLASS_CONFIG: "SPOT_AGGRESSIVE"
CMEOF

echo "Creating function task3 (min=1, max=11) for baseline hourglass..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task3-hourglass-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-1-task4-hourglass-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task4-hourglass-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task4
    baseline: hourglass
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: ""
  BASELINE: "hourglass"
  TASK_EXEC_TIME: "1.7978"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_EXEC_TIME: "4.0"
  CRITICAL_PATH_TIME: "4.0"
  CKPT_STRATEGY: "hourglass"
  HOURGLASS_DEADLINE_FACTOR: "1.5"
  HOURGLASS_SAFE_SLACK: "0.3"
  HOURGLASS_RISKY_SLACK: "0.1"
  HOURGLASS_CONFIG: "SPOT_AGGRESSIVE"
CMEOF

echo "Creating function task4 (min=1, max=11) for baseline hourglass..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task4-hourglass-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Recreating route wf-1-route ..."
fission route delete --name=wf-1-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-1-route --function task1 --url /wf-1 --method POST -n $NS
echo "✅ Deployed workflow wf-1 for baseline 'Hourglass (Slack-aware + OD Fallback)'"