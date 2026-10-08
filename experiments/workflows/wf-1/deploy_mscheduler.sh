#!/bin/bash
set -euo pipefail
WF_ID="wf-1"
BASELINE="mscheduler"
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

echo "Applying ConfigMap wf-1-task1-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task1-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task1
    baseline: mscheduler
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "3.5686"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function task1 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task1-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-1-task2-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task2-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task2
    baseline: mscheduler
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "5.7383"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function task2 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task2-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-1-task3-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3
    baseline: mscheduler
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3a,http://router.fission.svc.cluster.local/fission-function/task3b"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "12.5371"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function task3 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task3-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-1-task3a-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3a-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3a
    baseline: mscheduler
data:
  TASK_ID: "task3a"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr1"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "3.6273"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function task3a (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task3a -n $NS >/dev/null 2>&1 || true
fission fn create --name task3a --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task3a-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task3a"

echo "Applying ConfigMap wf-1-task3b-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3b-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3b
    baseline: mscheduler
data:
  TASK_ID: "task3b"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr2"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "4.8813"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function task3b (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task3b -n $NS >/dev/null 2>&1 || true
fission fn create --name task3b --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-task3b-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task3b"

echo "Applying ConfigMap wf-1-taskr1-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-taskr1-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: taskr1
    baseline: mscheduler
data:
  TASK_ID: "taskr1"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: ""
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "3.8942"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function taskr1 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name taskr1 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-taskr1-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr1"

echo "Applying ConfigMap wf-1-taskr2-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-taskr2-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: taskr2
    baseline: mscheduler
data:
  TASK_ID: "taskr2"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: ""
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "2.1966"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  MSCHEDULER_ENABLED: "true"
  MSCHEDULER_ENABLED: "true"
  MSCHED_MAKESPAN_S: "900"
  MSCHED_INTERVAL_S: "1800"
  MSCHED_DUMP_S: "36.0"
  MSCHED_OVERHEAD_SR: "1.0"
  MSCHED_RESTORE_S: "36.0"
  MSCHED_MIN_CKPTS: "1"
  MSCHED_MIN_SPOT_SAVINGS: "0.65"
  MSCHED_PRICE_OD: "3.00"
  MSCHED_PRICE_SPOT: "0.90"
CMEOF

echo "Creating function taskr2 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name taskr2 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-1-taskr2-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr2"

echo "Recreating route wf-1-route ..."
fission route delete --name=wf-1-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-1-route --function task1 --url /wf-1 --method POST -n $NS
echo "✅ Deployed workflow wf-1 for baseline 'MScheduler'"