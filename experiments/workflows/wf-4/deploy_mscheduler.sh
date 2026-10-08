#!/bin/bash
set -euo pipefail
WF_ID="wf-4"
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

echo "Applying ConfigMap wf-4-task1-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task1-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task1
    baseline: mscheduler
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "6.6192"
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

echo "Creating function task1 (min=2, max=21) for baseline mscheduler..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 120 --method POST --configmap wf-4-task1-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-4-task2-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task2-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task2
    baseline: mscheduler
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "25.2701"
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

echo "Creating function task2 (min=6, max=61) for baseline mscheduler..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task2-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-4-task3-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task3-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task3
    baseline: mscheduler
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "25.8489"
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

echo "Creating function task3 (min=6, max=61) for baseline mscheduler..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task3-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-4-task4-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task4-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task4
    baseline: mscheduler
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "17.1719"
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

echo "Creating function task4 (min=6, max=61) for baseline mscheduler..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task4-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-4-task5-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task5-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task5
    baseline: mscheduler
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "16.9091"
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

echo "Creating function task5 (min=6, max=61) for baseline mscheduler..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task5-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Applying ConfigMap wf-4-task6-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task6-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task6
    baseline: mscheduler
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "8.4891"
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

echo "Creating function task6 (min=2, max=21) for baseline mscheduler..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 120 --method POST --configmap wf-4-task6-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task6"

echo "Applying ConfigMap wf-4-task7-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task7-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task7
    baseline: mscheduler
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "2.1882"
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

echo "Creating function task7 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task7-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task7"

echo "Applying ConfigMap wf-4-task8-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task8-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task8
    baseline: mscheduler
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "3.1713"
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

echo "Creating function task8 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task8-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task8"

echo "Applying ConfigMap wf-4-task9-mscheduler-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task9-mscheduler-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task9
    baseline: mscheduler
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: ""
  BASELINE: "mscheduler"
  TASK_EXEC_TIME: "2.3046"
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

echo "Creating function task9 (min=1, max=11) for baseline mscheduler..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task9-mscheduler-cfg -n $NS
sleep 3
wait_rollout_for_task "task9"

echo "Recreating route wf-4-route ..."
fission route delete --name=wf-4-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-4-route --function task1 --url /wf-4 --method POST -n $NS
echo "✅ Deployed workflow wf-4 for baseline 'MScheduler'"