#!/bin/bash
set -euo pipefail
WF_ID="wf-4"
BASELINE="snape"
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

echo "Applying ConfigMap wf-4-task1-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task1-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task1
    baseline: snape
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "snape"
  TASK_EXEC_TIME: "9.6951"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task1 (min=2, max=21) for baseline snape..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 120 --method POST --configmap wf-4-task1-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-4-task2-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task2-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task2
    baseline: snape
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "snape"
  TASK_EXEC_TIME: "22.2126"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task2 (min=6, max=61) for baseline snape..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task2-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-4-task3-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task3-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task3
    baseline: snape
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "snape"
  TASK_EXEC_TIME: "16.4318"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task3 (min=6, max=61) for baseline snape..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task3-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-4-task4-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task4-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task4
    baseline: snape
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "snape"
  TASK_EXEC_TIME: "16.2145"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task4 (min=6, max=61) for baseline snape..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task4-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-4-task5-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task5-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task5
    baseline: snape
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "snape"
  TASK_EXEC_TIME: "26.5062"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task5 (min=6, max=61) for baseline snape..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 120 --method POST --configmap wf-4-task5-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Applying ConfigMap wf-4-task6-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task6-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task6
    baseline: snape
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "snape"
  TASK_EXEC_TIME: "6.742"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task6 (min=2, max=21) for baseline snape..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 120 --method POST --configmap wf-4-task6-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task6"

echo "Applying ConfigMap wf-4-task7-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task7-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task7
    baseline: snape
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "snape"
  TASK_EXEC_TIME: "4.0415"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task7 (min=1, max=11) for baseline snape..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task7-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task7"

echo "Applying ConfigMap wf-4-task8-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task8-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task8
    baseline: snape
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "snape"
  TASK_EXEC_TIME: "1.6676"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task8 (min=1, max=11) for baseline snape..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task8-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task8"

echo "Applying ConfigMap wf-4-task9-snape-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task9-snape-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task9
    baseline: snape
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: ""
  BASELINE: "snape"
  TASK_EXEC_TIME: "2.556"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  SNAPE_ENABLED: "true"
  SNAPE_TARGET_AVAIL: "0.9996"
  SNAPE_MIN_OD_RATIO: "0.0"
  SNAPE_MAX_OD_RATIO: "1.0"
CMEOF

echo "Creating function task9 (min=1, max=11) for baseline snape..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-4-task9-snape-cfg -n $NS
sleep 3
wait_rollout_for_task "task9"

echo "Recreating route wf-4-route ..."
fission route delete --name=wf-4-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-4-route --function task1 --url /wf-4 --method POST -n $NS
echo "✅ Deployed workflow wf-4 for baseline 'Snape'"