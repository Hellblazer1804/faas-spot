#!/bin/bash
set -euo pipefail
WF_ID="wf-7"
BASELINE="static_over_4x"
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

echo "Applying ConfigMap wf-7-task1-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task1-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task1
    baseline: static_over_4x
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "4.4229"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=32, max=321) for baseline static_over_4x..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 32 --maxscale 321 --fntimeout 120 --method POST --configmap wf-7-task1-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-7-task2-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task2-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task2
    baseline: static_over_4x
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "8.5944"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=64, max=641) for baseline static_over_4x..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 64 --maxscale 641 --fntimeout 120 --method POST --configmap wf-7-task2-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-7-task3-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task3-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task3
    baseline: static_over_4x
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "15.2573"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=32, max=321) for baseline static_over_4x..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 32 --maxscale 321 --fntimeout 120 --method POST --configmap wf-7-task3-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-7-task4-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task4-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task4
    baseline: static_over_4x
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "16.5217"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=32, max=321) for baseline static_over_4x..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 32 --maxscale 321 --fntimeout 120 --method POST --configmap wf-7-task4-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-7-task5-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task5-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task5
    baseline: static_over_4x
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: ""
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "6.0411"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task5 (min=8, max=81) for baseline static_over_4x..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 8 --maxscale 81 --fntimeout 120 --method POST --configmap wf-7-task5-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Recreating route wf-7-route ..."
fission route delete --name=wf-7-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-7-route --function task1 --url /wf-7 --method POST -n $NS
echo "✅ Deployed workflow wf-7 for baseline 'Static Overprovision 4x'"