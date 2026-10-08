#!/bin/bash
set -euo pipefail
WF_ID="wf-6"
BASELINE="ours"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"
echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"

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
      if kubectl -n "$NS" rollout status "deploy/$dep_name" --timeout=30s; then
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

echo "Applying ConfigMap wf-6-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-6-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-6
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-6"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "9.3806"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=6 max=601)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 601 --fntimeout 900 --method POST --configmap wf-6-task1-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-6-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-6-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-6
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-6"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "2.3765"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=6 max=601)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 601 --fntimeout 900 --method POST --configmap wf-6-task2-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-6-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-6-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-6
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-6"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "ours"
  TASK_EXEC_TIME: "5.1703"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=2 max=201)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 201 --fntimeout 900 --method POST --configmap wf-6-task3-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-6-task4-ours-cfg for task4..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-6-task4-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-6
    task-id: task4
    baseline: ours
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-6"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "3.3497"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=1 max=101)..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-6-task4-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Recreating route wf-6-route -> task1 ..."
fission route delete --name=wf-6-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-6-route --function task1 --url /wf-6 --method POST -n $NS
echo "✅ Deployed wf-6 for baseline ours"