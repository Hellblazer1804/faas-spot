#!/bin/bash
set -euo pipefail
WF_ID="wf-1"
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

echo "Applying ConfigMap wf-1-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "5.6947"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=1 max=101)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-task1-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-1-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "6.6563"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=1 max=101)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-task2-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-1-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3a,http://router.fission.svc.cluster.local/fission-function/task3b"
  BASELINE: "ours"
  TASK_EXEC_TIME: "13.5851"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=1 max=101)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-task3-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-1-task3a-ours-cfg for task3a..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3a-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3a
    baseline: ours
data:
  TASK_ID: "task3a"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr1"
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.7946"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3a (min=1 max=101)..."
fission fn delete --name task3a -n $NS >/dev/null 2>&1 || true
fission fn create --name task3a --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-task3a-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task3a"

echo "Applying ConfigMap wf-1-task3b-ours-cfg for task3b..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-task3b-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: task3b
    baseline: ours
data:
  TASK_ID: "task3b"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.2011"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3b (min=1 max=101)..."
fission fn delete --name task3b -n $NS >/dev/null 2>&1 || true
fission fn create --name task3b --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-task3b-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "task3b"

echo "Applying ConfigMap wf-1-taskr1-ours-cfg for taskr1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-taskr1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: taskr1
    baseline: ours
data:
  TASK_ID: "taskr1"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.5078"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr1 (min=1 max=101)..."
fission fn delete --name taskr1 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-taskr1-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr1"

echo "Applying ConfigMap wf-1-taskr2-ours-cfg for taskr2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-1-taskr2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-1
    task-id: taskr2
    baseline: ours
data:
  TASK_ID: "taskr2"
  WORKFLOW_ID: "wf-1"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.7309"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr2 (min=1 max=101)..."
fission fn delete --name taskr2 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 101 --fntimeout 900 --method POST --configmap wf-1-taskr2-ours-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr2"

echo "Recreating route wf-1-route -> task1 ..."
fission route delete --name=wf-1-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-1-route --function task1 --url /wf-1 --method POST -n $NS
echo "✅ Deployed wf-1 for baseline ours"