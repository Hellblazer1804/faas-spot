#!/bin/bash
set -euo pipefail
WF_ID="wf-2"
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

echo "Applying ConfigMap wf-2-task1-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task1-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task1
    baseline: static_over_4x
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "2.3997"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-task1-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-2-task2-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task2-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task2
    baseline: static_over_4x
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "7.2566"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-task2-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-2-task3-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3
    baseline: static_over_4x
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3a,http://router.fission.svc.cluster.local/fission-function/task3b"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "12.9018"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-task3-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-2-task3a-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3a-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3a
    baseline: static_over_4x
data:
  TASK_ID: "task3a"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr1"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "4.1604"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3a (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name task3a -n $NS >/dev/null 2>&1 || true
fission fn create --name task3a --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-task3a-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task3a"

echo "Applying ConfigMap wf-2-task3b-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3b-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3b
    baseline: static_over_4x
data:
  TASK_ID: "task3b"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr2"
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "3.2514"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3b (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name task3b -n $NS >/dev/null 2>&1 || true
fission fn create --name task3b --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-task3b-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "task3b"

echo "Applying ConfigMap wf-2-taskr1-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-taskr1-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: taskr1
    baseline: static_over_4x
data:
  TASK_ID: "taskr1"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: ""
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "1.3991"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr1 (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name taskr1 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-taskr1-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr1"

echo "Applying ConfigMap wf-2-taskr2-static-over-4x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-taskr2-static-over-4x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: taskr2
    baseline: static_over_4x
data:
  TASK_ID: "taskr2"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: ""
  BASELINE: "static_over_4x"
  TASK_EXEC_TIME: "2.4756"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr2 (min=4, max=41) for baseline static_over_4x..."
fission fn delete --name taskr2 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 4 --maxscale 41 --fntimeout 120 --method POST --configmap wf-2-taskr2-static-over-4x-cfg -n $NS
sleep 3
wait_rollout_for_task "taskr2"

echo "Recreating route wf-2-route ..."
fission route delete --name=wf-2-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-2-route --function task1 --url /wf-2 --method POST -n $NS
echo "✅ Deployed workflow wf-2 for baseline 'Static Overprovision 4x'"