#!/bin/bash
set -euo pipefail
WF_ID="wf-8"
BASELINE="static_over_3x"
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

echo "Applying ConfigMap wf-8-task1-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task1-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task1
    baseline: static_over_3x
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "4.6426"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=27, max=271) for baseline static_over_3x..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 27 --maxscale 271 --fntimeout 120 --method POST --configmap wf-8-task1-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-8-task2-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task2-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task2
    baseline: static_over_3x
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "5.0762"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=36, max=361) for baseline static_over_3x..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 36 --maxscale 361 --fntimeout 120 --method POST --configmap wf-8-task2-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-8-task3-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task3-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task3
    baseline: static_over_3x
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "21.346"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=9, max=91) for baseline static_over_3x..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task3-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-8-task4-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task4-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task4
    baseline: static_over_3x
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "11.2845"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=9, max=91) for baseline static_over_3x..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task4-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-8-task5-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task5-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task5
    baseline: static_over_3x
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "18.1994"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task5 (min=27, max=271) for baseline static_over_3x..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 27 --maxscale 271 --fntimeout 120 --method POST --configmap wf-8-task5-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Applying ConfigMap wf-8-task6-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task6-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task6
    baseline: static_over_3x
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "17.7992"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task6 (min=9, max=91) for baseline static_over_3x..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task6-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task6"

echo "Applying ConfigMap wf-8-task7-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task7-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task7
    baseline: static_over_3x
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "3.3972"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task7 (min=9, max=91) for baseline static_over_3x..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task7-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task7"

echo "Applying ConfigMap wf-8-task8-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task8-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task8
    baseline: static_over_3x
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "9.6451"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task8 (min=9, max=91) for baseline static_over_3x..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task8-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task8"

echo "Applying ConfigMap wf-8-task9-static-over-3x-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task9-static-over-3x-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task9
    baseline: static_over_3x
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: ""
  BASELINE: "static_over_3x"
  TASK_EXEC_TIME: "8.4029"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task9 (min=3, max=31) for baseline static_over_3x..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task9-static-over-3x-cfg -n $NS
sleep 3
wait_rollout_for_task "task9"

echo "Recreating route wf-8-route ..."
fission route delete --name=wf-8-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-8-route --function task1 --url /wf-8 --method POST -n $NS
echo "✅ Deployed workflow wf-8 for baseline 'Static Overprovision 3x'"