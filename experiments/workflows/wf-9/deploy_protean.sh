#!/bin/bash
set -euo pipefail
WF_ID="wf-9"
BASELINE="protean"
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

echo "Applying ConfigMap wf-9-task1-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task1-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task1
    baseline: protean
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "protean"
  TASK_EXEC_TIME: "2.4391"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task1 (min=9, max=91) for baseline protean..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-9-task1-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-9-task2-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task2-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task2
    baseline: protean
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "protean"
  TASK_EXEC_TIME: "8.2388"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task2 (min=12, max=121) for baseline protean..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 12 --maxscale 121 --fntimeout 120 --method POST --configmap wf-9-task2-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-9-task3-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task3-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task3
    baseline: protean
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "protean"
  TASK_EXEC_TIME: "28.8557"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task3 (min=3, max=31) for baseline protean..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-9-task3-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-9-task4-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task4-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task4
    baseline: protean
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "protean"
  TASK_EXEC_TIME: "26.2691"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task4 (min=3, max=31) for baseline protean..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-9-task4-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-9-task5-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task5-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task5
    baseline: protean
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "protean"
  TASK_EXEC_TIME: "19.9432"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task5 (min=9, max=91) for baseline protean..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-9-task5-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Applying ConfigMap wf-9-task6-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task6-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task6
    baseline: protean
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "protean"
  TASK_EXEC_TIME: "11.8645"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task6 (min=3, max=31) for baseline protean..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-9-task6-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task6"

echo "Applying ConfigMap wf-9-task7-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task7-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task7
    baseline: protean
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "protean"
  TASK_EXEC_TIME: "8.2165"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task7 (min=3, max=31) for baseline protean..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-9-task7-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task7"

echo "Applying ConfigMap wf-9-task8-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task8-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task8
    baseline: protean
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "protean"
  TASK_EXEC_TIME: "7.7668"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task8 (min=3, max=31) for baseline protean..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-9-task8-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task8"

echo "Applying ConfigMap wf-9-task9-protean-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-9-task9-protean-cfg
  labels:
    app: serverless-wf
    wf-id: wf-9
    task-id: task9
    baseline: protean
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-9"
  NEXT_TASK_URL: ""
  BASELINE: "protean"
  TASK_EXEC_TIME: "11.8613"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  PROTEAN_SCALING: "true"
CMEOF

echo "Creating function task9 (min=1, max=11) for baseline protean..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-9-task9-protean-cfg -n $NS
sleep 3
wait_rollout_for_task "task9"

echo "Recreating route wf-9-route ..."
fission route delete --name=wf-9-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-9-route --function task1 --url /wf-9 --method POST -n $NS
echo "✅ Deployed workflow wf-9 for baseline 'Protean'"