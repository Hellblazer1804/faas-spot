#!/bin/bash
set -euo pipefail
WF_ID="wf-8"
BASELINE="bag_of_tasks"
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

echo "Applying ConfigMap wf-8-task1-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task1-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task1
    baseline: bag_of_tasks
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "3.9706"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task1 (min=9, max=91) for baseline bag_of_tasks..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task1-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task1"

echo "Applying ConfigMap wf-8-task2-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task2-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task2
    baseline: bag_of_tasks
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "8.1398"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task2 (min=12, max=121) for baseline bag_of_tasks..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 12 --maxscale 121 --fntimeout 120 --method POST --configmap wf-8-task2-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task2"

echo "Applying ConfigMap wf-8-task3-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task3-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task3
    baseline: bag_of_tasks
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "15.0408"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task3 (min=3, max=31) for baseline bag_of_tasks..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task3-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task3"

echo "Applying ConfigMap wf-8-task4-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task4-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task4
    baseline: bag_of_tasks
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "13.4205"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task4 (min=3, max=31) for baseline bag_of_tasks..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task4-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task4"

echo "Applying ConfigMap wf-8-task5-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task5-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task5
    baseline: bag_of_tasks
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "15.5727"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task5 (min=9, max=91) for baseline bag_of_tasks..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 120 --method POST --configmap wf-8-task5-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task5"

echo "Applying ConfigMap wf-8-task6-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task6-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task6
    baseline: bag_of_tasks
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "12.8182"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task6 (min=3, max=31) for baseline bag_of_tasks..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task6-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task6"

echo "Applying ConfigMap wf-8-task7-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task7-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task7
    baseline: bag_of_tasks
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "4.6077"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task7 (min=3, max=31) for baseline bag_of_tasks..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task7-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task7"

echo "Applying ConfigMap wf-8-task8-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task8-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task8
    baseline: bag_of_tasks
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "4.73"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task8 (min=3, max=31) for baseline bag_of_tasks..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 120 --method POST --configmap wf-8-task8-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task8"

echo "Applying ConfigMap wf-8-task9-bag-of-tasks-cfg..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task9-bag-of-tasks-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task9
    baseline: bag_of_tasks
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: ""
  BASELINE: "bag_of_tasks"
  TASK_EXEC_TIME: "12.8128"
  SCALE_FACTOR: "1.25"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
  TOTAL_WORKFLOW_TASKS: "9"
  TOTAL_WORKFLOW_EXEC_TIME: "91.1131"
  CKPT_STRATEGY: "probabilistic"
  BOT_INITIAL_CREDITS: "100.0"
  BOT_CREDIT_EARN_RATE: "5.0"
  BOT_CREDIT_BURST_COST: "20.0"
  BOT_HIBERNATION_ENABLED: "true"
CMEOF

echo "Creating function task9 (min=1, max=11) for baseline bag_of_tasks..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 120 --method POST --configmap wf-8-task9-bag-of-tasks-cfg -n $NS
sleep 3
wait_rollout_for_task "task9"

echo "Recreating route wf-8-route ..."
fission route delete --name=wf-8-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-8-route --function task1 --url /wf-8 --method POST -n $NS
echo "✅ Deployed workflow wf-8 for baseline 'Bag of Tasks (Burstable + Hibernation)'"