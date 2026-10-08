#!/bin/bash
set -euo pipefail
WF_ID="wf-2"
BASELINE="ours"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"
echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"

echo "Applying ConfigMap wf-2-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "6.3986"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=1 max=11)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-task1-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.3510"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=1 max=11)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-task2-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3a,http://router.fission.svc.cluster.local/fission-function/task3b"
  BASELINE: "ours"
  TASK_EXEC_TIME: "7.3745"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=1 max=11)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-task3-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-task3a-ours-cfg for task3a..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3a-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3a
    baseline: ours
data:
  TASK_ID: "task3a"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr1"
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.0816"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3a (min=1 max=11)..."
fission fn delete --name task3a -n $NS >/dev/null 2>&1 || true
fission fn create --name task3a --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-task3a-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3a -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3a -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-task3b-ours-cfg for task3b..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-task3b-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: task3b
    baseline: ours
data:
  TASK_ID: "task3b"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/taskr2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.6644"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3b (min=1 max=11)..."
fission fn delete --name task3b -n $NS >/dev/null 2>&1 || true
fission fn create --name task3b --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-task3b-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3b -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3b -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-taskr1-ours-cfg for taskr1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-taskr1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: taskr1
    baseline: ours
data:
  TASK_ID: "taskr1"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.5805"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr1 (min=1 max=11)..."
fission fn delete --name taskr1 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-taskr1-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=taskr1 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=taskr1 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-2-taskr2-ours-cfg for taskr2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-2-taskr2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-2
    task-id: taskr2
    baseline: ours
data:
  TASK_ID: "taskr2"
  WORKFLOW_ID: "wf-2"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "2.0054"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function taskr2 (min=1 max=11)..."
fission fn delete --name taskr2 -n $NS >/dev/null 2>&1 || true
fission fn create --name taskr2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-2-taskr2-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=taskr2 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=taskr2 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Recreating route wf-2-route -> task1 ..."
fission route delete --name=wf-2-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-2-route --function task1 --url /wf-2 --method POST -n $NS
echo "✅ Deployed wf-2 for baseline ours"