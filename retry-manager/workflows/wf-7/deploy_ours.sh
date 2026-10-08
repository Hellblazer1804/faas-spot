#!/bin/bash
set -euo pipefail
WF_ID="wf-7"
BASELINE="ours"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"
echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"

echo "Applying ConfigMap wf-7-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.3681"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=8 max=81)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 8 --maxscale 81 --fntimeout 600 --method POST --configmap wf-7-task1-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-7-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "8.9624"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=16 max=161)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 16 --maxscale 161 --fntimeout 600 --method POST --configmap wf-7-task2-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-7-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "ours"
  TASK_EXEC_TIME: "17.9638"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=8 max=81)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 8 --maxscale 81 --fntimeout 600 --method POST --configmap wf-7-task3-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-7-task4-ours-cfg for task4..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task4-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task4
    baseline: ours
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "ours"
  TASK_EXEC_TIME: "15.6284"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=8 max=81)..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 8 --maxscale 81 --fntimeout 600 --method POST --configmap wf-7-task4-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-7-task5-ours-cfg for task5..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-7-task5-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-7
    task-id: task5
    baseline: ours
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-7"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.8251"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task5 (min=2 max=21)..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 600 --method POST --configmap wf-7-task5-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Recreating route wf-7-route -> task1 ..."
fission route delete --name=wf-7-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-7-route --function task1 --url /wf-7 --method POST -n $NS
echo "✅ Deployed wf-7 for baseline ours"