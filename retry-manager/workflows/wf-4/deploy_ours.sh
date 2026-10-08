#!/bin/bash
set -euo pipefail
WF_ID="wf-4"
BASELINE="ours"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"
echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"

echo "Applying ConfigMap wf-4-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "3.2653"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=2 max=21)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 600 --method POST --configmap wf-4-task1-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "14.7935"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=6 max=61)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 600 --method POST --configmap wf-4-task2-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "ours"
  TASK_EXEC_TIME: "20.1005"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=6 max=61)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 600 --method POST --configmap wf-4-task3-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task4-ours-cfg for task4..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task4-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task4
    baseline: ours
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "ours"
  TASK_EXEC_TIME: "18.0605"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=6 max=61)..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 600 --method POST --configmap wf-4-task4-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task5-ours-cfg for task5..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task5-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task5
    baseline: ours
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "ours"
  TASK_EXEC_TIME: "16.8575"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task5 (min=6 max=61)..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 6 --maxscale 61 --fntimeout 600 --method POST --configmap wf-4-task5-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task6-ours-cfg for task6..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task6-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task6
    baseline: ours
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "ours"
  TASK_EXEC_TIME: "3.6797"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task6 (min=2 max=21)..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 2 --maxscale 21 --fntimeout 600 --method POST --configmap wf-4-task6-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task6 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task6 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task7-ours-cfg for task7..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task7-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task7
    baseline: ours
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "ours"
  TASK_EXEC_TIME: "2.2259"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task7 (min=1 max=11)..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-4-task7-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task7 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task7 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task8-ours-cfg for task8..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task8-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task8
    baseline: ours
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "ours"
  TASK_EXEC_TIME: "1.8326"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task8 (min=1 max=11)..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-4-task8-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task8 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task8 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-4-task9-ours-cfg for task9..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-4-task9-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-4
    task-id: task9
    baseline: ours
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-4"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "2.5258"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task9 (min=1 max=11)..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-4-task9-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task9 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task9 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Recreating route wf-4-route -> task1 ..."
fission route delete --name=wf-4-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-4-route --function task1 --url /wf-4 --method POST -n $NS
echo "✅ Deployed wf-4 for baseline ours"