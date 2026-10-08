#!/bin/bash
set -euo pipefail
WF_ID="wf-8"
BASELINE="ours"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"
NS="default"
echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"

echo "Applying ConfigMap wf-8-task1-ours-cfg for task1..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task1-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task1
    baseline: ours
data:
  TASK_ID: "task1"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task2"
  BASELINE: "ours"
  TASK_EXEC_TIME: "9.7025"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task1 (min=9 max=91)..."
fission fn delete --name task1 -n $NS >/dev/null 2>&1 || true
fission fn create --name task1 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 600 --method POST --configmap wf-8-task1-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task1 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task2-ours-cfg for task2..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task2-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task2
    baseline: ours
data:
  TASK_ID: "task2"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task3"
  BASELINE: "ours"
  TASK_EXEC_TIME: "8.6177"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task2 (min=12 max=121)..."
fission fn delete --name task2 -n $NS >/dev/null 2>&1 || true
fission fn create --name task2 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 12 --maxscale 121 --fntimeout 600 --method POST --configmap wf-8-task2-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task2 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task3-ours-cfg for task3..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task3-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task3
    baseline: ours
data:
  TASK_ID: "task3"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task4"
  BASELINE: "ours"
  TASK_EXEC_TIME: "16.7053"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task3 (min=3 max=31)..."
fission fn delete --name task3 -n $NS >/dev/null 2>&1 || true
fission fn create --name task3 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 600 --method POST --configmap wf-8-task3-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task3 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task4-ours-cfg for task4..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task4-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task4
    baseline: ours
data:
  TASK_ID: "task4"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task5"
  BASELINE: "ours"
  TASK_EXEC_TIME: "6.1645"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task4 (min=3 max=31)..."
fission fn delete --name task4 -n $NS >/dev/null 2>&1 || true
fission fn create --name task4 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 600 --method POST --configmap wf-8-task4-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task4 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task5-ours-cfg for task5..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task5-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task5
    baseline: ours
data:
  TASK_ID: "task5"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task6"
  BASELINE: "ours"
  TASK_EXEC_TIME: "19.4763"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task5 (min=9 max=91)..."
fission fn delete --name task5 -n $NS >/dev/null 2>&1 || true
fission fn create --name task5 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 9 --maxscale 91 --fntimeout 600 --method POST --configmap wf-8-task5-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task5 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task6-ours-cfg for task6..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task6-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task6
    baseline: ours
data:
  TASK_ID: "task6"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task7"
  BASELINE: "ours"
  TASK_EXEC_TIME: "15.3432"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task6 (min=3 max=31)..."
fission fn delete --name task6 -n $NS >/dev/null 2>&1 || true
fission fn create --name task6 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 600 --method POST --configmap wf-8-task6-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task6 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task6 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task7-ours-cfg for task7..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task7-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task7
    baseline: ours
data:
  TASK_ID: "task7"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task8"
  BASELINE: "ours"
  TASK_EXEC_TIME: "4.7606"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task7 (min=3 max=31)..."
fission fn delete --name task7 -n $NS >/dev/null 2>&1 || true
fission fn create --name task7 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 600 --method POST --configmap wf-8-task7-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task7 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task7 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task8-ours-cfg for task8..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task8-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task8
    baseline: ours
data:
  TASK_ID: "task8"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: "http://router.fission.svc.cluster.local/fission-function/task9"
  BASELINE: "ours"
  TASK_EXEC_TIME: "7.2367"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task8 (min=3 max=31)..."
fission fn delete --name task8 -n $NS >/dev/null 2>&1 || true
fission fn create --name task8 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 3 --maxscale 31 --fntimeout 600 --method POST --configmap wf-8-task8-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task8 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task8 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Applying ConfigMap wf-8-task9-ours-cfg for task9..."
kubectl -n $NS apply -f - <<CMEOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: wf-8-task9-ours-cfg
  labels:
    app: serverless-wf
    wf-id: wf-8
    task-id: task9
    baseline: ours
data:
  TASK_ID: "task9"
  WORKFLOW_ID: "wf-8"
  NEXT_TASK_URL: ""
  BASELINE: "ours"
  TASK_EXEC_TIME: "7.9490"
  IS_CHECKPOINT: "false"
  CKPT_LEVEL: "1"
  SCALE_FACTOR: "1.0"
  AZ: "us-west-2a"
  INSTANCE_TYPE: "v100"
CMEOF

echo "Creating function task9 (min=1 max=11)..."
fission fn delete --name task9 -n $NS >/dev/null 2>&1 || true
fission fn create --name task9 --env $ENV_NAME --code $TASK_TEMPLATE --executortype newdeploy --minscale 1 --maxscale 11 --fntimeout 600 --method POST --configmap wf-8-task9-ours-cfg -n $NS
sleep 3
DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task9 -o jsonpath="{.items[0].metadata.name}")
for i in {1..30}; do [[ -n "$DEPLOY_NAME" ]] && break; sleep 2; DEPLOY_NAME=$(kubectl -n $NS get deployments -l functionName=task9 -o jsonpath="{.items[0].metadata.name}"); done
kubectl -n $NS rollout status deploy/$DEPLOY_NAME --timeout=180s

echo "Recreating route wf-8-route -> task1 ..."
fission route delete --name=wf-8-route -n $NS >/dev/null 2>&1 || true
fission route create --name=wf-8-route --function task1 --url /wf-8 --method POST -n $NS
echo "✅ Deployed wf-8 for baseline ours"