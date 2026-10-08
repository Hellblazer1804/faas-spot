#!/bin/bash
WF_ID="wf-7"
BASELINE="spotserve"
BASELINE_DISPLAY="SpotServe (OFP-TM)"
ENV_NAME="serverless"
TASK_TEMPLATE="task_template.py"

fission fn create --name task1 \
  --env $ENV_NAME \
  --code $TASK_TEMPLATE \
  --executortype newdeploy \
  --minscale 6 \
  --fntimeout 120 \
  --maxscale 60 \
  --method POST
sleep 4
DEPLOY_NAME=$(kubectl get deployments -n default -l functionName=task1 -o jsonpath="{.items[0].metadata.name}")
sleep 2
kubectl patch deployment $DEPLOY_NAME -n default --type=json -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env\", \"value\": [{\"name\": \"TASK_ID\", \"value\": \"task1\"},{\"name\": \"WORKFLOW_ID\", \"value\": \"$WF_ID\"},{\"name\": \"NEXT_TASK_URL\", \"value\": \"http://router.fission.svc.cluster.local/fission-function/task2\"},{\"name\": \"IS_CHECKPOINT\", \"value\": \"false\"},{\"name\": \"IS_GENERATOR\", \"value\": \"true\"},{\"name\": \"BASELINE\", \"value\": \"$BASELINE\"},{\"name\": \"HEFT_RANK\", \"value\": \"4\"},{\"name\": \"SPOTSERVE\" , \"value\": \"true\"},{\"name\": \"AUTO_CHECKPOINT\" , \"value\": \"false\"}] }]"

fission fn create --name task2 \
  --env $ENV_NAME \
  --code $TASK_TEMPLATE \
  --executortype newdeploy \
  --minscale 6 \
  --fntimeout 120 \
  --maxscale 60 \
  --method POST
sleep 4
DEPLOY_NAME=$(kubectl get deployments -n default -l functionName=task2 -o jsonpath="{.items[0].metadata.name}")
sleep 2
kubectl patch deployment $DEPLOY_NAME -n default --type=json -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env\", \"value\": [{\"name\": \"TASK_ID\", \"value\": \"task2\"},{\"name\": \"WORKFLOW_ID\", \"value\": \"$WF_ID\"},{\"name\": \"NEXT_TASK_URL\", \"value\": \"http://router.fission.svc.cluster.local/fission-function/task3\"},{\"name\": \"IS_CHECKPOINT\", \"value\": \"false\"},{\"name\": \"IS_GENERATOR\", \"value\": \"false\"},{\"name\": \"BASELINE\", \"value\": \"$BASELINE\"},{\"name\": \"HEFT_RANK\", \"value\": \"3\"},{\"name\": \"SPOTSERVE\" , \"value\": \"true\"},{\"name\": \"AUTO_CHECKPOINT\" , \"value\": \"false\"}] }]"

fission fn create --name task3 \
  --env $ENV_NAME \
  --code $TASK_TEMPLATE \
  --executortype newdeploy \
  --minscale 2 \
  --fntimeout 120 \
  --maxscale 20 \
  --method POST
sleep 4
DEPLOY_NAME=$(kubectl get deployments -n default -l functionName=task3 -o jsonpath="{.items[0].metadata.name}")
sleep 2
kubectl patch deployment $DEPLOY_NAME -n default --type=json -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env\", \"value\": [{\"name\": \"TASK_ID\", \"value\": \"task3\"},{\"name\": \"WORKFLOW_ID\", \"value\": \"$WF_ID\"},{\"name\": \"NEXT_TASK_URL\", \"value\": \"http://router.fission.svc.cluster.local/fission-function/task4\"},{\"name\": \"IS_CHECKPOINT\", \"value\": \"false\"},{\"name\": \"IS_GENERATOR\", \"value\": \"false\"},{\"name\": \"BASELINE\", \"value\": \"$BASELINE\"},{\"name\": \"HEFT_RANK\", \"value\": \"2\"},{\"name\": \"SPOTSERVE\" , \"value\": \"true\"},{\"name\": \"AUTO_CHECKPOINT\" , \"value\": \"false\"}] }]"

fission fn create --name task4 \
  --env $ENV_NAME \
  --code $TASK_TEMPLATE \
  --executortype newdeploy \
  --minscale 1 \
  --fntimeout 120 \
  --maxscale 10 \
  --method POST
sleep 4
DEPLOY_NAME=$(kubectl get deployments -n default -l functionName=task4 -o jsonpath="{.items[0].metadata.name}")
sleep 2
kubectl patch deployment $DEPLOY_NAME -n default --type=json -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env\", \"value\": [{\"name\": \"TASK_ID\", \"value\": \"task4\"},{\"name\": \"WORKFLOW_ID\", \"value\": \"$WF_ID\"},{\"name\": \"NEXT_TASK_URL\", \"value\": \"\"},{\"name\": \"IS_CHECKPOINT\", \"value\": \"false\"},{\"name\": \"IS_GENERATOR\", \"value\": \"false\"},{\"name\": \"BASELINE\", \"value\": \"$BASELINE\"},{\"name\": \"HEFT_RANK\", \"value\": \"1\"},{\"name\": \"SPOTSERVE\" , \"value\": \"true\"},{\"name\": \"AUTO_CHECKPOINT\" , \"value\": \"false\"}] }]"

fission route create --name=wf-7-route --function task1 --url /wf-7 --method POST
echo "✅ Workflow $WF_ID deployed for baseline $BASELINE_DISPLAY ($BASELINE)"