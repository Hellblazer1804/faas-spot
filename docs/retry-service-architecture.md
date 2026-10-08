# Retry Service Architecture Update

## Overview
The retry service has been updated to run as a Kubernetes pod in the `retry` namespace instead of being started as a background process by the test runner. This change was made because Redis is only accessible via ClusterIP within the cluster.

## Architecture Changes

### Before (Background Process)
- Test runner started retry service as a background process
- Retry service ran outside the cluster
- Required external Redis access (not possible with ClusterIP)

### After (Kubernetes Pod)
- Retry service runs as a persistent pod in the `retry` namespace
- Test runner updates the ConfigMap to change which workflow the retry service handles
- Retry service can access Redis via ClusterIP from within the cluster

## Implementation Details

### Test Runner Changes
The test runner now:

1. **Updates ConfigMap**: Calls `update_retry_service_workflow(workflow_id, baseline)` to update the retry service ConfigMap
2. **Restarts Pod**: Automatically restarts the retry service pod to pick up the new configuration
3. **No Background Process**: No longer starts retry service as a background process

### Key Functions

#### `update_retry_service_workflow(workflow_id, baseline)`
- Updates the `retry-config` ConfigMap in the `retry` namespace
- Sets `WORKFLOW_FILTER` to the current workflow ID
- Sets `BASELINE` to the current baseline
- Restarts the retry service pod to pick up changes

#### `restart_retry_service_pod(retry_namespace)`
- Finds the retry service deployment in the `retry` namespace
- Triggers a rollout restart
- Waits for the rollout to complete

### Workflow Integration
The retry service is configured during the experiment setup phase:

```python
# 2.6) Update retry service to handle this workflow
print("🔧 Configuring retry service for this workflow...")
update_retry_service_workflow(wf, BASELINE)
```

## Prerequisites

### Retry Service Deployment
The retry service must be deployed first using one of the deployment files:

```bash
# Deploy with health checks
kubectl apply -f kube/deployment.yaml

# Or deploy without health checks
kubectl apply -f kube/deployment-no-health-checks.yaml
```

### Required Components
- **Namespace**: `retry` namespace must exist
- **ConfigMap**: `retry-config` ConfigMap must exist
- **Deployment**: `retry-service` deployment must be running
- **Redis**: Redis cluster must be accessible from within the cluster

## Benefits

1. **Redis Access**: Retry service can access Redis via ClusterIP
2. **Persistent Service**: Retry service runs continuously, no startup overhead
3. **Resource Efficiency**: Single retry service handles all workflows sequentially
4. **Simplified Management**: No need to manage background processes
5. **Better Monitoring**: Retry service runs as a proper Kubernetes workload

## Configuration

The retry service ConfigMap (`retry-config`) contains:

```yaml
data:
  WORKFLOW_FILTER: "wf-1"  # Updated by test runner
  BASELINE: "ours"         # Updated by test runner
  POLL_INTERVAL: "20"
  COOLDOWN_SEC: "60"
  MAX_RETRIES_PER_UID: "3"
  FISSION_ROUTER_PREFIX: "http://172.22.162.172:32405/fission-function/"
  REDIS_HOST: "redis-redis-cluster.redis.svc.cluster.local"
  REDIS_PORT: "6379"
  REDIS_USER: "default"
  WORKFLOW_DIR: "/app/workflows"
```

## Troubleshooting

### Check Retry Service Status
```bash
kubectl get pods -n retry
kubectl logs -n retry deployment/retry-service
```

### Check ConfigMap
```bash
kubectl get configmap -n retry retry-config -o yaml
```

### Manual ConfigMap Update
```bash
kubectl patch configmap -n retry retry-config --patch '{"data":{"WORKFLOW_FILTER":"wf-5","BASELINE":"ours"}}'
kubectl rollout restart -n retry deployment/retry-service
```

## Migration Notes

- **No Breaking Changes**: Existing retry service deployments continue to work
- **Test Runner Update**: Test runner now uses the new ConfigMap-based approach
- **Backward Compatibility**: Old background process approach can be restored if needed


