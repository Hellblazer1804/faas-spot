# Kubernetes Health Probe Options for Retry Service

## Current Configuration (Recommended)
The deployment already includes both liveness and readiness probes using the custom health check script.

## Probe Types Available

### 1. Exec Probe (Current - Recommended)
```yaml
livenessProbe:
  exec:
    command: ["python3", "/app/health_check.py"]
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 3

readinessProbe:
  exec:
    command: ["python3", "/app/health_check.py"]
  initialDelaySeconds: 15
  periodSeconds: 15
  timeoutSeconds: 10
  failureThreshold: 3
```

### 2. Simple Exec Probe (Alternative)
```yaml
livenessProbe:
  exec:
    command: ["python3", "-c", "import sys; sys.exit(0)"]
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  exec:
    command: ["python3", "-c", "import os; exit(0 if os.path.exists('/app/workflows') else 1)"]
  initialDelaySeconds: 15
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
```

### 3. HTTP Probe (If you add a health endpoint)
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
```

### 4. TCP Probe (If you expose a port)
```yaml
livenessProbe:
  tcpSocket:
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  tcpSocket:
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
```

### 5. No Probes (Fallback)
```yaml
# Comment out or remove both probes entirely
# livenessProbe: ...
# readinessProbe: ...
```

## Probe Parameters Explained

- **initialDelaySeconds**: Wait time before first probe (startup grace period)
- **periodSeconds**: How often to run the probe
- **timeoutSeconds**: How long to wait for probe response
- **failureThreshold**: How many failures before considering unhealthy
- **successThreshold**: How many successes needed to consider healthy (default: 1)

## Current Deployment Options

### Option 1: Enhanced Health Checks (Active)
File: `deployment.yaml`
- Uses comprehensive health check script
- Checks Python, directories, modules, environment
- Most robust option

### Option 2: Simple Health Checks (Commented Alternative)
File: `deployment.yaml` (commented section)
- Uses simple Python commands
- Faster but less comprehensive

### Option 3: No Health Checks
File: `deployment-no-health-checks.yaml`
- Completely removes probes
- Use if you want to avoid any probe issues

## How to Modify Probes

### To use simple probes instead:
1. Comment out the current probes in `deployment.yaml`
2. Uncomment the simple probe section
3. Apply: `kubectl apply -f kube/deployment.yaml`

### To remove probes entirely:
1. Use: `kubectl apply -f kube/deployment-no-health-checks.yaml`

### To add HTTP probes:
1. Add a health endpoint to your retry service
2. Expose a port in the deployment
3. Replace exec probes with httpGet probes

## Troubleshooting Probes

### Check probe status:
```bash
kubectl describe pod -n retry deployment/retry-service
```

### Test probe manually:
```bash
kubectl exec -n retry deployment/retry-service -- python3 /app/health_check.py
```

### View probe logs:
```bash
kubectl logs -n retry deployment/retry-service
```
