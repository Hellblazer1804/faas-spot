# Retry Service Deployment Options

This directory contains multiple deployment configurations for the retry service to handle different health check requirements.

## Problem
The original deployment was failing liveness probes because it used the `ps` command which is not available in the Python slim container image.

## Solutions

### Option 1: Enhanced Health Checks (Recommended)
**File**: `deployment.yaml`

- Uses a dedicated `health_check.py` script
- Performs comprehensive checks:
  - Python functionality
  - Required directories exist
  - Required modules can be imported
  - Environment variables are set
- More robust and informative than simple checks

### Option 2: Simple Health Checks
**File**: `deployment.yaml` (commented alternative)

- Uses simple Python commands
- Liveness: `python3 -c "import sys; sys.exit(0)"`
- Readiness: `python3 -c "import os; exit(0 if os.path.exists('/app/workflows') else 1)"`
- Faster but less comprehensive

### Option 3: No Health Checks
**File**: `deployment-no-health-checks.yaml`

- Completely removes liveness and readiness probes
- Use this if you want to avoid any health check issues
- Pod will not be automatically restarted on failures
- Simpler deployment but less resilient

## Usage

### Deploy with Enhanced Health Checks
```bash
kubectl apply -f kube/deployment.yaml
```

### Deploy without Health Checks
```bash
kubectl apply -f kube/deployment-no-health-checks.yaml
```

### Build and Push Docker Image
```bash
# Build the image with health check script
docker build -t your-registry/retry-service:latest .

# Push to registry
docker push your-registry/retry-service:latest

# Update deployment.yaml with your image
```

## Health Check Script

The `health_check.py` script performs the following checks:

1. **Python Check**: Verifies Python is working
2. **Directory Check**: Ensures `/app/workflows` exists (required) and `/secrets` exists (optional)
3. **Module Check**: Verifies all required Python modules can be imported
4. **Environment Check**: Confirms required environment variables are set

**Note**: The `/secrets` directory is optional and will only be present if Redis secrets are mounted as volumes. The health check will warn if it's missing but won't fail the check.

## Troubleshooting

### If health checks still fail:
1. Check the pod logs: `kubectl logs -n retry deployment/retry-service`
2. Test the health check manually: `kubectl exec -n retry deployment/retry-service -- python3 /app/health_check.py`
3. Use the no-health-checks deployment as a fallback

### If you prefer no health checks:
- Use `deployment-no-health-checks.yaml`
- Monitor the service manually through logs
- Consider implementing application-level health endpoints instead

## Configuration

The deployment uses:
- **ConfigMap**: `retry-config` for runtime configuration
- **Secrets**: `mysql-auth` and `redis-auth` for credentials
- **Namespace**: `retry` (can be changed to `default` if needed)

Update the ConfigMap values as needed for your environment.
