# Example Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install deps (speed up with a requirements-only layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY retry_service.py ./
# Copy health check script
COPY health_check.py ./
# If you have other modules, copy them too
# COPY common_lib/ ./common_lib/

# 👇 Copy workflows into the image
COPY workflows/ ./workflows/

# (Optional but nice) prove directory exists even if empty
RUN mkdir -p /app/workflows

ENV WORKFLOW_DIR=/app/workflows

ENTRYPOINT ["python3", "retry_service.py"]
CMD ["--baseline=ours"]