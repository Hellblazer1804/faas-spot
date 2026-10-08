#!/usr/bin/env python3
"""
Enhanced retry service with HTTP health endpoints for Kubernetes probes.
This adds HTTP endpoints that can be used for liveness and readiness probes.
"""

import time
import json
import os
import argparse
from typing import Tuple, Any, Dict, Set, List
from collections import deque, defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

import mysql.connector
import requests
import redis

# -------------------------
# Configuration (env defaults)
# -------------------------
DB_CONFIG = {
    'user': os.getenv('MYSQL_USER', 'remote'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'database': os.getenv('MYSQL_DB', 'wms'),
}
WORKFLOW_DIR = os.getenv('WORKFLOW_DIR', 'workflows')
FISSION_ROUTER_PREFIX = os.getenv(
    'FISSION_ROUTER_PREFIX',
    f"{os.getenv('ROUTER_BASE_URL', 'http://localhost:32363')}/fission-function/"
)

# Default timings; CLI can override
POLL_INTERVAL       = 20            # seconds
MAX_RETRIES_PER_UID = 3             # per UID, per (workflow, baseline)
COOLDOWN_SEC        = 60            # seconds between attempts for same UID

# Redis secret mount location
SECRETS_ROOT = "/secrets"

# Health check server
HEALTH_PORT = int(os.getenv('HEALTH_PORT', '8080'))

session = requests.Session()

# -------------------------
# HTTP Health Server
# -------------------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.handle_liveness()
        elif self.path == '/ready':
            self.handle_readiness()
        else:
            self.send_response(404)
            self.end_headers()
    
    def handle_liveness(self):
        """Liveness probe - is the service running?"""
        try:
            # Basic check - is Python working?
            import sys
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"status": "alive", "python_version": sys.version}
            self.wfile.write(json.dumps(response).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"status": "unhealthy", "error": str(e)}
            self.wfile.write(json.dumps(response).encode())
    
    def handle_readiness(self):
        """Readiness probe - is the service ready to handle requests?"""
        try:
            # Check if all required components are available
            checks = {
                "python": self.check_python(),
                "directories": self.check_directories(),
                "modules": self.check_modules(),
                "environment": self.check_environment(),
                "mysql": self.check_mysql(),
                "redis": self.check_redis()
            }
            
            all_ready = all(checks.values())
            status_code = 200 if all_ready else 503
            
            self.send_response(status_code)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"status": "ready" if all_ready else "not_ready", "checks": checks}
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_response(503)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"status": "not_ready", "error": str(e)}
            self.wfile.write(json.dumps(response).encode())
    
    def check_python(self):
        try:
            import sys
            return True
        except:
            return False
    
    def check_directories(self):
        try:
            return os.path.exists('/app/workflows')
        except:
            return False
    
    def check_modules(self):
        try:
            import mysql.connector, redis, requests, json, time, os
            return True
        except:
            return False
    
    def check_environment(self):
        try:
            required_vars = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DB', 'BASELINE']
            return all(os.getenv(var) for var in required_vars)
        except:
            return False
    
    def check_mysql(self):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            conn.close()
            return True
        except:
            return False
    
    def check_redis(self):
        try:
            pw = read_secret("redis/redis-redis-cluster/redis-password")
            host = os.getenv("REDIS_HOST", "redis-redis-cluster")
            port = int(os.getenv("REDIS_PORT", "6379"))
            user = os.getenv("REDIS_USER", "default")
            
            r = redis.Redis(host=host, port=port, username=user, password=pw, decode_responses=True)
            r.ping()
            return True
        except:
            return False

def start_health_server():
    """Start HTTP health check server in background thread."""
    server = HTTPServer(('0.0.0.0', HEALTH_PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"🏥 Health server started on port {HEALTH_PORT}")
    print(f"   -> Liveness: http://localhost:{HEALTH_PORT}/health")
    print(f"   -> Readiness: http://localhost:{HEALTH_PORT}/ready")

# -------------------------
# DB Helpers (same as original)
# -------------------------
def mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

def read_secret(path_rel: str) -> str:
    p = os.path.join(SECRETS_ROOT, path_rel)
    try:
        with open(p, "r") as f:
            return f.read().strip()
    except Exception:
        return ""

def redis_client():
    pw = read_secret("redis/redis-redis-cluster/redis-password")
    host = os.getenv("REDIS_HOST", "redis-redis-cluster")
    port = int(os.getenv("REDIS_PORT", "6379"))
    user = os.getenv("REDIS_USER", "default")
    if not pw:
        raise RuntimeError("Redis password not found")
    return redis.Redis(
        host=host, port=port, username=user, password=pw,
        decode_responses=True,
        socket_connect_timeout=2, socket_timeout=2, retry_on_timeout=True
    )

# -------------------------
# Main retry service logic (simplified for example)
# -------------------------
def main():
    global COOLDOWN_SEC, POLL_INTERVAL

    parser = argparse.ArgumentParser(description="Retry service with HTTP health endpoints")
    parser.add_argument("--baseline", required=True, help="Baseline name")
    parser.add_argument("--workflow", help="Workflow filter")
    parser.add_argument("--health-port", type=int, default=HEALTH_PORT, help="Health server port")
    args = parser.parse_args()

    COOLDOWN_SEC = int(os.getenv('COOLDOWN_SEC', '60'))
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '20'))

    # Start health server
    start_health_server()

    print(f"🚀 Retry service starting with health endpoints...")
    print(f"   -> Baseline: {args.baseline}")
    print(f"   -> Workflow: {args.workflow or 'all'}")
    print(f"   -> Health port: {args.health_port}")

    # Main retry loop (simplified)
    while True:
        print("🔄 Retry service tick...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
