#!/usr/bin/env python3
import os
import time
import json
import uuid
import random
import glob
import requests
import mysql.connector
from flask import Flask, request

app = Flask(__name__)

# ---------- Fission mounts ----------
CONFIGS_ROOT = "/configs"
SECRETS_ROOT = "/secrets"

def _read_first(path_glob: str):
    for p in glob.glob(path_glob):
        try:
            with open(p, "r") as f:
                val = f.read().strip()
                if val:
                    return val
        except Exception:
            pass
    return None

def read_cfg(key: str, default: str = "") -> str:
    file_val = _read_first(os.path.join(CONFIGS_ROOT, "*", "*", key))
    if file_val is not None:
        return file_val
    return os.environ.get(key, default)

def read_secret(path_rel: str) -> str:
    """Read a single secret file (mounted)."""
    return _read_first(os.path.join(SECRETS_ROOT, path_rel)) or ""

# ---------- DB helpers ----------
def mysql_connection():
    return mysql.connector.connect(
        user     = os.getenv("MYSQL_USER", "remote"),
        password = os.getenv("MYSQL_PASSWORD", ""),
        host     = os.getenv("MYSQL_HOST", "localhost"),
        database = os.getenv("MYSQL_DB",   "wms"),
    )

def log_execution(workflow, task_id, uid, start_time, end_time, status_code, baseline):
    """Log execution to MySQL."""
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO serverless_workflows
              (workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code, baseline)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (workflow, task_id, start_time, end_time, uid, status_code, baseline)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Execution logging to MySQL failed: {e}")

# ---------- Task handler ----------
@app.route("/", methods=["POST"])
def main():
    task_id        = read_cfg("TASK_ID", "taskX")
    workflow       = read_cfg("WORKFLOW_ID", "unknown")
    baseline       = read_cfg("BASELINE", "ours")
    next_task_urls = read_cfg("NEXT_TASK_URL", "")  # comma-separated URLs

    TASK_EXEC_SEC  = float(read_cfg("TASK_EXEC_TIME", "0.25"))
    SCALING_FACTOR = float(read_cfg("SCALING_FACTOR", "1.0"))

    IS_CHECKPOINT  = read_cfg("IS_CHECKPOINT", "false").lower() == "true"
    CKPT_LEVEL     = int(read_cfg("CKPT_LEVEL", "0"))

    AZ            = read_cfg("AZ", "")
    INSTANCE_TYPE = read_cfg("INSTANCE_TYPE", "")

    payload = request.get_json(force=True, silent=True) or {}
    uid = payload.get("uid", str(uuid.uuid4()))
    resume_state = payload.get("checkpoint")

    print(f"🔁 [{baseline.upper()}] Executing {task_id} | UID={uid} | Scale={SCALING_FACTOR} | CP={IS_CHECKPOINT} L{CKPT_LEVEL}")
    start_time = int(time.time())

    if resume_state is not None:
        print(f"↩️  Resuming from checkpoint for {uid}: {resume_state}")

    # Simulated work
    scaled = TASK_EXEC_SEC / max(SCALING_FACTOR, 0.0001)
    final_duration = random.uniform(scaled * 0.9, scaled * 1.1)
    print(f"  -> Planned duration: base={TASK_EXEC_SEC:.3f}s, final={final_duration:.3f}s")
    time.sleep(final_duration)

    # Prepare checkpoint payload (if any)
    ckpt_payload = None
    if IS_CHECKPOINT:
        ckpt_payload = {
            "task_id": task_id,
            "progress": 1.0,
            "duration_s": final_duration,
            "az": AZ,
            "instance": INSTANCE_TYPE,
        }

    end_time = int(time.time())
    status = 200

    # Checkpoint before forward (recovery state must be saved before chain proceeds)
    if IS_CHECKPOINT:
        try:
            conn = mysql_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO task_checkpoints
                  (uid, task_id, result, checkpoint_level, timestamp, workflow_name, baseline)
                VALUES (%s, %s, %s, %s, NOW(), %s, %s)
                """,
                (uid, task_id, json.dumps(ckpt_payload or {}), int(CKPT_LEVEL), workflow, baseline)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️ Checkpoint write to MySQL failed: {e}")

    # Forward, then log (so chain is not blocked by DB)
    if next_task_urls:
        try:
            candidates = [u.strip() for u in next_task_urls.split(",") if u.strip()]
            if candidates:
                primary = random.choice(candidates)
                fwd_payload = {"uid": uid, "workflow_name": workflow, "baseline": baseline}
                requests.post(primary, json=fwd_payload, timeout=20)
                print(f"➡️  Forwarded to {primary}")
        except Exception as e:
            print(f"❌ Forwarding failed: {e}")
            status = 500

    log_execution(workflow, task_id, uid, start_time, end_time, status, baseline)

    return f"{task_id} completed.", status

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=False)