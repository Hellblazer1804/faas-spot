#!/usr/bin/env python3
import time
import json
import os
import argparse
from typing import Tuple, Any, Dict, Set, List

import mysql.connector
import requests

# -------------------------
# Configuration
# -------------------------
DB_CONFIG = {
    'user': os.getenv('MYSQL_USER', 'remote'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'database': os.getenv('MYSQL_DB', 'wms'),
}
WORKFLOW_DIR = os.getenv('WORKFLOW_DIR', 'workflows')
FISSION_ROUTER_PREFIX = os.getenv('FISSION_ROUTER_PREFIX',
                                  'http://localhost:32363/fission-function/')

# Default timings; can be overridden by CLI
POLL_INTERVAL = 20            # seconds
MAX_RETRIES_PER_UID = 3       # attempts per UID for a given baseline+workflow
COOLDOWN_SEC = 60             # min seconds between attempts per UID

session = requests.Session()


# -------------------------
# DB Helpers
# -------------------------
def mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)


def setup_tables(clear=False):
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint_retries (
                uid VARCHAR(256),
                workflow_name VARCHAR(256),
                baseline VARCHAR(128),
                retry_count INT DEFAULT 0,
                last_task_id VARCHAR(256) NULL,
                last_status INT NULL,
                last_attempt_at DATETIME NULL,
                PRIMARY KEY(uid, workflow_name, baseline)
            )
        """)
        if clear:
            cursor.execute("TRUNCATE TABLE checkpoint_retries")

        # Optional helper indexes
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_swv_wb ON serverless_workflows(workflow_name, baseline)",
            "CREATE INDEX IF NOT EXISTS idx_swv_uid ON serverless_workflows(uuid_passed)",
            "CREATE INDEX IF NOT EXISTS idx_tc_uwb ON task_checkpoints(uid, workflow_name, baseline)",
        ]:
            try:
                cursor.execute(stmt)
            except Exception:
                pass
        conn.commit()


# -------------------------
# DAG Utilities
# -------------------------
def load_workflow_tasks(workflow_name) -> Dict[str, Dict[str, Any]]:
    tasks_path = os.path.join(WORKFLOW_DIR, workflow_name, "tasks.json")
    with open(tasks_path) as f:
        return json.load(f)["tasks"]


def build_predecessor_map(tasks_data: Dict[str, Dict[str, Any]]) -> Dict[str, Set[str]]:
    preds: Dict[str, Set[str]] = {tid: set() for tid in tasks_data}
    for tid, meta in tasks_data.items():
        for succ in meta.get("successors", []):
            preds.setdefault(succ, set()).add(tid)
    return preds


def find_source_tasks(tasks_data: Dict[str, Dict[str, Any]]) -> List[str]:
    preds = build_predecessor_map(tasks_data)
    return sorted([tid for tid, ps in preds.items() if len(ps) == 0])


def find_final_tasks(tasks_data: Dict[str, Dict[str, Any]]) -> List[str]:
    """True finals = tasks with NO successors (sinks)."""
    return sorted([tid for tid, meta in tasks_data.items() if not meta.get("successors")])


def determine_next_tasks(tasks_data: Dict[str, Dict[str, Any]], completed: Set[str]) -> List[str]:
    """Return tasks eligible to run next (all predecessors completed)."""
    completed = completed or set()
    preds = build_predecessor_map(tasks_data)
    if not completed:
        return find_source_tasks(tasks_data)

    candidates = []
    for tid in sorted(tasks_data.keys()):
        if tid in completed:
            continue
        if preds.get(tid, set()).issubset(completed):
            candidates.append(tid)
    return candidates


def verify_dag_once(tasks_data: Dict[str, Dict[str, Any]], workflow: str):
    """One-time visibility / sanity check for each DAG."""
    src = find_source_tasks(tasks_data)
    fin = find_final_tasks(tasks_data)
    if not src:
        print(f"⚠️ [{workflow}] No sources detected. Check DAG.")
    if not fin:
        print(f"⚠️ [{workflow}] No finals detected. Check DAG.")
    print(f"   [{workflow}] Sources={src} | Finals={fin}")


# -------------------------
# Query Helpers
# -------------------------
def get_uids_incomplete_workflows(workflow: str, final_tasks: List[str], baseline: str) -> List[str]:
    """Return UIDs that started under (workflow, baseline) but have NOT completed any final task."""
    with mysql_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT DISTINCT uuid_passed FROM serverless_workflows WHERE workflow_name=%s AND baseline=%s",
            (workflow, baseline)
        )
        all_uids = {uid for (uid,) in cursor.fetchall() if uid}

        succeeded = set()
        if final_tasks:
            placeholders = ','.join(['%s'] * len(final_tasks))
            params = [workflow, baseline] + final_tasks
            cursor.execute(
                f"""SELECT DISTINCT uuid_passed
                    FROM serverless_workflows
                    WHERE workflow_name=%s AND baseline=%s
                      AND workflow_stage IN ({placeholders})
                      AND response_code=200""",
                params
            )
            succeeded = {uid for (uid,) in cursor.fetchall() if uid}

        cursor.execute(
            "SELECT uid FROM checkpoint_retries WHERE workflow_name=%s AND baseline=%s AND retry_count >= %s",
            (workflow, baseline, MAX_RETRIES_PER_UID)
        )
        over_retried = {uid for (uid,) in cursor.fetchall() if uid}

        cursor.execute(
            """
            SELECT uid FROM checkpoint_retries
            WHERE workflow_name=%s AND baseline=%s
              AND last_attempt_at IS NOT NULL
              AND last_attempt_at > (NOW() - INTERVAL %s SECOND)
            """,
            (workflow, baseline, COOLDOWN_SEC)
        )
        cooling = {uid for (uid,) in cursor.fetchall() if uid}

    candidates = [uid for uid in all_uids if uid not in succeeded and uid not in over_retried and uid not in cooling]
    return sorted(candidates)


def get_completed_tasks(uid: str, workflow_name: str, baseline: str) -> Set[str]:
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT workflow_stage FROM serverless_workflows
            WHERE uuid_passed=%s AND workflow_name=%s AND baseline=%s AND response_code=200
            """,
            (uid, workflow_name, baseline)
        )
        return {task for (task,) in cursor.fetchall()}


def get_latest_checkpoint(uid: str, workflow_name: str, baseline: str) -> Tuple[Any, Any]:
    """
    Returns (task_id, checkpoint_data or None).
    Decodes JSON if needed; returns (None, None) if no row.
    """
    with mysql_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT task_id, result FROM task_checkpoints
                WHERE uid=%s AND workflow_name=%s AND baseline=%s
                ORDER BY timestamp DESC LIMIT 1
                """,
                (uid, workflow_name, baseline)
            )
        except mysql.connector.Error:
            # Fallback if older schema (no wf/baseline columns)
            cursor.execute(
                "SELECT task_id, result FROM task_checkpoints WHERE uid=%s ORDER BY timestamp DESC LIMIT 1",
                (uid,)
            )
        row = cursor.fetchone()

    if not row:
        return None, None

    task_id_val, result_val = row[0], row[1]
    if isinstance(result_val, (bytes, bytearray)):
        try:
            result_val = result_val.decode("utf-8")
        except Exception:
            pass
    if isinstance(result_val, str):
        try:
            result_val = json.loads(result_val)
        except Exception:
            # Keep raw string if not JSON
            pass
    return task_id_val, result_val


def get_hibernation_state(uid: str, workflow_name: str, baseline: str) -> Tuple[Any, Any]:
    """
    Get hibernated state for Bag-of-Tasks baseline (IEEE TCC 2023).
    Returns (task_id, state_data or None).
    Hibernation preserves full execution state including CPU credits.
    """
    with mysql_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT task_id, state_data, cpu_credits FROM bot_hibernation_state
                WHERE uuid=%s AND workflow_name=%s AND baseline=%s AND resumed_at IS NULL
                ORDER BY hibernated_at DESC LIMIT 1
                """,
                (uid, workflow_name, baseline)
            )
            row = cursor.fetchone()
        except mysql.connector.Error:
            # Table might not exist yet
            return None, None

    if not row:
        return None, None

    task_id_val, state_data, cpu_credits = row[0], row[1], row[2] if len(row) > 2 else None
    
    # Decode state_data
    if isinstance(state_data, (bytes, bytearray)):
        try:
            state_data = state_data.decode("utf-8")
        except Exception:
            pass
    if isinstance(state_data, str):
        try:
            state_data = json.loads(state_data)
        except Exception:
            pass
    
    # Include CPU credits in state for restoration
    if state_data and cpu_credits is not None:
        if isinstance(state_data, dict):
            state_data["_cpu_credits"] = cpu_credits
    
    return task_id_val, state_data


def mark_hibernation_resumed(uid: str, workflow_name: str, baseline: str):
    """Mark hibernation state as resumed."""
    try:
        with mysql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE bot_hibernation_state SET resumed_at = NOW()
                WHERE uuid=%s AND workflow_name=%s AND baseline=%s AND resumed_at IS NULL
                """,
                (uid, workflow_name, baseline)
            )
            conn.commit()
    except mysql.connector.Error:
        pass  # Table might not exist


def bump_retry(uid: str, workflow_name: str, baseline: str, task_id: str, status: int):
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO checkpoint_retries (uid, workflow_name, baseline, retry_count, last_task_id, last_status, last_attempt_at)
            VALUES (%s, %s, %s, 1, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
              retry_count = retry_count + 1,
              last_task_id = VALUES(last_task_id),
              last_status = VALUES(last_status),
              last_attempt_at = VALUES(last_attempt_at)
            """,
            (uid, workflow_name, baseline, task_id, status)
        )
        conn.commit()


# -------------------------
# Retry
# -------------------------
def retry_from_checkpoint(task_id: str, checkpoint: Any, fn_url: str, uid: str, workflow_name: str, baseline: str) -> int:
    data = {"uid": uid, "workflow_name": workflow_name, "baseline": baseline}
    if checkpoint is not None:
        data["checkpoint"] = checkpoint
    try:
        resp = session.post(fn_url, json=data, timeout=(10, 60))  # (connect, read) timeouts
        print(f"🔄 Retried {task_id} for UID {uid}: HTTP {resp.status_code}")
        return int(resp.status_code)
    except Exception as e:
        print(f"⚠️ Retry failed for UID {uid}: {e}")
        return -1


# -------------------------
# Main
# -------------------------
def main():
    global COOLDOWN_SEC, POLL_INTERVAL

    parser = argparse.ArgumentParser(description="Checkpoint-based retry service")
    parser.add_argument("--clear", action="store_true", help="Clear retries table before running")
    parser.add_argument("--baseline", required=True, help="Baseline name (matches what tasks write)")
    parser.add_argument("--workflow", type=str, default=None, help="Filter to specific workflow (optional)")
    parser.add_argument("--cooldown", type=int, default=COOLDOWN_SEC, help="Per-UID cooldown seconds")
    parser.add_argument("--poll", type=int, default=POLL_INTERVAL, help="Polling interval seconds")
    args = parser.parse_args()

    COOLDOWN_SEC = int(args.cooldown)
    POLL_INTERVAL = int(args.poll)

    setup_tables(clear=args.clear)
    
    # Filter to specific workflow if provided
    if args.workflow:
        workflow_names = [args.workflow]
    else:
        workflow_names = sorted([wf for wf in os.listdir(WORKFLOW_DIR) if wf.startswith('wf-')])

    print("Detected workflows:", workflow_names)
    print("Operating baseline:", args.baseline)
    print(f"MAX_RETRIES_PER_UID={MAX_RETRIES_PER_UID}, COOLDOWN_SEC={COOLDOWN_SEC}, POLL_INTERVAL={POLL_INTERVAL}")

    # One-time DAG visibility (helps catch wrong finals)
    dag_checked = set()

    while True:
        for workflow in workflow_names:
            try:
                tasks_data = load_workflow_tasks(workflow)
            except Exception as e:
                print(f"❌ Could not load tasks for {workflow}: {e}")
                continue

            if workflow not in dag_checked:
                verify_dag_once(tasks_data, workflow)
                dag_checked.add(workflow)

            final_tasks = find_final_tasks(tasks_data)
            print(f"🔎 {workflow}: Final tasks = {final_tasks or '[]'}")

            uids = get_uids_incomplete_workflows(workflow, final_tasks, args.baseline)
            if not uids:
                print(f"[{workflow}] No incomplete UIDs to retry.")
                continue

            for uid in uids:
                task_id = None
                checkpoint = None
                is_hibernation = False
                
                # For bag_of_tasks baseline, prefer hibernation state over checkpoint
                if args.baseline == "bag_of_tasks":
                    task_id, checkpoint = get_hibernation_state(uid, workflow, args.baseline)
                    if task_id:
                        is_hibernation = True
                        print(f"💤 UID {uid}: found hibernation state for task '{task_id}'")
                
                # Fall back to regular checkpoint
                if not task_id:
                    task_id, checkpoint = get_latest_checkpoint(uid, workflow, args.baseline)

                # If no checkpoint, derive the next eligible task
                if not task_id:
                    completed = get_completed_tasks(uid, workflow, args.baseline)
                    next_tasks = determine_next_tasks(tasks_data, completed)
                    if not next_tasks:
                        print(f"❌ UID {uid}: no checkpoint and cannot determine next task (completed={sorted(completed)})")
                        continue
                    task_id = next_tasks[0]
                    checkpoint = None
                    print(f"ℹ️ UID {uid}: no checkpoint; next eligible task → '{task_id}' (completed={sorted(completed)})")

                fn_url = FISSION_ROUTER_PREFIX + str(task_id)
                resume_type = "hibernation" if is_hibernation else "checkpoint"
                print(f"🛑 Retrying UID {uid} for {workflow} (baseline={args.baseline}) at task {task_id} [{resume_type}]")

                status = retry_from_checkpoint(task_id, checkpoint, fn_url, uid, workflow, args.baseline)

                # Mark hibernation as resumed if applicable
                if is_hibernation and status == 200:
                    mark_hibernation_resumed(uid, workflow, args.baseline)

                # Count attempt regardless of HTTP result
                bump_retry(uid, workflow, args.baseline, task_id, status)

                time.sleep(0.25)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
