import os
#!/usr/bin/env python3
"""
Simplified Checkpoint Classification Service

Classifies each task in a workflow DAG into one of three criticality levels:
- CRITICAL: Parallelism boundaries (successor or predecessor has minscale > 1)
- MEDIUM: Parallel work (minscale > 1) or feeds into joins
- LOW: Simple sequential work

No CDF, no DP, no exec_time thresholds.
Based purely on task structure and minscale from tasks.json.
"""
import argparse
import json
import sys
import pymysql

# ============================================================================
# CHECKPOINT LEVELS
# ============================================================================
CHECKPOINT_LEVEL_CRITICAL = "CRITICAL"
CHECKPOINT_LEVEL_MEDIUM = "MEDIUM"
CHECKPOINT_LEVEL_LOW = "LOW"

# ============================================================================
# CORE ALGORITHM
# ============================================================================

def load_tasks_json(tasks_json_path):
    """Load tasks from tasks.json"""
    with open(tasks_json_path) as f:
        return json.load(f)["tasks"]


def build_dep_maps(tasks):
    """Build successor and predecessor maps from tasks."""
    task_to_successors = {t: list(tasks[t].get("successors", [])) for t in tasks}
    task_to_predecessors = {t: [] for t in tasks}

    for u, succs in task_to_successors.items():
        for v in succs:
            task_to_predecessors.setdefault(v, []).append(u)

    return task_to_successors, task_to_predecessors


def calculate_task_depth(task_id, tasks, t2d, t2a):
    """
    Calculate normalized depth of task in DAG (0.0 = start, 1.0 = end).
    Uses longest path from start to this task / longest path in entire DAG.
    """
    # Calculate longest path to each task
    longest_path_memo = {}

    def get_longest_path(tid):
        if tid in longest_path_memo:
            return longest_path_memo[tid]
        preds = t2a.get(tid, [])
        if not preds:
            longest_path_memo[tid] = 1
        else:
            longest_path_memo[tid] = 1 + max((get_longest_path(p) for p in preds), default=0)
        return longest_path_memo[tid]

    # Calculate longest path for all tasks
    for task in tasks:
        get_longest_path(task)

    # Get max path length in DAG
    max_path = max(longest_path_memo.values()) if longest_path_memo else 1

    # Return normalized depth for this task
    path_length = longest_path_memo.get(task_id, 1)
    return path_length / max_path


def classify_single_task(task_id, tasks, t2d, t2a, task_depths=None):
    """
    Classify a single task as CRITICAL, MEDIUM, or LOW based purely on minscale.

    Classification rules:
    - CRITICAL: any successor has minscale != this task's minscale (transition/boundary)
    - MEDIUM: this task's minscale > 1 AND no transition (in parallel zone)
    - LOW: this task's minscale = 1 AND no transition (purely sequential)
    """
    successors = t2d.get(task_id, [])
    task_minscale = tasks[task_id].get("minscale", 1)

    # === CRITICAL: Minscale transitions ===

    # Any transition to a different minscale (fan-in or fan-out)
    for succ_id in successors:
        succ_minscale = tasks[succ_id].get("minscale", 1)
        if succ_minscale != task_minscale:
            return CHECKPOINT_LEVEL_CRITICAL

    # === No transition — classify by this task's minscale ===

    # MEDIUM: in a parallel zone (minscale > 1)
    if task_minscale > 1:
        return CHECKPOINT_LEVEL_MEDIUM

    # LOW: purely sequential (minscale = 1)
    return CHECKPOINT_LEVEL_LOW


def classify_tasks(tasks_json_path):
    """
    Classify all tasks in a workflow.

    Returns: {task_id: level}
    """
    tasks = load_tasks_json(tasks_json_path)
    t2d, t2a = build_dep_maps(tasks)

    # Pre-calculate depths for all tasks
    task_depths = {}
    for task_id in tasks:
        task_depths[task_id] = calculate_task_depth(task_id, tasks, t2d, t2a)

    levels = {}
    for task_id in tasks:
        level = classify_single_task(task_id, tasks, t2d, t2a, task_depths=task_depths)
        levels[task_id] = level

    return levels


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def get_db_connection(host, user, password, db):
    """Create database connection."""
    try:
        return pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.Error as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)


def ensure_plan_table(db):
    """Ensure workflow_checkpoint_plan table exists."""
    with db.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS workflow_checkpoint_plan (
              workflow_name VARCHAR(256),
              baseline VARCHAR(128),
              task_id VARCHAR(256),
              checkpoint_level VARCHAR(32) DEFAULT 'LOW',
              notes VARCHAR(255) DEFAULT 'main_experiment',
              PRIMARY KEY (workflow_name, baseline, task_id)
            )
        """)
        db.commit()


def write_plan_to_db(db, workflow, baseline, checkpoint_levels, experiment_tag="main_experiment"):
    """Write checkpoint plan to database."""
    ensure_plan_table(db)

    with db.cursor() as cur:
        for task_id, level in checkpoint_levels.items():
            cur.execute("""
                INSERT INTO workflow_checkpoint_plan
                (workflow_name, baseline, task_id, checkpoint_level, notes)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                checkpoint_level = VALUES(checkpoint_level),
                notes = VALUES(notes)
            """, (workflow, baseline, task_id, level, experiment_tag))
        db.commit()


def print_plan(checkpoint_levels):
    """Print checkpoint plan to stdout."""
    if not checkpoint_levels:
        print("No checkpoints assigned.")
        return

    # Group by level
    critical = {t: l for t, l in checkpoint_levels.items() if l == CHECKPOINT_LEVEL_CRITICAL}
    medium = {t: l for t, l in checkpoint_levels.items() if l == CHECKPOINT_LEVEL_MEDIUM}
    low = {t: l for t, l in checkpoint_levels.items() if l == CHECKPOINT_LEVEL_LOW}

    if critical:
        print(f"CRITICAL tasks ({len(critical)}):")
        for task_id in sorted(critical.keys()):
            print(f"  {task_id}")

    if medium:
        print(f"\nMEDIUM tasks ({len(medium)}):")
        for task_id in sorted(medium.keys()):
            print(f"  {task_id}")

    if low:
        print(f"\nLOW tasks ({len(low)}):")
        for task_id in sorted(low.keys()):
            print(f"  {task_id}")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Simplified Checkpoint Classification Service"
    )
    parser.add_argument(
        "--workflows-dir",
        required=True,
        help="Directory containing workflow subdirectories"
    )
    parser.add_argument(
        "--workflow",
        required=True,
        help="Workflow name (e.g., wf-1)"
    )
    parser.add_argument(
        "--baseline",
        default="ours",
        help="Baseline label for database (default: ours)"
    )
    parser.add_argument(
        "--write-plan-to-db",
        action="store_true",
        help="Write plan to MySQL database"
    )
    parser.add_argument(
        "--mysql-host",
        default=os.getenv("DB_HOST", "localhost"),
        help="MySQL host (default: localhost)"
    )
    parser.add_argument(
        "--mysql-user",
        default=os.getenv("DB_USER", ""),
        help="MySQL user (default: remote)"
    )
    parser.add_argument(
        "--mysql-password",
        default=os.getenv("DB_PASSWORD", ""),
        help="MySQL password"
    )
    parser.add_argument(
        "--mysql-db",
        default=os.getenv("DB_NAME", ""),
        help="MySQL database (default: wms)"
    )
    parser.add_argument(
        "--experiment-tag",
        default="main_experiment",
        help="Tag written to database notes column"
    )

    args = parser.parse_args()

    # Construct path to tasks.json
    tasks_json_path = f"{args.workflows_dir}/{args.workflow}/tasks.json"

    print(f"Classifying tasks from {tasks_json_path}...", file=sys.stderr)

    # Classify tasks
    checkpoint_levels = classify_tasks(tasks_json_path)

    # Print results
    print_plan(checkpoint_levels)

    # Write to database if requested
    if args.write_plan_to_db:
        print(f"\nWriting plan to database {args.mysql_host}:{args.mysql_db}...", file=sys.stderr)
        db = get_db_connection(
            args.mysql_host,
            args.mysql_user,
            args.mysql_password,
            args.mysql_db
        )
        try:
            write_plan_to_db(db, args.workflow, args.baseline, checkpoint_levels, args.experiment_tag)
            print(f"Plan written successfully for workflow={args.workflow}, baseline={args.baseline}", file=sys.stderr)
        finally:
            db.close()


if __name__ == "__main__":
    main()
