import os
#!/usr/bin/env python3
"""
Checkpoint-Service: Query workflow checkpoint plan from DB.

This module provides a function to fetch checkpoint levels from the
workflow_checkpoint_plan table and map them to integer levels:
  CRITICAL → 3
  MEDIUM → 2
  LOW → 1
"""
import argparse
import sys
import pymysql

# ============================================================================
# LEVEL MAPPING
# ============================================================================
LEVEL_MAP = {
    "CRITICAL": 3,
    "MEDIUM": 2,
    "LOW": 1,
}

LEVEL_NAMES = {v: k for k, v in LEVEL_MAP.items()}


def get_db_connection(host, user, password, db):
    """Create a MySQL database connection."""
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
        print(f"❌ Error connecting to database: {e}", file=sys.stderr)
        raise


def fetch_checkpoint_levels(workflow, baseline, host, user, password, db):
    """
    Query workflow_checkpoint_plan and return {task_id: int_level}.

    Maps string levels (CRITICAL/MEDIUM/LOW) to integers (3/2/1).
    Returns an empty dict if no plan exists for the workflow.

    Args:
        workflow: Workflow name (e.g., "wf-1")
        baseline: Baseline label (e.g., "ours")
        host, user, password, db: MySQL connection params

    Returns:
        dict: {task_id (str): level (int)}
    """
    try:
        db_conn = get_db_connection(host, user, password, db)
        levels = {}

        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT task_id, checkpoint_level
                FROM workflow_checkpoint_plan
                WHERE workflow_name = %s AND baseline = %s
                ORDER BY task_id
            """, (workflow, baseline))

            rows = cur.fetchall()
            for row in rows:
                task_id = row['task_id']
                level_str = row['checkpoint_level']
                level_int = LEVEL_MAP.get(level_str, 1)
                levels[task_id] = level_int

        db_conn.close()
        return levels

    except pymysql.Error as e:
        print(f"❌ Error querying database: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        return {}


def main():
    """CLI interface for manual inspection and debugging."""
    parser = argparse.ArgumentParser(
        description="Query checkpoint levels for a workflow"
    )
    parser.add_argument(
        "--workflow",
        required=True,
        help="Workflow name (e.g., wf-1)"
    )
    parser.add_argument(
        "--baseline",
        default="ours",
        help="Baseline label (default: ours)"
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

    args = parser.parse_args()

    print(f"Fetching checkpoint levels for {args.workflow}/{args.baseline}...", file=sys.stderr)

    levels = fetch_checkpoint_levels(
        args.workflow,
        args.baseline,
        args.mysql_host,
        args.mysql_user,
        args.mysql_password,
        args.mysql_db
    )

    if not levels:
        print(f"⚠️  No checkpoint plan found for {args.workflow}/{args.baseline}")
        return

    print(f"Checkpoint levels for {args.workflow}/{args.baseline}:")
    for task_id in sorted(levels.keys()):
        level_int = levels[task_id]
        level_str = LEVEL_NAMES.get(level_int, "UNKNOWN")
        print(f"  {task_id}: {level_int} ({level_str})")


if __name__ == "__main__":
    main()
