import mysql.connector
import json
import os
import argparse

DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}

def _workflow_dir(args=None):
    """Resolve workflow directory: --workflows-dir if given, else default (Experiments/workflows)."""
    if args is not None and getattr(args, 'workflows_dir', None):
        return args.workflows_dir
    return os.path.join(os.path.dirname(__file__), "../workflows")

WORKFLOW_DIR = os.path.join(os.path.dirname(__file__), "../workflows")  # default for legacy callers

def mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

def _parse_tags(raw):
    if not raw:
        return []
    return [t.strip() for t in str(raw).split(",") if t.strip()]

def evaluate_workflow_success_with_paths(workflow_name, baseline=None, workflow_dir=None, experiment_tags=None):
    """Evaluate workflow success considering valid execution paths."""
    wdir = workflow_dir or WORKFLOW_DIR
    tasks_path = os.path.join(wdir, workflow_name, "tasks.json")
    with open(tasks_path) as f:
        tasks_data = json.load(f)["tasks"]
    task_ids = list(tasks_data.keys())

    conn = mysql_connection()
    cursor = conn.cursor()

    sql_query = "SELECT DISTINCT uuid_passed FROM serverless_workflows WHERE workflow_name = %s"
    params = [workflow_name]
    if baseline:
        sql_query += " AND baseline = %s"
        params.append(baseline)
    if experiment_tags:
        placeholders = ",".join(["%s"] * len(experiment_tags))
        sql_query += f" AND notes IN ({placeholders})"
        params.extend(experiment_tags)
    
    cursor.execute(sql_query, tuple(params))
    all_uids = {str(row[0]) for row in cursor.fetchall() if row[0]}

    successful_uids = {uid for uid in all_uids if is_valid_execution_path(
        uid, workflow_name, task_ids, tasks_data, cursor, baseline, experiment_tags
    )}

    conn.close()

    total = len(all_uids)
    success = len(successful_uids)
    rate = success / total if total > 0 else 0.0

    baseline_str = f" (Baseline: {baseline})" if baseline else ""
    tag_str = f" [tags: {', '.join(experiment_tags)}]" if experiment_tags else ""
    print(f"Workflow: {workflow_name}{baseline_str}{tag_str} (with path validation)")
    print(f"Total Runs Initiated: {total}")
    print(f"Successful Runs (Valid Paths): {success}")
    print(f"Success Rate: {rate*100:.2f}%")

    return {
        "workflow": workflow_name, "baseline": baseline,
        "total_runs": total, "successful": success, "success_rate": rate
    }

def is_valid_execution_path(uid, workflow_name, task_ids, tasks_data, cursor, baseline=None, experiment_tags=None):
    """Checks if a UID completed a valid execution path."""
    sql_query = "SELECT DISTINCT workflow_stage FROM serverless_workflows WHERE uuid_passed = %s AND workflow_name = %s AND response_code = 200"
    params = [uid, workflow_name]
    if baseline:
        sql_query += " AND baseline = %s"
        params.append(baseline)
    if experiment_tags:
        placeholders = ",".join(["%s"] * len(experiment_tags))
        sql_query += f" AND notes IN ({placeholders})"
        params.extend(experiment_tags)
        
    cursor.execute(sql_query, tuple(params))
    completed_tasks = {row[0] for row in cursor.fetchall()}
    
    if workflow_name == "wf-2":
        required_start = {"task1", "task2", "task3"}
        if not required_start.issubset(completed_tasks):
            return False
        
        path_a = {"task3a", "taskr1"}
        path_b = {"task3b", "taskr2"}
        return path_a.issubset(completed_tasks) or path_b.issubset(completed_tasks)
    
    else:
        return set(task_ids).issubset(completed_tasks)

def get_available_baselines(experiment_tags=None):
    """Get all available baselines from the database."""
    conn = mysql_connection()
    cursor = conn.cursor()
    sql_query = "SELECT DISTINCT baseline FROM serverless_workflows WHERE baseline IS NOT NULL"
    params = []
    if experiment_tags:
        placeholders = ",".join(["%s"] * len(experiment_tags))
        sql_query += f" AND notes IN ({placeholders})"
        params.extend(experiment_tags)
    cursor.execute(sql_query, tuple(params))
    baselines = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()
    return sorted(baselines)

def main():
    parser = argparse.ArgumentParser(description='Evaluate workflow success rates')
    parser.add_argument('--baseline', help='Filter by specific baseline')
    parser.add_argument('--list-baselines', action='store_true', help='List all available baselines')
    parser.add_argument('--compare-baselines', action='store_true', help='Compare success rates across all baselines')
    parser.add_argument('--workflow', help='Evaluate specific workflow only')
    parser.add_argument('--experiment-tag', help='Filter by experiment tag (notes). Comma-separated for multiple tags')
    parser.add_argument('--use-paths', action='store_true', help='Use path-aware evaluation for branching workflows')
    parser.add_argument('--workflows-dir', help='Override workflow directory (e.g. Algorithm-Tester/workflows) for tasks.json')
    
    args = parser.parse_args()
    
    workflow_dir = _workflow_dir(args)
    if args.workflows_dir:
        workflow_dir = args.workflows_dir
    
    experiment_tags = _parse_tags(args.experiment_tag)

    if args.list_baselines:
        baselines = get_available_baselines(experiment_tags)
        print("Available baselines:")
        for baseline in baselines:
            print(f"  - {baseline}")
        return
    
    workflows = [d for d in os.listdir(workflow_dir) if d.startswith("wf-")]
    if args.workflow:
        if args.workflow not in workflows:
            print(f"Error: Workflow '{args.workflow}' not found in {workflow_dir}")
            return
        workflows = [args.workflow]
    
    # *** KEY CHANGE: Use underscore instead of hyphen ***
    if args.compare_baselines:
        baselines = get_available_baselines(experiment_tags)
        print("Comparing success rates across baselines:")
        print("=" * 80)
        
        for wf in sorted(workflows):
            print(f"\nWorkflow: {wf}")
            print("-" * 40)
            wf_results = []
            
            for baseline in baselines:
                try:
                    result = evaluate_workflow_success_with_paths(
                        wf, baseline, workflow_dir=workflow_dir, experiment_tags=experiment_tags
                    )
                    wf_results.append(result)
                except Exception as e:
                    print(f"Error evaluating {wf} for baseline {baseline}: {e}")
            
            wf_results.sort(key=lambda x: x['success_rate'], reverse=True)
            print(f"\nRanking for {wf}:")
            for i, result in enumerate(wf_results, 1):
                print(f"{i}. {result['baseline']}: {result['success_rate']*100:.2f}% ({result['successful']}/{result['total_runs']})")
    else:
        for wf in sorted(workflows):
            try:
                evaluate_workflow_success_with_paths(
                    wf, args.baseline, workflow_dir=workflow_dir, experiment_tags=experiment_tags
                )
            except Exception as e:
                print(f"Error evaluating {wf}: {e}")

if __name__ == "__main__":
    main()