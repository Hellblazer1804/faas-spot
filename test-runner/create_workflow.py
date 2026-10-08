#!/usr/bin/env python3
"""
create_workflow.py — scaffold a new FaaSpot workflow from a tasks.json definition.

Usage:
    python3 test-runner/create_workflow.py \\
        --name  my-wf \\
        --tasks path/to/tasks.json \\
        [--baseline     ours        ] \\
        [--az           us-west-2a  ] \\
        [--instance     v100        ] \\
        [--budget       300         ] \\
        [--run-ranker               ] \\
        [--run-checkpointer         ]

tasks.json format:
    {
      "tasks": {
        "task1": { "exec_time": 2.5, "successors": ["task2"], "minscale": 1 },
        "task2": { "exec_time": 5.0, "successors": [],        "minscale": 2 }
      }
    }

Fields:
  exec_time   – simulated task duration in seconds (float)
  successors  – list of task IDs this task forwards to (empty list for leaf tasks)
  minscale    – minimum Fission replica count; acts as a concurrency hint

After running, the workflow is available in:
  test-runner/workflows/<name>/
  retry-manager/workflows/<name>/

Run with --run-ranker to also compute HEFT scaling factors (writes metadata.json).
Run with --run-checkpointer to compute the checkpoint plan and write it to MySQL.
"""

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
from collections import deque

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(SCRIPT_DIR)
FISSION_BASE = "http://router.fission.svc.cluster.local/fission-function"
AZ_DEFAULT   = os.getenv("TRACE_AZ", "us-west-2a")
INST_DEFAULT = os.getenv("TRACE_INSTANCE", "v100")

# ─── Validation ──────────────────────────────────────────────────────────────

def load_tasks(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if "tasks" not in data or not isinstance(data["tasks"], dict):
        sys.exit("tasks.json must have a top-level 'tasks' object.")
    tasks = data["tasks"]
    if not tasks:
        sys.exit("tasks.json 'tasks' object is empty.")
    required = {"exec_time", "successors", "minscale"}
    for tid, cfg in tasks.items():
        missing = required - cfg.keys()
        if missing:
            sys.exit(f"Task '{tid}' is missing fields: {missing}")
        if not isinstance(cfg["exec_time"], (int, float)) or cfg["exec_time"] <= 0:
            sys.exit(f"Task '{tid}': exec_time must be a positive number.")
        if not isinstance(cfg["successors"], list):
            sys.exit(f"Task '{tid}': successors must be a list.")
        if not isinstance(cfg["minscale"], int) or cfg["minscale"] < 1:
            sys.exit(f"Task '{tid}': minscale must be an integer >= 1.")
        for s in cfg["successors"]:
            if s not in tasks:
                sys.exit(f"Task '{tid}': successor '{s}' is not defined in tasks.")
    _check_acyclic(tasks)
    return tasks


def _check_acyclic(tasks: dict):
    in_degree = {t: 0 for t in tasks}
    for cfg in tasks.values():
        for s in cfg["successors"]:
            in_degree[s] += 1
    queue = deque(t for t, d in in_degree.items() if d == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for s in tasks[node]["successors"]:
            in_degree[s] -= 1
            if in_degree[s] == 0:
                queue.append(s)
    if visited != len(tasks):
        sys.exit("tasks.json contains a cycle — workflow DAGs must be acyclic.")


def _topo_sort(tasks: dict) -> list:
    in_degree = {t: 0 for t in tasks}
    for cfg in tasks.values():
        for s in cfg["successors"]:
            in_degree[s] += 1
    queue = deque(t for t, d in in_degree.items() if d == 0)
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for s in tasks[node]["successors"]:
            in_degree[s] -= 1
            if in_degree[s] == 0:
                queue.append(s)
    return order


def _find_root(tasks: dict) -> str:
    in_degree = {t: 0 for t in tasks}
    for cfg in tasks.values():
        for s in cfg["successors"]:
            in_degree[s] += 1
    roots = [t for t, d in in_degree.items() if d == 0]
    if len(roots) != 1:
        print(f"⚠️  Multiple DAG roots found: {roots} — using '{roots[0]}' as entry point.")
    return roots[0]

# ─── File generation ─────────────────────────────────────────────────────────

_ROLLOUT_FUNC = """\
wait_rollout_for_task() {
  local task="$1"
  local timeout_sec=420
  local waited=0
  local dep_name=""
  while [ "$waited" -lt "$timeout_sec" ]; do
    dep_name="$(kubectl -n "$NS" get deployments -l functionName="$task" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
    if [ -n "$dep_name" ]; then
      if kubectl -n "$NS" rollout status "deploy/$dep_name" --timeout=30s; then
        local desired available
        desired="$(kubectl -n "$NS" get deploy "$dep_name" -o jsonpath='{.status.replicas}' 2>/dev/null || echo 0)"
        available="$(kubectl -n "$NS" get deploy "$dep_name" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)"
        if [ -n "$desired" ] && [ -n "$available" ] && [ "$desired" = "$available" ] && [ "$available" != "" ]; then
          return 0
        fi
      fi
    fi
    sleep 3
    waited=$((waited+3))
  done
  echo "⚠️ Timeout waiting for rollout of task '$task' after ${timeout_sec}s"
  return 1
}
"""


def generate_deploy_script(wf_id: str, tasks: dict, baseline: str, az: str, instance: str) -> str:
    root = _find_root(tasks)
    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        f'WF_ID="{wf_id}"',
        f'BASELINE="{baseline}"',
        'ENV_NAME="serverless"',
        'TASK_TEMPLATE="task_template.py"',
        'NS="default"',
        'echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"',
        "",
        _ROLLOUT_FUNC,
    ]

    for tid, cfg in tasks.items():
        next_urls = ",".join(
            f"{FISSION_BASE}/{s}" for s in cfg["successors"]
        )
        minscale = cfg["minscale"]
        maxscale = minscale * 100 + 1
        cfgmap   = f"{wf_id}-{tid}-{baseline}-cfg"

        lines += [
            f'echo "Applying ConfigMap {cfgmap} for {tid}..."',
            "kubectl -n $NS apply -f - <<CMEOF",
            "apiVersion: v1",
            "kind: ConfigMap",
            "metadata:",
            f"  name: {cfgmap}",
            "  labels:",
            "    app: serverless-wf",
            f"    wf-id: {wf_id}",
            f"    task-id: {tid}",
            f"    baseline: {baseline}",
            "data:",
            f'  TASK_ID: "{tid}"',
            f'  WORKFLOW_ID: "{wf_id}"',
            f'  NEXT_TASK_URL: "{next_urls}"',
            f'  BASELINE: "{baseline}"',
            f'  TASK_EXEC_TIME: "{cfg["exec_time"]}"',
            '  IS_CHECKPOINT: "false"',
            '  CKPT_LEVEL: "1"',
            '  SCALE_FACTOR: "1.0"',
            f'  AZ: "{az}"',
            f'  INSTANCE_TYPE: "{instance}"',
            "CMEOF",
            "",
            f'echo "Creating function {tid} (min={minscale} max={maxscale})..."',
            f"fission fn delete --name {tid} -n $NS >/dev/null 2>&1 || true",
            f"fission fn create --name {tid} --env $ENV_NAME --code $TASK_TEMPLATE"
            f" --executortype newdeploy --minscale {minscale} --maxscale {maxscale}"
            f" --fntimeout 900 --method POST --configmap {cfgmap} -n $NS",
            "sleep 3",
            f'wait_rollout_for_task "{tid}"',
            "",
        ]

    route = f"{wf_id}-route"
    lines += [
        f'echo "Recreating route {route} -> {root} ..."',
        f"fission route delete --name={route} -n $NS >/dev/null 2>&1 || true",
        f"fission route create --name={route} --function {root} --url /{wf_id} --method POST -n $NS",
        f'echo "✅ Deployed {wf_id} for baseline {baseline}"',
    ]
    return "\n".join(lines) + "\n"

# ─── Scaffold ────────────────────────────────────────────────────────────────

def scaffold(wf_id: str, tasks: dict, baseline: str, az: str, instance: str):
    dirs = [
        os.path.join(SCRIPT_DIR, "workflows", wf_id),
        os.path.join(REPO_ROOT, "retry-manager", "workflows", wf_id),
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

    tasks_json_path = os.path.join(SCRIPT_DIR, "workflows", wf_id, "tasks.json")
    with open(tasks_json_path, "w") as f:
        json.dump({"tasks": tasks}, f, indent=2)
    print(f"  Wrote tasks.json → {tasks_json_path}")

    template_src = os.path.join(SCRIPT_DIR, "workflows", "wf-1", "task_template.py")
    for d in dirs:
        dest = os.path.join(d, "task_template.py")
        shutil.copy2(template_src, dest)
    print(f"  Copied task_template.py")

    # tasks.json in retry-manager/workflows too
    rm_tasks = os.path.join(REPO_ROOT, "retry-manager", "workflows", wf_id, "tasks.json")
    shutil.copy2(tasks_json_path, rm_tasks)

    deploy_sh = generate_deploy_script(wf_id, tasks, baseline, az, instance)
    for d in dirs:
        path = os.path.join(d, "deploy_ours.sh")
        with open(path, "w") as f:
            f.write(deploy_sh)
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f"  Generated deploy_ours.sh")

    print(f"\n✅  Workflow '{wf_id}' scaffolded in:")
    for d in dirs:
        print(f"    {d}/")

# ─── Offline analysis ────────────────────────────────────────────────────────

def run_ranker(wf_id: str, budget: float, baseline: str):
    ranker = os.path.join(REPO_ROOT, "task-ranker", "heft_rank_identifier.py")
    workflows_dir = os.path.join(SCRIPT_DIR, "workflows")
    cmd = [
        sys.executable, ranker,
        "--workflow",       wf_id,
        "--budget",         str(budget),
        "--workflows-dir",  workflows_dir,
        "--baseline",       baseline,
    ]
    print(f"\n🔢  Running Task Ranker ...")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print("⚠️  Ranker exited with non-zero status — check output above.")
    else:
        metadata_path = os.path.join(workflows_dir, wf_id, "metadata.json")
        rm_meta = os.path.join(REPO_ROOT, "retry-manager", "workflows", wf_id, "metadata.json")
        if os.path.exists(metadata_path):
            shutil.copy2(metadata_path, rm_meta)
            print(f"  Copied metadata.json → retry-manager/workflows/{wf_id}/")


def run_checkpointer(wf_id: str, baseline: str, az: str, instance: str):
    chk = os.path.join(REPO_ROOT, "checkpoint-planner", "checkpointer.py")
    workflows_dir = os.path.join(SCRIPT_DIR, "workflows")
    cmd = [
        sys.executable, chk,
        "--workflows-dir",     workflows_dir,
        "--workflow",          wf_id,
        "--az",                az,
        "--instance",          instance,
        "--baseline",          baseline,
        "--write-plan-to-db",
    ]
    print(f"\n📋  Running Checkpoint Planner ...")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print("⚠️  Checkpointer exited with non-zero status — check output above.")

# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Scaffold a custom FaaSpot workflow from a tasks.json definition.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--name",              required=True,  help="Workflow ID (e.g. my-wf)")
    p.add_argument("--tasks",             required=True,  help="Path to tasks.json")
    p.add_argument("--baseline",          default="ours", help="Baseline label (default: ours)")
    p.add_argument("--az",                default=AZ_DEFAULT,   help="Availability zone for trace lookup")
    p.add_argument("--instance",          default=INST_DEFAULT, help="Instance type for trace lookup")
    p.add_argument("--budget",            type=float, default=300.0, help="Spot budget in dollars for ranker (default: 300)")
    p.add_argument("--run-ranker",        action="store_true", help="Run the HEFT Task Ranker after scaffolding")
    p.add_argument("--run-checkpointer",  action="store_true", help="Run the Checkpoint Planner after scaffolding")
    args = p.parse_args()

    if not os.path.isfile(args.tasks):
        sys.exit(f"tasks.json not found: {args.tasks}")

    print(f"\n📐 Validating {args.tasks} ...")
    tasks = load_tasks(args.tasks)
    root  = _find_root(tasks)
    order = _topo_sort(tasks)
    print(f"  Tasks ({len(tasks)}): {order}")
    print(f"  Entry point: {root}")
    print(f"  Validation: OK\n")

    print(f"📁 Scaffolding workflow '{args.name}' ...")
    scaffold(args.name, tasks, args.baseline, args.az, args.instance)

    if args.run_ranker:
        run_ranker(args.name, args.budget, args.baseline)

    if args.run_checkpointer:
        run_checkpointer(args.name, args.baseline, args.az, args.instance)

    print(f"\nNext steps:")
    print(f"  1. cd test-runner/workflows/{args.name}")
    print(f"  2. bash deploy_ours.sh            # deploy to Fission")
    print(f"  3. python3 test-runner/test_runner.py --workflows {args.name}")

if __name__ == "__main__":
    main()
