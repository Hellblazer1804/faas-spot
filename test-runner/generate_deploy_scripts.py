import os
#!/usr/bin/env python3
import json, os, re, random, sys

# Add checkpoint-service to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'checkpoint-service'))
from checkpoint_service import fetch_checkpoint_levels

WORKFLOW_IDS = [f”wf-{i}” for i in range(1,10)]
ENV_NAME = “serverless”
TASK_TEMPLATE = “task_template.py”
NAMESPACE = “default”

# one baseline for your run; “ours” per your note
BASELINE_KEY = “ours”

# MySQL connection — set via environment variables (see .env.example)
MYSQL_HOST     = os.getenv(“DB_HOST”, “localhost”)
MYSQL_USER     = os.getenv(“DB_USER”, “”)
MYSQL_PASSWORD = os.getenv(“DB_PASSWORD”, “”)
MYSQL_DB       = os.getenv(“DB_NAME”, “”)

def dns1123(s):
    s = s.lower().replace('_', '-')
    s = re.sub(r'[^a-z0-9\.-]+', '-', s)
    s = re.sub(r'^[^a-z0-9]+', '', s)
    s = re.sub(r'[^a-z0-9]+$', '', s)
    return s[:63] or "x"

def generate_deploy_script(wf_id, tasks, ckpt_levels=None):
    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        f'WF_ID="{wf_id}"',
        f'BASELINE="{BASELINE_KEY}"',
        f'ENV_NAME="{ENV_NAME}"',
        f'TASK_TEMPLATE="{TASK_TEMPLATE}"',
        f'NS="{NAMESPACE}"',
        'echo "Deploying $WF_ID baseline=$BASELINE in ns=$NS"',
        "",
        "# Robust rollout wait with auto-retry if deployment is recreated",
        "wait_rollout_for_task() {",
        "  local task=\"$1\"",
        "  local timeout_sec=420",
        "  local waited=0",
        "  local dep_name=\"\"",
        "  while [ \"$waited\" -lt \"$timeout_sec\" ]; do",
        "    dep_name=\"$(kubectl -n \"$NS\" get deployments -l functionName=\"$task\" -o jsonpath=\"{.items[0].metadata.name}\" 2>/dev/null || true)\"",
        "    if [ -n \"$dep_name\" ]; then",
        "      # Try to wait for rollout; if the object was deleted/recreated, retry",
        "      if kubectl -n \"$NS\" rollout status \"deploy/$dep_name\" --timeout=30s; then",
        "        # Double-check availability equals desired to avoid race on recreate",
        "        local desired available",
        "        desired=\"$(kubectl -n \"$NS\" get deploy \"$dep_name\" -o jsonpath='{.status.replicas}' 2>/dev/null || echo 0)\"",
        "        available=\"$(kubectl -n \"$NS\" get deploy \"$dep_name\" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)\"",
        "        if [ -n \"$desired\" ] && [ -n \"$available\" ] && [ \"$desired\" = \"$available\" ] && [ \"$available\" != \"\" ]; then",
        "          return 0",
        "        fi",
        "      fi",
        "    fi",
        "    sleep 3",
        "    waited=$((waited+3))",
        "  done",
        "  echo \"⚠️ Timeout waiting for rollout of task '$task' after ${timeout_sec}s\"",
        "  return 1",
        "}",
        ""
    ]

    if ckpt_levels is None:
        ckpt_levels = {}

    for task, meta in tasks.items():
        minscale = int(meta.get("minscale", 1))
        maxscale = minscale*100 + 1
        exec_time = float(meta.get("exec_time", 0.25)) * random.uniform(1.0, 5.0)

        next_urls = ",".join(
            f"http://router.fission.svc.cluster.local/fission-function/{s}"
            for s in meta.get("successors", []) if s
        )

        # Get checkpoint level from fetched plan, default to 1 if not found
        ckpt_level = ckpt_levels.get(task, 1)
        # Only checkpoint for CRITICAL (3) and MEDIUM (2), not for LOW (1)
        is_checkpoint = "true" if ckpt_level >= 2 else "false"

        cm_name = dns1123(f"{wf_id}-{task}-{BASELINE_KEY}-cfg")
        lines += [
            f'echo "Applying ConfigMap {cm_name} for {task}..."',
            'kubectl -n $NS apply -f - <<CMEOF',
            'apiVersion: v1',
            'kind: ConfigMap',
            'metadata:',
            f'  name: {cm_name}',
            '  labels:',
            '    app: serverless-wf',
            f'    wf-id: {wf_id}',
            f'    task-id: {task}',
            f'    baseline: {BASELINE_KEY}',
            'data:',
            f'  TASK_ID: "{task}"',
            f'  WORKFLOW_ID: "{wf_id}"',
            f'  NEXT_TASK_URL: "{next_urls}"',
            f'  BASELINE: "{BASELINE_KEY}"',
            f'  TASK_EXEC_TIME: "{exec_time:.4f}"',
            f'  IS_CHECKPOINT: "{is_checkpoint}"',
            f'  CKPT_LEVEL: "{ckpt_level}"',
            '  SCALE_FACTOR: "1.0"',
            '  AZ: "us-west-2a"',
            '  INSTANCE_TYPE: "v100"',
            'CMEOF',
            "",
            f'echo "Creating function {task} (min={minscale} max={maxscale})..."',
            f'fission fn delete --name {task} -n $NS >/dev/null 2>&1 || true',
            (
                "fission fn create "
                f"--name {task} "
                f"--env $ENV_NAME "
                f"--code $TASK_TEMPLATE "
                f"--executortype newdeploy "
                f"--minscale {minscale} --maxscale {maxscale} "
                f"--fntimeout 900 --method POST "  # Increased timeout to 15 min to avoid dry run failures
                f"--configmap {cm_name} "
                f"-n $NS"
            ),
            "sleep 3",
            f'wait_rollout_for_task "{task}"',
            ""
        ]

    first_task = next(iter(tasks.keys()))
    lines += [
        f'echo "Recreating route {wf_id}-route -> {first_task} ..."',
        f'fission route delete --name={wf_id}-route -n $NS >/dev/null 2>&1 || true',
        f'fission route create --name={wf_id}-route --function {first_task} --url /{wf_id} --method POST -n $NS',
        f'echo "✅ Deployed {wf_id} for baseline {BASELINE_KEY}"'
    ]
    return "\n".join(lines)

def main():
    for wf_id in WORKFLOW_IDS:
        wf_dir = os.path.join("workflows", wf_id)
        tasks_path = os.path.join(wf_dir, "tasks.json")
        if not os.path.exists(tasks_path):
            print(f"⚠️ Skipping {wf_id}, no tasks.json")
            continue
        with open(tasks_path) as f:
            tasks = json.load(f)["tasks"]

        # Fetch checkpoint levels from DB for this workflow
        ckpt_levels = fetch_checkpoint_levels(
            wf_id,
            BASELINE_KEY,
            MYSQL_HOST,
            MYSQL_USER,
            MYSQL_PASSWORD,
            MYSQL_DB
        )
        if ckpt_levels:
            print(f"  Fetched checkpoint levels for {wf_id}: {ckpt_levels}", file=sys.stderr)
        else:
            print(f"  No checkpoint plan found in DB for {wf_id}, using defaults", file=sys.stderr)

        script = generate_deploy_script(wf_id, tasks, ckpt_levels)
        out = os.path.join(wf_dir, f"deploy_{BASELINE_KEY}.sh")
        with open(out, "w") as fo:
            fo.write(script)
        os.chmod(out, 0o755)
        print(f"Generated {out}")

if __name__ == "__main__":
    main()
