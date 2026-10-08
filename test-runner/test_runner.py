#!/usr/bin/env python3
import os
import re
import subprocess
import time
import signal
import json
import sys
import argparse
import requests
import mysql.connector
from datetime import datetime
from typing import Optional, Tuple, Dict

from kubernetes import client, config

from discord import init_discord_notifications, get_discord_notifier

# Database configuration (same as scaler)
DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}
# --- Paths & Constants ---
TESTER_DIR          = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS_DIR       = os.path.join(TESTER_DIR, "workflows")
RANKER_SCRIPT       = os.path.join(TESTER_DIR, "../Ranker/heft_rank_identifier.py")
SCALER_SCRIPT       = os.path.join(TESTER_DIR, "../Scaler/preemptive_scaler.py")
EMULATOR_SCRIPT     = os.path.join(TESTER_DIR, "../New-Emulator/Emulator.py")
CHECKPOINTER_SCRIPT = os.path.join(TESTER_DIR, "../Checkpoint-Planner/checkpointer.py")
EVAL_SCRIPT         = os.path.join(TESTER_DIR, "../Experiments/evaluation-scripts/success_rate.py")
CLEANUP_SCRIPT      = os.path.join(TESTER_DIR, "../Experiments/cleanup.sh")
SURVIVAL_CURVE_PATH = os.path.join(TESTER_DIR, "../Scaler/survival_curves/us-west-2a_v100_cdf_survival_curve.json")
LOADGEN_SCRIPT      = os.path.join(TESTER_DIR, "load_generator.py")
RETRY_SERVICE_SCRIPT = os.path.join(TESTER_DIR, "../Retry-Service-v2/retry_service.py")
RETRY_SERVICE_DIR    = os.path.dirname(RETRY_SERVICE_SCRIPT)
EMULATOR_DIR        = os.path.dirname(EMULATOR_SCRIPT)
# Checkscale: every task checkpointed (task templates write checkpoint on completion);
# retry service prioritizes retries from checkpointer-selected tasks; when success drops
# below threshold, also retries from non-selected checkpoints (Retry-Service-v2).
BASELINE            = "ours"
NAMESPACE           = "default"
EXPERIMENT_TAG      = os.getenv("EXPERIMENT_TAG", "main_experiment")

# spot trace selection for checkpointer
TRACE_AZ            = "us-west-2a"
TRACE_INSTANCE      = "v100"

# Router (NodePort) endpoint to kick workflows
ROUTER_ENTRY_URL    = os.getenv("ROUTER_BASE_URL", "http://localhost:32363")

# --- Dry Run Retry Configuration ---
DRY_RUN_MAX_RETRIES     = 3
DRY_RUN_RETRY_DELAY     = 10

# --- Success Rate Retry Configuration ---
SUCCESS_RATE_MAX_RETRIES = 3
SUCCESS_RATE_RETRY_DELAY = 30

# Workflow classifications based on DAG depth and complexity
# Simple workflows (wf-1, wf-2, wf-3, wf-4, wf-7): Shallow DAGs, fewer dependencies
# Complex workflows (wf-5, wf-6, wf-8, wf-9): Deeper DAGs, more dependencies, bottlenecks
SIMPLE_WORKFLOWS = {'wf-1', 'wf-2', 'wf-3', 'wf-4', 'wf-7'}
COMPLEX_WORKFLOWS = {'wf-5', 'wf-6', 'wf-8', 'wf-9'}

# Target success rates (aligned with scaler and retry service)
# Simple workflows: target 95-100% (use 97.5% as midpoint)
# Complex workflows: target 65-70% (use 67.5% as midpoint)
SIMPLE_WORKFLOW_TARGET_SUCCESS_RATE = 97.5  # Percentage
COMPLEX_WORKFLOW_TARGET_SUCCESS_RATE = 67.5  # Percentage

def get_target_success_rate(workflow_name: str) -> float:
    """Get target success rate for a workflow based on its complexity."""
    if workflow_name in COMPLEX_WORKFLOWS:
        return COMPLEX_WORKFLOW_TARGET_SUCCESS_RATE
    else:
        return SIMPLE_WORKFLOW_TARGET_SUCCESS_RATE

# -----------------------------
# Cost Calculation from Database (same as scaler)
# -----------------------------
# Machine packing: 3 pods share 1 machine
PODS_PER_MACHINE = 3

# Emulator time compression: 1 real second = 60 emulator seconds
EMULATOR_TIME_COMPRESSION = 60

# Budget as percentage of on-demand cost
# Updated: 50% hard cap includes all costs (scaling, retry, churn)
# Exception: wf-5 and wf-9 get 60% base (adaptive in scaler)
BUDGET_DEFAULT_PERCENT = 0.50  # 50% of on-demand (default for most workflows)

# Per-workflow budget overrides (complex workflows get more headroom)
WORKFLOW_BUDGET_PERCENT = {
    'wf-5': 0.60,  # 60% base - complex deep DAG, adaptive scaler may boost
    'wf-9': 0.60,  # 60% base - most complex workflow, adaptive scaler may boost
    # All other workflows use BUDGET_DEFAULT_PERCENT (50%)
}

# Churn overhead: Previously used to account for pod respawning after emulator kills pods
# Now set to 1.0 because budget cap already includes all costs
# Must match scaler's CHURN_OVERHEAD_FACTOR for consistent budget calculations
CHURN_OVERHEAD_FACTOR = 1.0  # No additional overhead

# RL Adaptive Scaler mode (contextual bandit for scaling decisions)
# Cold start and activation settings are now workflow-specific in rl_config.py
# The scaler reads them from the config at runtime, no need to pass via CLI
RL_MODE_ENABLED = True  # Set to True to enable RL-based scaling decisions

def env_flag(name: str) -> bool:
    val = os.getenv(name, "").strip().lower()
    return val in {"1", "true", "yes", "y", "on"}

def parse_env_percent(raw: str):
    if not raw:
        return None
    try:
        val = float(raw)
    except Exception:
        return None
    if val > 1.0:
        val = val / 100.0
    return max(0.0, min(1.0, val))

def parse_env_workflow_set(raw: str):
    if not raw:
        return set()
    return {w.strip() for w in raw.split(",") if w.strip()}

CHECKSCALE_DISABLE_CHECKPOINTER = env_flag("CHECKSCALE_DISABLE_CHECKPOINTER")
CHECKSCALE_DISABLE_SCALER = env_flag("CHECKSCALE_DISABLE_SCALER")
CHECKSCALE_DISABLE_RETRY = env_flag("CHECKSCALE_DISABLE_RETRY")
CHECKSCALE_BUDGET_OVERRIDE_PERCENT = parse_env_percent(os.getenv("CHECKSCALE_BUDGET_OVERRIDE_PERCENT"))
CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS = parse_env_workflow_set(os.getenv("CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS"))
IGNORE_SIGHUP = env_flag("TEST_RUNNER_IGNORE_SIGHUP")

def resolve_discord_config_path():
    candidate_paths = [
        os.path.join(TESTER_DIR, "discord_config.json"),
        os.path.join(TESTER_DIR, "discord", "discord_config.json"),
    ]
    for path in candidate_paths:
        if os.path.exists(path):
            return path
    return candidate_paths[0]

def mysql_connection():
    """Create MySQL database connection."""
    return mysql.connector.connect(**DB_CONFIG)

def ensure_metadata_columns():
    """Ensure notes column exists on experiment tables for tagging."""
    db_name = DB_CONFIG.get("database")
    tables = {
        "serverless_workflows": "notes VARCHAR(255) DEFAULT 'main_experiment'",
        "task_checkpoints": "notes VARCHAR(255) DEFAULT 'main_experiment'",
        "cost_logs": "notes VARCHAR(255) DEFAULT 'main_experiment'",
        "checkpoint_retries": "notes VARCHAR(255) DEFAULT 'main_experiment'",
        "workflow_checkpoint_plan": "notes VARCHAR(255) DEFAULT 'main_experiment'",
    }
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        for table, col_def in tables.items():
            cursor.execute(
                """SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""",
                (db_name, table),
            )
            if cursor.fetchone()[0] == 0:
                continue
            cursor.execute(
                """SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='notes'""",
                (db_name, table),
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Failed to ensure notes columns: {e}")

def task_from_podname(pod_name: str) -> str:
    """Extract task name from pod name (e.g., 'task1-abc123' -> 'task1')."""
    if not pod_name:
        return ""
    # Pod names are like 'task1-abc123-xyz' - extract the task part
    parts = pod_name.split('-')
    if parts:
        return parts[0]
    return pod_name

def normalize_baseline(value: Optional[str]) -> str:
    """Normalize baseline names for consistent comparison."""
    if value is None:
        return ""
    normalized = str(value).strip().lower()
    if normalized in {"none", "nan", ""}:
        return ""
    return normalized

def compute_cost_per_wf_baseline(cost_df, evals_df, task_to_wf, wf_minscale):
    if cost_df is None or cost_df.empty:
        import pandas as pd
        return pd.DataFrame(columns=["workflow_name", "baseline", "total_cost"])

    import pandas as pd
    import numpy as np

    c = cost_df.copy()
    c.columns = [x.strip() for x in c.columns]
    c["workflow_name"] = c.get("workflow_name", "").astype(str).str.strip().str.lower()
    c["baseline"] = c.get("baseline", "").astype(str).map(normalize_baseline)

    if "timestamp" in c.columns:
        c["ts"] = pd.to_datetime(c["timestamp"], errors="coerce")
    else:
        c["ts"] = pd.NaT
    for col in ["pod_age_seconds", "price_per_hour"]:
        if col in c.columns:
            c[col] = pd.to_numeric(c[col], errors="coerce")

    c["task"] = c["pod_name"].apply(task_from_podname)
    mask_missing_wf = (c["workflow_name"] == "") | (c["workflow_name"].isna())
    if mask_missing_wf.any():
        c.loc[mask_missing_wf, "workflow_name"] = c.loc[mask_missing_wf, "task"].map(task_to_wf).fillna("unknown")
    c = c[c["workflow_name"] != "unknown"].copy()

    # Assign machine IDs: 3 pods share 1 machine
    # Build pod_index mapping for each (workflow, task) group, then assign machine_id
    # Using iterative approach to avoid pandas groupby.apply returning DataFrame issue
    c["machine_id"] = 1  # Initialize with default
    for (wf, task), group_idx in c.groupby(["workflow_name", "task"]).groups.items():
        group_pods = sorted(c.loc[group_idx, "pod_name"].unique())
        pod_index = {p: i for i, p in enumerate(group_pods)}
        for idx in group_idx:
            pod_name = c.loc[idx, "pod_name"]
            pod_idx = pod_index.get(pod_name, 0)
            c.loc[idx, "machine_id"] = (pod_idx // 3) + 1
    c["machine_id"] = c["machine_id"].astype(int)

    windows = evals_df.groupby(["workflow_name", "baseline"], as_index=False).agg(
        t0=("start_time", "min"), t1=("end_time", "max")
    )
    windows["t0_dt"] = pd.to_datetime(windows["t0"], unit="s", errors="coerce")
    windows["t1_dt"] = pd.to_datetime(windows["t1"], unit="s", errors="coerce")

    totals = []
    for wf, base, t0, t1, t0d, t1d in windows[["workflow_name", "baseline", "t0", "t1", "t0_dt", "t1_dt"]].itertuples(index=False):
        d = c[(c["workflow_name"] == wf) & (c["baseline"] == base)]
        if d.empty:
            totals.append((wf, base, 0.0))
            continue
        if d["ts"].notna().any() and pd.notna(t0d) and pd.notna(t1d):
            di = d[(d["ts"] >= t0d) & (d["ts"] <= t1d)].copy()
            if di.empty:
                di = d.copy()
        else:
            di = d.copy()
        di = di.sort_values(["pod_name", "ts"], kind="mergesort")
        di["prev_age"] = di.groupby("pod_name")["pod_age_seconds"].shift(1)
        di["delta_age_sec"] = (di["pod_age_seconds"] - di["prev_age"]).clip(lower=0)
        if "check_interval_seconds" in di.columns:
            sel = di["delta_age_sec"].isna() | (di["delta_age_sec"] == 0)
            di.loc[sel, "delta_age_sec"] = di.loc[sel, "check_interval_seconds"].fillna(0)
        else:
            di["delta_age_sec"] = di["delta_age_sec"].fillna(0)

        # Machine packing: 3 pods per machine - only charge once per machine
        # For each (task, machine_id), take the max delta_age_sec across pods sharing that machine
        # Then charge for the machine time, not individual pod times
        di["price_per_hour"] = pd.to_numeric(di["price_per_hour"], errors="coerce").fillna(0)

        # Compute machine-level stats: prefer lifetime = max_age - min_age across pods
        # Fallback to max_delta_sec when age values are missing or non-informative.
        # Compute machine-level stats: prefer lifetime = max_age - min_age across pods
        # Fallback to max_delta_sec when age values are missing or non-informative.
        machine_stats = di.groupby(["task", "machine_id"], observed=False).agg(
            min_age=("pod_age_seconds", "min"),
            max_age=("pod_age_seconds", "max"),
            max_delta_sec=("delta_age_sec", "max")
        ).reset_index()

        # Choose the price from the oldest observed pod on the machine (the row with max_age)
        price_map = (
            di.sort_values(["task", "machine_id", "pod_age_seconds"], ascending=[True, True, False])
              .groupby(["task", "machine_id"], observed=False)
              .first()
              .reset_index()[["task", "machine_id", "price_per_hour"]]
        )
        machine_stats = machine_stats.merge(price_map, on=["task", "machine_id"], how="left")
        machine_stats["price_per_hour"] = pd.to_numeric(machine_stats["price_per_hour"], errors="coerce").fillna(0)

        # Simplified VM lifetime: use the oldest pod's age (max_age) as machine lifetime
        # If max_age is missing or non-positive, fall back to the previous max per-pod delta.
        machine_stats["machine_lifetime_sec"] = machine_stats["max_age"].fillna(np.nan)

        # Use only the oldest pod's age as the machine lifetime (no fallback)
        machine_stats["effective_sec"] = machine_stats["machine_lifetime_sec"].fillna(0)

        # Convert seconds to hours and compute cost
        machine_stats["spot_hours"] = machine_stats["effective_sec"] / 3600.0
        machine_stats["cost"] = machine_stats["price_per_hour"] * machine_stats["spot_hours"]

        totals.append((wf, base, float(machine_stats["cost"].sum())))
    res = pd.DataFrame(totals, columns=["workflow_name", "baseline", "total_cost"])
    present_wf = evals_df["workflow_name"].unique()
    res = res[res["workflow_name"].isin(present_wf)]
    return res

def get_total_cost_from_db(workflow_name: str, baseline: str) -> Dict[str, Optional[float]]:
    """
    Get total cost from the cost_logs table in the database.
    Uses compute_cost_per_wf_baseline for machine packing cost.
    
    Steps:
    1. Load eval windows from serverless_workflows
    2. Load cost_logs for workflow/baseline
    3. Use compute_cost_per_wf_baseline to calculate total cost
    
    Returns dict with:
        - total_cost: Total cost spent during experiment
        - avg_price_per_hour: Average spot price per hour (per machine)
        - sample_count: Number of cost log entries
        - total_runtime_seconds: Total experiment runtime in real seconds
        - normalized_cost: Cost relative to budget
    """
    result = {
        'total_cost': None,
        'avg_price_per_hour': None,
        'sample_count': 0,
        'total_runtime_seconds': None,
        'normalized_cost': None
    }
    
    try:
        import pandas as pd
        conn = mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        # =====================================================================
        # STEP 1: Load eval windows from serverless_workflows
        # =====================================================================
        eval_query = """
            SELECT workflow_name, baseline, start_time, end_time
            FROM serverless_workflows
            WHERE workflow_name = %s AND baseline = %s
        """
        eval_params = [workflow_name, baseline]
        if EXPERIMENT_TAG:
            eval_query += " AND notes = %s"
            eval_params.append(EXPERIMENT_TAG)
        cursor.execute(eval_query, tuple(eval_params))
        eval_rows = cursor.fetchall()
        evals_df = pd.DataFrame(eval_rows)
        if not evals_df.empty:
            evals_df["workflow_name"] = evals_df["workflow_name"].astype(str).str.strip().str.lower()
            evals_df["baseline"] = evals_df["baseline"].astype(str).map(normalize_baseline)
        else:
            # Fallback: create a windowless row so we still compute over all cost logs
            evals_df = pd.DataFrame([{
                "workflow_name": str(workflow_name).strip().lower(),
                "baseline": normalize_baseline(baseline),
                "start_time": None,
                "end_time": None
            }])
            print("⚠️ No serverless_workflows rows found; using unbounded cost window.")

        # =====================================================================
        # STEP 2: Load cost_logs
        # =====================================================================
        cost_query = """
            SELECT
                workflow_name,
                baseline,
                pod_name,
                pod_age_seconds,
                price_per_hour,
                check_interval_seconds,
                timestamp
            FROM cost_logs
            WHERE workflow_name = %s AND baseline = %s
        """
        cost_params = [workflow_name, baseline]
        if EXPERIMENT_TAG:
            cost_query += " AND notes = %s"
            cost_params.append(EXPERIMENT_TAG)
        cost_query += " ORDER BY pod_name, timestamp"
        cursor.execute(cost_query, tuple(cost_params))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        print(f"📊 Found {len(rows)} cost_logs records for {workflow_name}/{baseline}")
        
        if not rows:
            print(f"⚠️ No cost data found in DB for {workflow_name}/{baseline}")
            return result
        
        cost_df = pd.DataFrame(rows)
        sample_count = len(cost_df)
        workflow_name_norm = str(workflow_name).strip().lower()
        baseline_norm = normalize_baseline(baseline)

        # Build task -> workflow mapping (for missing workflow_name in cost_logs)
        task_to_wf = {}
        tasks_path = os.path.join(WORKFLOWS_DIR, workflow_name, "tasks.json")
        if os.path.exists(tasks_path):
            try:
                with open(tasks_path) as f:
                    tasks_data = json.load(f).get("tasks", {})
                task_to_wf = {task: workflow_name_norm for task in tasks_data.keys()}
            except Exception:
                task_to_wf = {}
        if not task_to_wf:
            task_to_wf = {task_from_podname(p): workflow_name_norm
                          for p in cost_df["pod_name"].dropna().unique()}

        cost_summary = compute_cost_per_wf_baseline(cost_df, evals_df, task_to_wf, wf_minscale={})
        if cost_summary.empty:
            print("⚠️ compute_cost_per_wf_baseline returned no cost rows.")
            return result
        match = cost_summary[
            (cost_summary["workflow_name"] == workflow_name_norm) &
            (cost_summary["baseline"] == baseline_norm)
        ]
        if match.empty:
            print(f"⚠️ No cost row matched for {workflow_name}/{baseline}.")
            return result

        total_cost = float(match["total_cost"].iloc[0])
        avg_price = float(pd.to_numeric(cost_df.get("price_per_hour"), errors="coerce").mean()) if "price_per_hour" in cost_df else 0.0

        total_runtime_sec = None
        try:
            t0 = pd.to_numeric(evals_df["start_time"], errors="coerce").min()
            t1 = pd.to_numeric(evals_df["end_time"], errors="coerce").max()
            if pd.notna(t0) and pd.notna(t1):
                total_runtime_sec = float(max(t1 - t0, 0))
        except Exception:
            total_runtime_sec = None
        
        result['total_cost'] = total_cost
        result['avg_price_per_hour'] = avg_price
        result['sample_count'] = sample_count
        result['total_runtime_seconds'] = total_runtime_sec
        
        # Normalized cost: compare to budget (calculated elsewhere)
        # We don't calculate normalized_cost here since it depends on budget
        
        time_filter_msg = "(filtered to experiment window)" if evals_df["start_time"].notna().any() else "(no time filter)"
        print(f"💾 Cost from DB: ${total_cost:.2f} total, ${avg_price:.4f}/machine/hr avg, "
              f"{sample_count} samples {time_filter_msg}")
            
    except Exception as e:
        import traceback
        print(f"⚠️ Error getting cost from database: {e}")
        traceback.print_exc()
    
    return result

def get_budget_for_workflow(workflow_name: str, az: str = "us-west-2a", instance: str = "v100") -> Optional[float]:
    """
    Calculate the budget for a workflow using the same logic as the scaler.
    Budget = 50% of estimated on-demand cost (hard cap, includes scaling/retry/churn).
    Total experiment on-demand cost is ~$60, so total budget cap is ~$30.
    """
    # Workflow runtime estimates (in REAL hours) - must match scaler's WORKFLOW_ESTIMATED_RUNTIME_HOURS
    # Costs use real time (pod_age_seconds), so budget uses real hours
    # UPDATED from run8/run9/run10: actual load generator durations are much longer
    WORKFLOW_ESTIMATED_RUNTIME_HOURS = {
        # Based on actual observed experiment runtimes from run8.log
        # Load generator: 500 requests, concurrency=4, interval=0.25s, up to 60s timeout per request
        'wf-1': 1.00,    # Simple: observed 3539s = 59 min (was 0.25)
        'wf-2': 1.75,    # Simple with branches: observed 6211s = 103 min (was 0.33)
        'wf-3': 1.65,    # Medium: observed 5820s = 97 min (was 0.50)
        'wf-4': 1.85,    # Medium: observed 6527s = 109 min (was 0.50)
        'wf-5': 2.10,    # Complex: observed 7363s = 123 min (was 1.60)
        'wf-6': 1.50,    # Complex: ~90 real minutes estimated
        'wf-7': 1.00,    # Medium: ~60 real minutes estimated
        'wf-8': 1.50,    # Complex: ~90 real minutes estimated
        'wf-9': 2.00,    # Most complex: ~120 real minutes estimated
    }
    
    # Estimated machines per workflow (from tasks.json minscale / 3)
    WORKFLOW_ESTIMATED_MACHINES = {
        'wf-1': 2, 'wf-2': 3, 'wf-3': 4, 'wf-4': 4,
        'wf-5': 11, 'wf-6': 12, 'wf-7': 5, 'wf-8': 14, 'wf-9': 16
    }
    
    # Load on-demand price from spot cost CSV
    try:
        import pandas as pd
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cost_csv_path = os.path.join(script_dir, "Scaler", "spot-cost-csv", f"{az}_{instance}_cost.csv")
        
        if os.path.exists(cost_csv_path):
            df = pd.read_csv(cost_csv_path)
            avg_spot_price = df["SpotPrice ($)"].mean()
            avg_savings = df["Savings (%)"].mean() / 100.0
            # On-demand = spot / (1 - savings)
            ondemand_price = avg_spot_price / (1 - avg_savings) if avg_savings < 1.0 else avg_spot_price * 3
        else:
            # Fallback prices
            ondemand_price = 3.06
    except Exception as e:
        print(f"⚠️ Error loading spot prices: {e}")
        ondemand_price = 3.06
    
    runtime_hours = WORKFLOW_ESTIMATED_RUNTIME_HOURS.get(workflow_name, 0.15)
    estimated_machines = WORKFLOW_ESTIMATED_MACHINES.get(workflow_name, 6)
    
    # Get per-workflow budget percentage (wf-5, wf-9 get 60%, others get 50%)
    budget_percent = WORKFLOW_BUDGET_PERCENT.get(workflow_name, BUDGET_DEFAULT_PERCENT)
    override_percent = get_budget_override_percent(workflow_name)
    if override_percent is not None:
        budget_percent = override_percent
    
    # Budget as percentage of on-demand cost
    ondemand_cost = ondemand_price * runtime_hours * estimated_machines
    budget = ondemand_cost * budget_percent * CHURN_OVERHEAD_FACTOR
    
    return budget

def get_budget_override_percent(workflow_name: str):
    if CHECKSCALE_BUDGET_OVERRIDE_PERCENT is None:
        return None
    if CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS and workflow_name not in CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS:
        return None
    return CHECKSCALE_BUDGET_OVERRIDE_PERCENT

def extract_cost_from_scaler_log(log_path: str) -> Dict[str, Optional[float]]:
    """
    Fallback: Extract cost metrics from scaler log file if DB is unavailable.
    """
    result = {
        'total_cost': None,
        'budget': None,
        'budget_remaining': None,
        'normalized_cost': None,
        'cost_per_pod_hour': None
    }
    
    if not os.path.exists(log_path):
        return result
    
    try:
        with open(log_path, 'r') as f:
            content = f.read()
        
        # Extract budget info: 💵 Budget: $X.XX/$Y.YY ($Z.ZZ remaining)
        budget_pattern = r'💵 Budget: \$([0-9.]+)/\$([0-9.]+) \(\$([0-9.]+) remaining\)'
        budget_matches = re.findall(budget_pattern, content)
        if budget_matches:
            last_match = budget_matches[-1]
            result['total_cost'] = float(last_match[0])
            result['budget'] = float(last_match[1])
            result['budget_remaining'] = float(last_match[2])
        
        # Extract normalized cost from COST-WIN lines
        norm_pattern = r'norm_cost=([0-9.]+)'
        norm_matches = re.findall(norm_pattern, content)
        if norm_matches:
            result['normalized_cost'] = float(norm_matches[-1])
        
        # Check for BUDGET ENVELOPE initialization
        envelope_pattern = r'Budget: \$([0-9.]+) \(([0-9]+)% of on-demand\)'
        envelope_matches = re.findall(envelope_pattern, content)
        if envelope_matches and result['budget'] is None:
            result['budget'] = float(envelope_matches[0][0])
    except Exception as e:
        print(f"⚠️ Error parsing scaler log: {e}")
    
    return result

# --- Global State & Helper Functions ---
background_processes = []
current_experiment = None
_signal_handled = False

def cleanup_processes(cleanup_fission=False, wf_id=None):
    print("\n🔄 Terminating background processes...")
    for proc_info in background_processes:
        try:
            if proc_info['process'] and proc_info['process'].poll() is None:
                os.killpg(os.getpgid(proc_info['process'].pid), signal.SIGTERM)
                time.sleep(2)
                if proc_info['process'].poll() is None:
                    os.killpg(os.getpgid(proc_info['process'].pid), signal.SIGKILL)
                print(f"✅ {proc_info['name']} terminated.")
            
            # Wait for log thread to finish processing remaining output
            if 'log_thread' in proc_info and proc_info['log_thread']:
                proc_info['log_thread'].join(timeout=2)
            
            # Close log file handle and add end timestamp (EDT timezone)
            if 'log_handle' in proc_info and proc_info['log_handle']:
                try:
                    timestamp = _get_edt_timestamp()
                    proc_info['log_handle'].write(f"{'='*60}\n")
                    proc_info['log_handle'].write(f"[{timestamp}] Log ended\n")
                    proc_info['log_handle'].write(f"{'='*60}\n")
                    proc_info['log_handle'].close()
                except:
                    pass
        except Exception as e:
            print(f"⚠️ Error terminating {proc_info['name']}: {e}")
    background_processes.clear()
    
    if cleanup_fission:
        try:
            cmd = [CLEANUP_SCRIPT]
            if wf_id:
                print(f"🧹 Performing targeted cleanup for: {wf_id}")
                cmd.append(wf_id)
            else:
                print("🧹 Performing blanket cleanup...")
            subprocess.run(cmd, check=True, text=True, stderr=subprocess.PIPE)
            print("✅ Fission cleanup successful.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Error during Fission cleanup:\n{e.stderr}")

def signal_handler(signum, _frame):
    global _signal_handled
    if _signal_handled:
        return
    _signal_handled = True
    try:
        signal_name = signal.Signals(signum).name
    except Exception:
        signal_name = str(signum)

    if signum == signal.SIGINT:
        description = "User Interrupt (Ctrl+C)"
    elif signum == signal.SIGTERM:
        description = "System Termination"
    elif signum == signal.SIGHUP:
        description = "Terminal Hangup"
    elif signum == signal.SIGQUIT:
        description = "Quit"
    else:
        description = f"Signal {signal_name}"

    print(f"\n❌ Runner received signal {signal_name} ({signum}). Initiating cleanup...")
    discord_notifier = get_discord_notifier()
    if discord_notifier:
        try:
            exp = current_experiment or "startup"
            discord_notifier.notify_error("Test Runner", f"Signal {signal_name}", f"Received {signal_name} during {exp}")
            discord_notifier.notify_stage("Global", exp, "Runner", f"Received {signal_name}. Cleaning up.")
            discord_notifier.notify_termination(signal_name, description, current_experiment)
        except Exception:
            pass
    cleanup_processes(cleanup_fission=True)
    sys.exit(1)

def _get_edt_timestamp():
    """Get current timestamp in EDT timezone."""
    from datetime import datetime, timezone, timedelta
    # EDT is UTC-4
    edt = timezone(timedelta(hours=-4))
    return datetime.now(edt).strftime("%Y-%m-%d %H:%M:%S EDT")

def _log_output_with_timestamps(pipe, log_handle, name):
    """Thread function to read from pipe and write timestamped lines to log file."""
    try:
        for line in iter(pipe.readline, ''):
            if line:
                timestamp = _get_edt_timestamp()
                log_handle.write(f"[{timestamp}] {line}")
                log_handle.flush()
    except:
        pass
    finally:
        pipe.close()

def run_background(cmd_list, cwd=None, log_file=None, name="Unknown", env=None):
    import threading
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # Use write mode ('w') to get fresh logs each run, not append mode
    log_handle = open(log_file, 'w')
    # Add timestamp header to log file (EDT timezone)
    timestamp = _get_edt_timestamp()
    log_handle.write(f"{'='*60}\n")
    log_handle.write(f"[{timestamp}] Log started\n")
    log_handle.write(f"[{timestamp}] Process: {name}\n")
    log_handle.write(f"[{timestamp}] Command: {' '.join(cmd_list)}\n")
    log_handle.write(f"{'='*60}\n")
    log_handle.flush()
    
    # Start subprocess with pipes for stdout/stderr
    proc = subprocess.Popen(
        cmd_list,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Merge stderr into stdout
        preexec_fn=os.setsid,
        env=env if env is not None else os.environ.copy(),
        text=True,  # Text mode for line reading
        bufsize=1   # Line buffered
    )
    
    # Start thread to read output and write with timestamps
    log_thread = threading.Thread(
        target=_log_output_with_timestamps,
        args=(proc.stdout, log_handle, name),
        daemon=True
    )
    log_thread.start()
    
    background_processes.append({
        'process': proc, 
        'name': name, 
        'log_handle': log_handle,
        'log_thread': log_thread
    })
    return proc


# -----------------------------
# HTTP dry-run helpers
# -----------------------------
def dry_run_function(function_name, workflow_id, test_payload=None, max_retries=3, retry_delay=10):
    print(f"🧪 Dry running function '{function_name}' from workflow '{workflow_id}'...")
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                print(f"🔄 Retry attempt {attempt}/{max_retries} for function '{function_name}' (waiting {retry_delay}s)...")
                time.sleep(retry_delay)

            # Make test request directly to internal router path (service DNS)
            function_url = f"http://router.fission.svc.cluster.local/fission-function/{function_name}"

            if test_payload is None:
                test_payload = {"uid": f"dry-run-test-{int(time.time())}-attempt-{attempt}", "test": True}

            print(f"  -> Testing URL: {function_url}")
            response = requests.post(function_url, json=test_payload, timeout=900, headers={'Content-Type': 'application/json'})  # 15 min timeout
            print(f"  -> Response Status: {response.status_code}")
            print(f"  -> Body (first 200): {response.text[:200]}...")

            if response.status_code == 200:
                print(f"✅ Function '{function_name}' dry run successful{' on retry' if attempt>0 else ''}")
                return True
            else:
                print(f"❌ Function '{function_name}' dry run failed with status {response.status_code}")
                if attempt < max_retries:
                    continue
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for function '{function_name}' (attempt {attempt+1}): {e}")
            if attempt < max_retries:
                continue
            return False
        except Exception as e:
            print(f"❌ Unexpected error during dry run of '{function_name}' (attempt {attempt+1}): {e}")
            if attempt < max_retries:
                continue
            return False
    return False

def dry_run_workflow(workflow_id, tasks_data, max_retries=3, retry_delay=10):
    print(f"🧪 Dry running complete workflow '{workflow_id}'...")
    route_url = f"{ROUTER_ENTRY_URL}/{workflow_id}"

    # For workflows with longer execution times, use longer timeout
    # wf-9 is the longest workflow (~33s path), wf-3/4/5 also have long execution times
    # Complex workflows (wf-5,6,8,9) have deeper DAGs and may need more time
    workflows_with_long_tasks = {'wf-3', 'wf-4', 'wf-5', 'wf-6', 'wf-8', 'wf-9'}
    timeout = 1200 if workflow_id in workflows_with_long_tasks else 900  # 20 min for long/complex tasks, 15 min default
    
    # Find the final/exit tasks in the workflow (tasks with no successors)
    exit_tasks = [task_id for task_id, task_info in tasks_data.items() 
                  if not task_info.get('successors', [])]
    if not exit_tasks:
        exit_tasks = [list(tasks_data.keys())[-1]]  # Fallback to last task

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                print(f"🔄 Retry attempt {attempt}/{max_retries} for workflow '{workflow_id}' (waiting {retry_delay}s)...")
                time.sleep(retry_delay)
            else:
                # On first attempt, wait a bit for pods to be ready (especially after checkpoint rollouts)
                if workflow_id in workflows_with_long_tasks:
                    print(f"  ⏳ Waiting 10s for pods to stabilize (long-running workflow)...")
                    time.sleep(10)

            payload = {"uid": f"workflow-dry-run-{int(time.time())}-attempt-{attempt}", "test": True}
            print(f"  -> Testing URL: {route_url}")
            response = requests.post(route_url, json=payload, timeout=timeout, headers={'Content-Type': 'application/json'})
            print(f"  -> Response Status: {response.status_code}")
            print(f"  -> Body (first 200): {response.text[:200]}...")

            if "taskX" in response.text:
                print(f"⚠️ Response contains 'taskX' -> cleanup+redeploy {workflow_id} and retry from scratch.")
                cleanup_processes(cleanup_fission=True, wf_id=workflow_id)
                try:
                    deploy_script = os.path.join(WORKFLOWS_DIR, workflow_id, f"deploy_{BASELINE}.sh")
                    subprocess.run([deploy_script], cwd=os.path.join(WORKFLOWS_DIR, workflow_id), check=True, text=True, stderr=subprocess.PIPE)
                    print("✅ Redeployment completed; waiting 30s for stabilization...")
                    time.sleep(30)
                    return dry_run_workflow(workflow_id, tasks_data, max_retries, retry_delay)
                except subprocess.CalledProcessError as e:
                    print(f"❌ Redeploy failed: {e.stderr}")
                    return False

            if response.status_code == 200:
                print(f"✅ Complete workflow '{workflow_id}' dry run successful{' on retry' if attempt>0 else ''}")
                return True
            
            # Check if workflow actually completed even with non-200 status code
            # This happens when Fission returns 500 but all tasks actually succeeded
            # Look for "completed" messages for exit tasks in the response body
            response_text = response.text.lower()
            exit_task_completed = any(f"{task} completed" in response_text for task in exit_tasks)
            first_task_completed = "task1 completed" in response_text
            
            if response.status_code >= 500 and first_task_completed:
                # HTTP 500 but tasks are running - check if workflow actually completed
                # For chained functions, 500 can happen even when workflow succeeds
                if exit_task_completed:
                    print(f"✅ Workflow '{workflow_id}' completed successfully (exit task in response body)")
                    print(f"   Note: HTTP {response.status_code} is misleading - workflow execution succeeded")
                    return True
                else:
                    # First task completed but exit task not in response - partial execution
                    # This is likely a real failure mid-chain, retry
                    print(f"⚠️ Workflow started (task1 completed) but exit task not found - retrying...")
            else:
                print(f"❌ Workflow dry run failed with status {response.status_code}")
            
            # For 500 errors, wait longer before retry (pods might still be starting)
            if response.status_code >= 500 and attempt < max_retries:
                extra_wait = 15 if workflow_id in workflows_with_long_tasks else 10
                print(f"  ⏳ HTTP {response.status_code} error - waiting {extra_wait}s for pods to recover...")
                time.sleep(extra_wait)
            if attempt < max_retries:
                continue
            return False

        except requests.exceptions.Timeout:
            print(f"⏱️ Workflow dry run timed out after {timeout}s (attempt {attempt+1})")
            if attempt < max_retries:
                # For timeout, wait longer before retry
                timeout_wait = 20 if workflow_id in workflows_with_long_tasks else 15
                print(f"  ⏳ Waiting {timeout_wait}s before retry...")
                time.sleep(timeout_wait)
                continue
            return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Workflow request failed (attempt {attempt+1}): {e}")
            if attempt < max_retries:
                # For network errors, wait a bit before retry
                network_wait = 10 if workflow_id in workflows_with_long_tasks else 5
                time.sleep(network_wait)
                continue
            return False
        except Exception as e:
            print(f"❌ Unexpected error during workflow dry run (attempt {attempt+1}): {e}")
            if attempt < max_retries:
                continue
            return False

    return False

# -----------------------------
# K8s helpers for deployments & configmaps
# -----------------------------
def cm_dns_name(wf_id, task_id, baseline="ours"):
    raw = f"{wf_id}-{task_id}-{baseline}-cfg".lower().replace("_","-")
    raw = re.sub(r'[^a-z0-9\.-]+', '-', raw)
    raw = re.sub(r'^[^a-z0-9]+','', raw)
    raw = re.sub(r'[^a-z0-9]+$','', raw)
    return raw[:63] or "x"

def patch_configmap_key(namespace, cm_name, key, value):
    api = client.CoreV1Api()
    body = {"data": {key: value}}
    api.patch_namespaced_config_map(name=cm_name, namespace=namespace, body=body)
    print(f"  -> Patched ConfigMap {cm_name}: {key}={value}")

def rollout_restart_task(namespace, task_id):
    """
    Ensure pods pick up env from ConfigMap (Fission injects CM as env -> needs restart).
    Uses Python Kubernetes API to avoid jsonpath issues with kubectl.
    """
    try:
        apps = client.AppsV1Api()
        # Wait a bit for deployment to be created if it doesn't exist yet
        max_retries = 3
        dep_list = []
        for retry in range(max_retries):
            dep_list = apps.list_namespaced_deployment(namespace, label_selector=f"functionName={task_id}").items
            if dep_list:
                break
            if retry < max_retries - 1:
                print(f"  ⏳ Deployment for {task_id} not found yet, waiting... (retry {retry+1}/{max_retries})")
                time.sleep(5)
        
        if not dep_list:
            print(f"  ⚠️ No deployment found for task {task_id} in ns={namespace} after {max_retries} retries")
            return
        
        dep_name = dep_list[0].metadata.name
        deployment = dep_list[0]
        initial_generation = deployment.metadata.generation
        print(f"  -> Rolling out restart for deploy/{dep_name} ...")
        
        # Trigger rollout restart using kubectl (this is fine)
        # The jsonpath issue happens in 'rollout status', not 'rollout restart'
        try:
            subprocess.run(['kubectl', '-n', namespace, 'rollout', 'restart', f'deployment/{dep_name}'], 
                         check=True, text=True, capture_output=True, timeout=10)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ⚠️ Error triggering rollout restart: {e}")
            return
        
        # Wait for rollout to complete using Python API (avoids jsonpath issues)
        print(f"  ⏳ Waiting for rollout to complete...")
        max_wait = 180
        wait_interval = 3
        waited = 0
        initial_generation = deployment.metadata.generation
        
        while waited < max_wait:
            try:
                current_deployment = apps.read_namespaced_deployment(dep_name, namespace)
                # Check if rollout is complete
                if (current_deployment.status.observed_generation is not None and 
                    current_deployment.metadata.generation <= current_deployment.status.observed_generation):
                    # Check if all replicas are ready
                    if (current_deployment.status.ready_replicas is not None and
                        current_deployment.status.ready_replicas == current_deployment.spec.replicas):
                        print(f"  ✅ Rollout complete for {task_id}")
                        return
            except client.rest.ApiException as e:
                if e.status == 404:
                    print(f"  ⚠️ Deployment {dep_name} not found during status check")
                    return
                # Continue waiting on other API errors
            
            time.sleep(wait_interval)
            waited += wait_interval
            if waited % 30 == 0:
                print(f"  ⏳ Still waiting for rollout... ({waited}s/{max_wait}s)")
        
        print(f"  ⚠️ Timeout waiting for rollout to complete for {task_id} (waited {max_wait}s)")
        
    except Exception as e:
        print(f"  ⚠️ Rollout restart error for {task_id}: {e}")

# Retry service functions removed - now runs as subprocess

def verify_and_repair_deployments(workflow_id, tasks_data):
    print("🔎 Verifying and repairing deployments if necessary...")
    discord_notifier = get_discord_notifier()
    if discord_notifier: 
        discord_notifier.notify_stage(BASELINE, workflow_id, "Verification", "Checking for correct environment variables...")

    try:
        config.load_kube_config()
        api = client.AppsV1Api()
        
        for task_id in tasks_data.keys():
            print(f"  -> Verifying '{task_id}'...")
            deployments = api.list_namespaced_deployment(NAMESPACE, label_selector=f"functionName={task_id}").items
            if not deployments:
                raise RuntimeError(f"Could not find deployment for function '{task_id}'.")
            print(f"     ✅ '{task_id}' deployment present.")
        print("✅ All deployments verified.")
        if discord_notifier: 
            discord_notifier.notify_stage(BASELINE, workflow_id, "Verification", "All deployments present.")
        return True

    except Exception as e:
        print(f"❌ FATAL: Deployment verification failed: {e}")
        if discord_notifier: 
            discord_notifier.notify_error(BASELINE, workflow_id, f"Deployment verification failed: {e}")
        return False

# -----------------------------
# Checkpointer integration
# -----------------------------
def run_checkpointer(workflows_dir, workflow, baseline="ours"):
    """
    Run the offline checkpointer to determine optimal checkpoint placement.
    Returns tuple of (list of task IDs, dict of task_id -> checkpoint_level).
    
    Checkpoint levels (state machine-inspired):
    - Level 1 (Standard): Normal recovery points
    - Level 2 (Critical): After expensive computations or late-stage tasks
    - Level 3 (Saga): At branch points for potential compensation/rollback
    """
    cmd = [
        "python3", CHECKPOINTER_SCRIPT,
        "--workflows-dir", workflows_dir,
        "--workflow", workflow,
        "--baseline", baseline,
    ]
    print(f"  -> Running checkpointer: {' '.join(cmd)}")
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)

    # Parse plain-text output: sections "CRITICAL tasks (N):", "MEDIUM tasks (N):", "LOW tasks (N):"
    checkpoint_tasks = []
    checkpoint_levels = {}
    level_name_to_int = {"CRITICAL": 3, "MEDIUM": 2, "LOW": 1}
    current_level = None
    for line in res.stdout.splitlines():
        line = line.strip()
        for level_name in level_name_to_int:
            if line.startswith(f"{level_name} tasks"):
                current_level = level_name
                break
        else:
            if current_level and line:
                task_id = line
                level_int = level_name_to_int[current_level]
                checkpoint_levels[task_id] = level_int
                if current_level in ("CRITICAL", "MEDIUM"):
                    checkpoint_tasks.append(task_id)
                    print(f"  -> 📍 {task_id}: checkpoint level {level_int} ({current_level})")

    print(f"  -> Checkpoint tasks: {checkpoint_tasks}")
    return checkpoint_tasks, checkpoint_levels

def apply_checkpoint_plan_to_configmaps(workflow, baseline, checkpoint_tasks, namespace="default", checkpoint_levels=None):
    """
    Apply checkpoint plan to ConfigMaps with state machine-inspired checkpoint levels.
    
    Args:
        workflow: Workflow ID
        baseline: Baseline name
        checkpoint_tasks: List of task IDs to checkpoint
        namespace: Kubernetes namespace
        checkpoint_levels: Dict of task_id -> checkpoint_level (1=standard, 2=critical, 3=saga)
    """
    print(f"🧠 Applying checkpoint plan to ConfigMaps (wf={workflow}, baseline={baseline})")
    config.load_kube_config()
    
    # Track which rollouts we've initiated
    rolled_out_tasks = []
    checkpoint_levels = checkpoint_levels or {}
    
    # Count checkpoint levels for logging
    level_counts = {1: 0, 2: 0, 3: 0}
    
    # Flip IS_CHECKPOINT/CKPT_LEVEL for selected tasks and restart them
    for t in checkpoint_tasks:
        cm = cm_dns_name(workflow, t, baseline)
        # Use checkpoint level from state machine analysis, default to 1
        level = checkpoint_levels.get(t, 1)
        level_counts[level] = level_counts.get(level, 0) + 1
        
        patch_configmap_key(namespace, cm, "IS_CHECKPOINT", "true")
        patch_configmap_key(namespace, cm, "CKPT_LEVEL", str(level))
        rollout_restart_task(namespace, t)
        rolled_out_tasks.append(t)
    
    # Log checkpoint level distribution
    if any(level_counts[l] > 0 for l in [2, 3]):
        print(f"   📊 Checkpoint levels: standard={level_counts[1]}, critical={level_counts[2]}, saga={level_counts[3]}")
    
    # Wait for all checkpoint tasks to stabilize after rollout
    # This is critical for all workflows - pods need time to restart and be ready
    if rolled_out_tasks:
        print(f"⏳ Waiting for all checkpoint task rollouts to complete: {rolled_out_tasks}")
        
        # Verify all rollouts are actually complete using K8s API
        apps = client.AppsV1Api()
        all_ready = False
        max_wait = 180  # 3 minutes max
        wait_interval = 3
        waited = 0
        
        while waited < max_wait and not all_ready:
            all_ready = True
            for task_id in rolled_out_tasks:
                try:
                    deployments = apps.list_namespaced_deployment(namespace, label_selector=f"functionName={task_id}").items
                    if not deployments:
                        all_ready = False
                        break
                    
                    deployment = deployments[0]
                    # Check if rollout is complete
                    if (deployment.status.observed_generation is None or 
                        deployment.metadata.generation > deployment.status.observed_generation):
                        all_ready = False
                        break
                    
                    # Check if all replicas are ready
                    if (deployment.status.ready_replicas is None or
                        deployment.status.ready_replicas != deployment.spec.replicas):
                        all_ready = False
                        break
                except Exception as e:
                    print(f"  ⚠️ Error checking rollout status for {task_id}: {e}")
                    all_ready = False
                    break
            
            if all_ready:
                print(f"✅ All checkpoint task rollouts completed ({waited}s)")
                break
            
            time.sleep(wait_interval)
            waited += wait_interval
            if waited % 15 == 0:
                print(f"  ⏳ Still waiting for rollouts... ({waited}s/{max_wait}s)")
        
        if not all_ready:
            print(f"  ⚠️ Timeout waiting for rollouts to complete (waited {max_wait}s), but continuing...")
        
        # Additional stabilization wait for all workflows (pods need time to be ready to serve traffic)
        # For longest workflows (wf-9), wait longer since they have more tasks and longer execution times
        longest_workflows = {'wf-9'}  # wf-9 has longest path (~33s) and most tasks
        wait_time = 20 if workflow in longest_workflows else 10
        print(f"⏳ Waiting {wait_time}s for pods to be ready to serve traffic...")
        time.sleep(wait_time)

# -----------------------------
# Dry-run mode driver
# -----------------------------
def handle_dry_run(args, discord_notifier):
    print(f"\n{'='*20} DRY RUN MODE {'='*20}")
    if args.function and not args.workflow:
        print("❌ Error: --function requires --workflow to be specified")
        return 1
    try:
        if args.function and args.workflow:
            print(f"🧪 Testing specific function '{args.function}' in workflow '{args.workflow}'...")
            tasks_path = os.path.join(WORKFLOWS_DIR, args.workflow, "tasks.json")
            if not os.path.exists(tasks_path):
                print(f"❌ Error: Workflow '{args.workflow}' not found or tasks.json missing")
                return 1
            with open(tasks_path) as f:
                tasks_data = json.load(f)["tasks"]
            if args.function not in tasks_data:
                print(f"❌ Error: Function '{args.function}' not found in workflow '{args.workflow}'")
                return 1
            success = dry_run_function(args.function, args.workflow, max_retries=DRY_RUN_MAX_RETRIES, retry_delay=DRY_RUN_RETRY_DELAY)
            return 0 if success else 1

        elif args.workflow:
            print(f"🧪 Testing specific workflow '{args.workflow}'...")
            tasks_path = os.path.join(WORKFLOWS_DIR, args.workflow, "tasks.json")
            if not os.path.exists(tasks_path):
                print(f"❌ Error: Workflow '{args.workflow}' not found or tasks.json missing")
                return 1
            with open(tasks_path) as f:
                tasks_data = json.load(f)["tasks"]
            success = dry_run_workflow(args.workflow, tasks_data, DRY_RUN_MAX_RETRIES, DRY_RUN_RETRY_DELAY)
            return 0 if success else 1

        else:
            print("🧪 Testing all deployed workflows...")
            workflows_to_test = sorted([wf for wf in os.listdir(WORKFLOWS_DIR) if wf.startswith("wf-")])
            all_passed = True
            for wf in workflows_to_test:
                print(f"\n{'='*10} Testing {wf} {'='*10}")
                tasks_path = os.path.join(WORKFLOWS_DIR, wf, "tasks.json")
                if not os.path.exists(tasks_path):
                    print(f"⚠️ Skipping {wf} - tasks.json not found")
                    continue
                with open(tasks_path) as f:
                    tasks_data = json.load(f)["tasks"]
                if not dry_run_workflow(wf, tasks_data, DRY_RUN_MAX_RETRIES, DRY_RUN_RETRY_DELAY):
                    all_passed = False
            print("\n✅ All dry runs passed!" if all_passed else "\n❌ Some dry runs failed!")
            return 0 if all_passed else 1

    except Exception as e:
        print(f"❌ Error during dry run: {e}")
        if discord_notifier:
            discord_notifier.notify_error("Dry Run", "FATAL", str(e))
        return 1

# -----------------------------
# Retry Service (now runs as subprocess)
# -----------------------------


# -----------------------------
# Main experiment driver
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description='Test runner for FaaS-on-Spot experiments (checkscale: offline checkpointer + retry prioritization)')
    parser.add_argument('--dry-run', action='store_true', help='Perform dry run tests without full experiments')
    parser.add_argument('--workflow', type=str, help='Specific workflow to dry run (e.g., wf-1)')
    parser.add_argument('--function', type=str, help='Specific function to dry run (requires --workflow)')
    parser.add_argument('--workflows', nargs='*', default=None,
                        help='Workflows to run (e.g. wf-1 wf-5 wf-9). Default: all wf-* with tasks.json')
    args = parser.parse_args()
    
    if IGNORE_SIGHUP:
        try:
            signal.signal(signal.SIGHUP, signal.SIG_IGN)
            print("🧯 SIGHUP ignored (TEST_RUNNER_IGNORE_SIGHUP=1)")
        except Exception:
            pass

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
        try:
            signal.signal(sig, signal_handler)
        except Exception:
            pass
    if not IGNORE_SIGHUP:
        try:
            signal.signal(signal.SIGHUP, signal_handler)
        except Exception:
            pass
    
    discord_notifier = init_discord_notifications(config_file=resolve_discord_config_path())
    if args.dry_run:
        sys.exit(handle_dry_run(args, discord_notifier))

    all_workflows = sorted([d for d in os.listdir(WORKFLOWS_DIR)
                            if d.startswith("wf-") and os.path.exists(os.path.join(WORKFLOWS_DIR, d, "tasks.json"))])
    _excluded_workflows = {'wf-4', 'wf-5'}
    _default_workflows = [wf for wf in all_workflows if wf not in _excluded_workflows]
    workflows_to_run = args.workflows if args.workflows else _default_workflows
    if not workflows_to_run:
        workflows_to_run = _default_workflows
    if all_workflows:
        invalid = [wf for wf in workflows_to_run if wf not in all_workflows]
        if invalid:
            print(f"⚠️ Unknown workflow(s) {invalid}; available: {all_workflows}. Skipping invalid.")
        workflows_to_run = [wf for wf in workflows_to_run if wf in all_workflows]
    if not workflows_to_run:
        workflows_to_run = ['wf-4', 'wf-5']
    total_experiments = len(workflows_to_run)
    current_experiment_count = 0
    start_time = time.time()
    
    print(f"\n{'='*20} PERFORMING INITIAL BLANKET CLEANUP {'='*20}")
    print(f"🎯 Target success rates: Simple workflows = {SIMPLE_WORKFLOW_TARGET_SUCCESS_RATE}%, Complex workflows = {COMPLEX_WORKFLOW_TARGET_SUCCESS_RATE}%")
    print(f"🏷️ Experiment tag: {EXPERIMENT_TAG}")
    ensure_metadata_columns()
    if discord_notifier: discord_notifier.notify_stage("Global", "Global", "Initial Cleanup", "Starting blanket cleanup of all resources.")
    cleanup_processes(cleanup_fission=True)
    
    try:
        for wf in workflows_to_run:
            current_experiment_count += 1
            global current_experiment
            current_experiment = f"{BASELINE}_{wf}"
            print(f"\n{'='*20} Running Experiment {current_experiment_count}/{total_experiments}: {current_experiment} {'='*20}")
            
            if discord_notifier:
                discord_notifier.notify_experiment_start(BASELINE, wf)
                discord_notifier.notify_progress(current_experiment_count, total_experiments, BASELINE, wf)

            # Define log paths before try block so they're accessible in finally
            scaler_log = os.path.join(os.path.dirname(SCALER_SCRIPT), "logs", f"scaler_{current_experiment}.log")
            experiment_duration = 0.0

            try:
                tasks_path = os.path.join(WORKFLOWS_DIR, wf, "tasks.json")
                with open(tasks_path) as f:
                    tasks_data = json.load(f)["tasks"]

                # 1) Deploy the base functions (CMs contain IS_CHECKPOINT=false by default)
                print(f"🚀 Deploying base functions for {wf}...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Deploy", "Deploying base Fission functions (ConfigMap-driven).")
                deploy_script = os.path.join(WORKFLOWS_DIR, wf, f"deploy_{BASELINE}.sh")
                subprocess.run([deploy_script], cwd=os.path.join(WORKFLOWS_DIR, wf), check=True, text=True, stderr=subprocess.PIPE)

                # 2) Verify deployments exist
                if not verify_and_repair_deployments(wf, tasks_data):
                    cleanup_processes(cleanup_fission=True, wf_id=wf)
                    continue

                # 2.25) Tag all tasks with experiment metadata
                print(f"🏷️ Applying experiment tag to ConfigMaps: {EXPERIMENT_TAG}")
                for t in tasks_data.keys():
                    cm = cm_dns_name(wf, t, BASELINE)
                    patch_configmap_key(NAMESPACE, cm, "EXPERIMENT_TAG", EXPERIMENT_TAG)

                # 2.5) Run the offline checkpointer and flip CMs accordingly (then rollout-restart those tasks)
                ck_tasks, ck_levels = [], {}
                if CHECKSCALE_DISABLE_CHECKPOINTER:
                    print("🧠 Checkpointer disabled (ablation): skipping checkpoint planning")
                    if discord_notifier:
                        discord_notifier.notify_stage(BASELINE, wf, "Checkpointer", "Disabled (ablation).")
                else:
                    print("🧠 Running checkpointer and marking checkpoint tasks...")
                    ck_tasks, ck_levels = run_checkpointer(WORKFLOWS_DIR, wf, baseline=BASELINE)
                    print(f"   -> Checkpoint tasks: {ck_tasks or '[]'}")
                    if ck_tasks:
                        apply_checkpoint_plan_to_configmaps(wf, BASELINE, ck_tasks, namespace=NAMESPACE, checkpoint_levels=ck_levels)
                    else:
                        print("   -> No checkpoint tasks selected by planner.")
                    if discord_notifier:
                        discord_notifier.notify_stage(BASELINE, wf, "Checkpointer", "Checkpoint determination complete")

                # 2.6) Retry service will be started as background process later (after emulator/scaler)

                # 3) Optional workflow dry run
                # For workflows with longer execution times (wf-3, wf-4, wf-5), use longer retry delays
                # These workflows have tasks that take 2-4 seconds each, so pods need more time to stabilize
                # wf-9 is the longest workflow (~33s path), wf-3/4/5 also have long execution times
                workflows_with_long_tasks = {'wf-3', 'wf-4', 'wf-5', 'wf-9'}
                dry_run_retry_delay = DRY_RUN_RETRY_DELAY * 2 if wf in workflows_with_long_tasks else DRY_RUN_RETRY_DELAY
                
                print(f"🧪 Performing dry run test for {wf}...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Dry Run", "Testing complete workflow execution.")
                if not dry_run_workflow(wf, tasks_data, DRY_RUN_MAX_RETRIES, dry_run_retry_delay):
                    print(f"⚠️ Dry run failed for {wf}. Continuing anyway...")
                    if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Dry Run", "Failed, continuing.")
                else:
                    print(f"✅ Dry run passed for {wf}")
                    if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Dry Run", "Success.")

                # Short delay so all task logs from the dry run are committed before we measure success rate
                time.sleep(5)
                # 3.5) Success rate verification with retries
                print(f"📊 Checking success rate (pre-flight verification)...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Success Rate Check", "Verifying deployment.")
                success_rate_verified = False
                for retry_attempt in range(SUCCESS_RATE_MAX_RETRIES + 1):
                    try:
                        if retry_attempt > 0:
                            print(f"🔄 Success rate retry {retry_attempt}/{SUCCESS_RATE_MAX_RETRIES} ... waiting {SUCCESS_RATE_RETRY_DELAY}s")
                            time.sleep(SUCCESS_RATE_RETRY_DELAY)
                        
                        eval_cmd = [
                            "python3", EVAL_SCRIPT,
                            "--workflow", wf,
                            "--baseline", BASELINE,
                            "--use-paths",
                            "--workflows-dir", WORKFLOWS_DIR,
                            "--experiment-tag", EXPERIMENT_TAG
                        ]
                        result = subprocess.run(eval_cmd, capture_output=True, text=True, check=True)
                        success_rate = None
                        for line in result.stdout.split('\n'):
                            if "Success Rate:" in line:
                                success_rate = float(line.split(":")[1].strip().rstrip('%'))
                                break

                        if success_rate is not None:
                            # Pre-flight: we only have a few UIDs from the dry run(s). Accept if at least one
                            # full path succeeded (success_rate > 0) or if we meet the usual lenient threshold.
                            target_success_rate = get_target_success_rate(wf)
                            verification_threshold = target_success_rate * 0.9  # 90% of target
                            # After dry run passed, require at least one valid path (success_rate > 0) to proceed
                            preflight_ok = success_rate > 0 and (success_rate >= verification_threshold or success_rate >= 10.0)
                            print(f"📊 Success rate {success_rate:.1f}% (target: {target_success_rate:.1f}%, verification threshold: {verification_threshold:.1f}%)")
                            if not preflight_ok:
                                print(f"❌ Below verification threshold ({success_rate:.1f}% < {verification_threshold:.1f}%); redeploying if retries remain...")
                                if discord_notifier:
                                    discord_notifier.notify_stage(BASELINE, wf, "Success Rate Check",
                                                                 f"Below verification threshold ({success_rate:.1f}% < {verification_threshold:.1f}%).")
                                if retry_attempt < SUCCESS_RATE_MAX_RETRIES:
                                    cleanup_processes(cleanup_fission=True, wf_id=wf)
                                    print(f"🚀 Redeploying base functions for {wf}...")
                                    subprocess.run([deploy_script], cwd=os.path.join(WORKFLOWS_DIR, wf),
                                                   check=True, text=True, stderr=subprocess.PIPE)
                                    if not verify_and_repair_deployments(wf, tasks_data):
                                        continue
                                    # Re-apply checkpoint plan after redeploy (with state machine levels)
                                    if ck_tasks:
                                        apply_checkpoint_plan_to_configmaps(wf, BASELINE, ck_tasks, namespace=NAMESPACE, checkpoint_levels=ck_levels)
                                    # Re-run dry run with appropriate delay for long-running workflows
                                    # wf-9 is the longest workflow (~33s path), wf-3/4/5 also have long execution times
                                    workflows_with_long_tasks = {'wf-3', 'wf-4', 'wf-5', 'wf-9'}
                                    dry_run_retry_delay = DRY_RUN_RETRY_DELAY * 2 if wf in workflows_with_long_tasks else DRY_RUN_RETRY_DELAY
                                    print(f"🔄 Re-running dry run after redeploy...")
                                    _ = dry_run_workflow(wf, tasks_data, DRY_RUN_MAX_RETRIES, dry_run_retry_delay)
                                    continue
                                else:
                                    print(f"❌ Exhausted success rate retries for {wf}")
                                    cleanup_processes(cleanup_fission=True, wf_id=wf)
                                    break
                            else:
                                print(f"✅ Success rate meets verification threshold ({success_rate:.1f}% >= {verification_threshold:.1f}%); proceeding.")
                                if discord_notifier:
                                    discord_notifier.notify_stage(BASELINE, wf, "Success Rate Check", 
                                                                 f"Verified ({success_rate:.1f}% >= {verification_threshold:.1f}%, target: {target_success_rate:.1f}%).")
                                success_rate_verified = True
                                break
                        else:
                            print("⚠️ Could not parse success rate; continuing.")
                            break

                    except subprocess.CalledProcessError as e:
                        print(f"⚠️ Success rate evaluation failed for {wf}: {e.stderr[:300]}")
                        if retry_attempt < SUCCESS_RATE_MAX_RETRIES:
                            print(f"  -> Retry after {SUCCESS_RATE_RETRY_DELAY}s")
                            continue
                        else:
                            print("Continuing with experiment...")
                            break

                if not success_rate_verified:
                    print(f"⚠️ Skipping {wf} due to failed pre-flight verification.")
                    continue

                # 4) Run Ranker
                print(f"📈 Running Ranker for {wf}...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Ranker", "Patching scaling factors.")
                ranker_cmd = ["python3", RANKER_SCRIPT, "--workflow", wf, "--workflows-dir", WORKFLOWS_DIR]
                subprocess.run(ranker_cmd, check=True, text=True, stderr=subprocess.PIPE)
                print("...waiting 30s for deployments to stabilize...")
                time.sleep(30)
                if discord_notifier: discord_notifier.notify_deployment_complete(BASELINE, wf)

                # 5) Start Emulator & Scaler in background
                print("🌀 Starting background services (Emulator & Scaler)...")
                emulator_log = os.path.join(EMULATOR_DIR, "logs", f"emulator_{current_experiment}.log")
                emulator_cmd = [
                    "python3", EMULATOR_SCRIPT,
                    "--instance", TRACE_INSTANCE,
                    "--az", TRACE_AZ,
                    "--baseline", BASELINE,
                    "--workflow", wf
                ]
                emulator_env = os.environ.copy()
                emulator_env["EXPERIMENT_TAG"] = EXPERIMENT_TAG
                run_background(emulator_cmd, cwd=EMULATOR_DIR, log_file=emulator_log, name="Emulator", env=emulator_env)
                if discord_notifier: discord_notifier.notify_emulator_started(BASELINE, wf)
                
                # Enhanced scaler with success rate monitoring and critical task scaling
                # Cost optimization: Add max-scale and cooldown based on workflow type
                # Simple workflows (wf-1,2,3,4,7): Target 95-100% - need aggressive scaling
                # Complex workflows (wf-5,6,8,9): Target 65-70% - deeper DAGs, bottlenecks
                
                # For complex workflows (longer execution, deeper DAGs), use MODERATE limits
                # REDUCED from previous values to prevent runaway costs
                # These limits are now in line with the scaler's hard caps (5x base, 50 max per task)
                if wf in COMPLEX_WORKFLOWS:
                    if wf in ['wf-5', 'wf-9']:  # Longest workflows
                        max_scale_limit = "350"  # Restored for success rate (was 500)
                    elif wf == 'wf-8':
                        max_scale_limit = "300"  # Restored (was 450)
                    elif wf == 'wf-6':
                        max_scale_limit = "280"  # Restored (was 400)
                    else:
                        max_scale_limit = "250"
                    scale_cooldown = "120"
                elif wf in SIMPLE_WORKFLOWS:
                    # Simple workflows: sufficient limits for high targets
                    if wf == 'wf-3':
                        max_scale_limit = "200"  # Restored (was 350)
                    elif wf in ['wf-2', 'wf-4', 'wf-7']:
                        max_scale_limit = "180"  # Restored (was 300)
                    else:  # wf-1
                        max_scale_limit = "150"  # Restored (was 250)
                    scale_cooldown = "180"
                else:
                    max_scale_limit = None
                    scale_cooldown = "60"
                
                scaler_log = os.path.join(os.path.dirname(SCALER_SCRIPT), "logs", f"scaler_{current_experiment}.log")
                scaler_cmd = [
                    "python3", SCALER_SCRIPT,
                    "--workflow", wf,
                    "--survival-curve", SURVIVAL_CURVE_PATH,
                    "--workflows-dir", WORKFLOWS_DIR,
                    "--scale-up-threshold", "0.5",
                    "--scale-down-threshold", "0.8",
                    "--interval", "30",
                    "--success-rate-threshold", str(get_target_success_rate(wf) * 0.90),  # 90% of target
                    "--success-rate-interval", "60",
                    "--critical-tasks-count", "3",
                    "--critical-scale-factor", "2.0",
                    "--scale-cooldown", scale_cooldown,
                    "--baseline", BASELINE,
                    "--az", TRACE_AZ,
                    "--instance", TRACE_INSTANCE
                ]
                # Add max-scale limit only for high-cost workflows to prevent runaway costs
                if max_scale_limit:
                    scaler_cmd.extend(["--max-scale", max_scale_limit])
                    print(f"💰 Cost control enabled for {wf}: max-scale={max_scale_limit}, cooldown={scale_cooldown}s")
                
                # Add RL mode if enabled (contextual bandit for scaling decisions)
                # Cold start and activation settings are workflow-specific in rl_config.py
                if RL_MODE_ENABLED:
                    scaler_cmd.extend(["--rl-mode"])
                    print(f"🎰 RL Adaptive Scaler enabled (workflow-specific cold start from rl_config.py)")
                
                if CHECKSCALE_DISABLE_SCALER:
                    print("🚫 Scaler disabled (ablation): skipping scaler start")
                else:
                    scaler_env = os.environ.copy()
                    scaler_env["EXPERIMENT_TAG"] = EXPERIMENT_TAG
                    if CHECKSCALE_DISABLE_RETRY:
                        scaler_env["CHECKSCALE_RETRY_EXHAUST_DISABLE_ALL"] = "1"
                        scaler_env["CHECKSCALE_RETRY_EXHAUST_DISABLE_MIN_RUNS"] = "0"
                    run_background(scaler_cmd, cwd=os.path.dirname(SCALER_SCRIPT), log_file=scaler_log, name="Scaler", env=scaler_env)
                    if discord_notifier: discord_notifier.notify_scaler_started(BASELINE, wf)
                
                # 5.5) Start Retry Service as background process
                print("🔄 Starting retry service...")
                retry_log = os.path.join(RETRY_SERVICE_DIR, "logs", f"retry_service_{current_experiment}.log")
                os.makedirs(os.path.dirname(retry_log), exist_ok=True)
                
                # Complex workflows need more aggressive retrying (shorter cooldown, faster polling)
                # since they have deeper DAGs and more failure points
                if wf in COMPLEX_WORKFLOWS:
                    retry_poll = "15"      # Faster polling for complex workflows
                    retry_cooldown = "45"  # Shorter cooldown to retry faster
                else:
                    retry_poll = "20"
                    retry_cooldown = "60"
                
                retry_cmd = [
                    "python3", RETRY_SERVICE_SCRIPT,
                    "--baseline", BASELINE,
                    "--workflow", wf,
                    "--poll", retry_poll,
                    "--cooldown", retry_cooldown
                ]
                # Set WORKFLOW_DIR environment variable so retry service can find workflow definitions
                retry_env = os.environ.copy()
                retry_env["WORKFLOW_DIR"] = WORKFLOWS_DIR
                retry_env["EXPERIMENT_TAG"] = EXPERIMENT_TAG
                if CHECKSCALE_DISABLE_CHECKPOINTER:
                    retry_env["CHECKSCALE_DISABLE_CHECKPOINTER_SELECTION"] = "1"
                if CHECKSCALE_DISABLE_RETRY:
                    print("🚫 Retry service disabled (ablation): skipping retry service start")
                else:
                    run_background(retry_cmd, cwd=RETRY_SERVICE_DIR, log_file=retry_log, name="Retry Service", env=retry_env)
                    if discord_notifier: 
                        discord_notifier.notify_retry_service_started(BASELINE, wf)
                        discord_notifier.notify_stage(BASELINE, wf, "Retry Service", "Retry service started as subprocess")
                    print("✅ Retry service started")



                # 6) Start Load Generator
                print("⚡ Starting load generator...")
                url = f"{ROUTER_ENTRY_URL}/{wf}"
                loadgen_log = os.path.join(TESTER_DIR, "logs", f"loadgen_{current_experiment}.log")
                loadgen_cmd = ["python3", LOADGEN_SCRIPT, "--url", url, "--concurrency", "4", "--total", "500"]
                loadgen_proc = run_background(loadgen_cmd, cwd=TESTER_DIR, log_file=loadgen_log, name="Load Generator")
                if discord_notifier: discord_notifier.notify_loadgen_started(BASELINE, wf)

                # 7) Wait for loadgen to finish
                experiment_start_time = time.time()
                loadgen_proc.wait()
                experiment_duration = time.time() - experiment_start_time
                print(f"✅ Load generator finished in {experiment_duration:.2f} seconds.")
                if discord_notifier: discord_notifier.notify_experiment_complete(BASELINE, wf, experiment_duration)

            except subprocess.CalledProcessError as e:
                print(f"\n❌ FATAL ERROR during setup for {current_experiment}:")
                print(f"--- STDERR ---\n{e.stderr}\n--- END STDERR ---")
                if discord_notifier:
                    discord_notifier.notify_error(BASELINE, wf, f"Experiment setup failed:\n```\n{e.stderr[:500]}\n```")

            finally:
                # 8) Teardown
                print("🛑 Tearing down experiment...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Teardown", "Terminating and cleaning up.")
                cleanup_processes(cleanup_fission=True, wf_id=wf)
                
                # 9) Evaluate success rate and cost
                print(f"📊 Evaluating success rate and cost...")
                if discord_notifier: discord_notifier.notify_stage(BASELINE, wf, "Evaluation", "Starting success rate and cost analysis.")
                
                success_rate = None
                total_cost = None
                budget = None
                normalized_cost = None
                
                # Get cost from database (proper machine packing calculation)
                print(f"💰 Getting cost metrics from database...")
                cost_info = get_total_cost_from_db(wf, BASELINE)
                total_cost = cost_info.get('total_cost')
                
                # Get budget using same calculation as scaler
                budget = get_budget_for_workflow(wf, TRACE_AZ, TRACE_INSTANCE)
                
                # Fallback to scaler log if DB has no data
                if total_cost is None:
                    print(f"⚠️ No cost data in DB, falling back to scaler log...")
                    log_cost_info = extract_cost_from_scaler_log(scaler_log)
                    total_cost = log_cost_info.get('total_cost')
                    if log_cost_info.get('budget'):
                        budget = log_cost_info.get('budget')
                
                if total_cost is not None and budget is not None:
                    budget_pct = (total_cost / budget * 100) if budget > 0 else 0
                    # Normalized cost = cost relative to budget (1.0x = exactly budget)
                    normalized_cost = total_cost / budget if budget > 0 else 0
                    cost_status = "✅" if budget_pct <= 100 else "❌"
                    print(f"{cost_status} Cost for {BASELINE} | {wf}: ${total_cost:.2f} / ${budget:.2f} ({budget_pct:.1f}% of budget)")
                    norm_status = "✅" if normalized_cost <= 1.0 else "⚠️" if normalized_cost <= 1.5 else "❌"
                    print(f"   {norm_status} Normalized cost: {normalized_cost:.2f}x budget")
                    
                    if discord_notifier:
                        discord_notifier.notify_cost(BASELINE, wf, total_cost, budget, budget_pct, normalized_cost)
                else:
                    print(f"⚠️ Could not determine cost metrics for {BASELINE} | {wf}")
                
                # Evaluate success rate
                try:
                    eval_cmd = [
                        "python3", EVAL_SCRIPT,
                        "--workflow", wf,
                        "--baseline", BASELINE,
                        "--use-paths",
                        "--workflows-dir", WORKFLOWS_DIR,
                        "--experiment-tag", EXPERIMENT_TAG
                    ]
                    result = subprocess.run(eval_cmd, capture_output=True, text=True, check=True)
                    for line in result.stdout.split('\n'):
                        if "Success Rate:" in line:
                            success_rate = float(line.split(":")[1].strip().rstrip('%'))
                            break
                    
                    if success_rate is not None:
                        target_success_rate = get_target_success_rate(wf)
                        status_icon = "✅" if success_rate >= target_success_rate * 0.9 else "⚠️"
                        target_status = "meets" if success_rate >= target_success_rate else "below"
                        print(f"{status_icon} Success rate for {BASELINE} | {wf}: {success_rate:.1f}% "
                              f"(target: {target_success_rate:.1f}%, {target_status} target)")
                        
                        if discord_notifier:
                            discord_notifier.notify_success_rate(BASELINE, wf, success_rate)
                            
                            # Send combined summary if we have both metrics
                            if total_cost is not None and budget is not None:
                                discord_notifier.notify_experiment_summary(
                                    BASELINE, wf, success_rate, total_cost, budget,
                                    experiment_duration, target_success_rate
                                )
                    else:
                        print(f"⚠️ Could not determine success rate for {BASELINE} | {wf}")
                except subprocess.CalledProcessError as e:
                    print(f"\n❌ Evaluation script failed for {current_experiment}!")
                    print(f"--- EVALUATION SCRIPT STDERR ---\n{e.stderr}\n--- END STDERR ---")
                    if discord_notifier:
                        discord_notifier.notify_error(BASELINE, wf, f"Evaluation failed:\n```\n{e.stderr[:500]}\n```")
                
                time.sleep(5)
                # reset current_experiment for signal handler
                globals()["current_experiment"] = None

        total_duration = time.time() - start_time
        print(f"\n🎉 All experiments completed! Total time: {total_duration/3600:.2f} hours")
        if discord_notifier: discord_notifier.notify_all_complete(total_experiments, total_duration)
        
    except Exception as e:
        print(f"\n❌ An unexpected error occurred in the test runner: {e}")
        if discord_notifier: discord_notifier.notify_error("Test Runner", "FATAL", str(e))
    finally:
        print("--- Running final safety cleanup ---")
        cleanup_processes(cleanup_fission=True)

if __name__ == "__main__":
    main()