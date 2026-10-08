import os
import subprocess
import time
import signal
import json
import sys
import argparse
import requests
import re
import pandas as pd
import numpy as np
from datetime import datetime
from discord import init_discord_notifications, get_discord_notifier

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS_DIR = os.path.join(BASE_DIR, "workflows")
EMULATOR_SCRIPT = os.path.join(BASE_DIR, "../New-Emulator/Emulator.py")
LOADGEN_SCRIPT = os.path.join(BASE_DIR, "load_generator.py")
CLEANUP_SCRIPT = os.path.join(BASE_DIR, "cleanup.sh")
RETRY_SERVICE = os.path.join(BASE_DIR, "retry_service.py")
EVAL_SCRIPT = os.path.join(BASE_DIR, "evaluation-scripts", "success_rate.py")

# All baselines (static, op-based, cp-based)
# uses_retry=True means we'll run retry_service.py --baseline <baseline>
# uses_on_demand=True means the baseline can fall back to on-demand VMs
# uses_hibernation=True means the baseline uses hibernation (BoT paper)
BASELINES_CONFIG = {
    # static / op-based
    # "static":           {"display_name": "Static", "uses_on_demand": False, "uses_retry": False},
    # "static_over_2x":   {"display_name": "Static Overprovision 2x", "uses_on_demand": False, "uses_retry": False},
    # "protean":          {"display_name": "Protean", "uses_on_demand": False, "uses_retry": False},  # Not implementing
    # MScheduler, Snape, OFP-TM: on-demand from paper logic only (Emulator reads per-task decisions)
    # NOT static "finals" - that would be cheating
    # "mscheduler":       {"display_name": "MScheduler", "uses_on_demand": False, "uses_retry": False},
    "snape":            {"display_name": "Snape", "uses_on_demand": True,        "uses_retry": False},
    # "ofp_tm":           {"display_name": "OFP-TM", "uses_on_demand": True,       "uses_retry": False},
    # checkpointing (enable retry service)
    # "static_cp":        {"display_name": "Static-CP",      "uses_on_demand": False, "uses_retry": True},
    # "multi_level_cp":   {"display_name": "Multi-Level-CP", "uses_on_demand": False, "uses_retry": True},
    # # Hourglass (EuroSys 2019): Slack-aware provisioning with on-demand fallback
    # "hourglass":        {
    #     "display_name": "Hourglass (Slack-aware+OD Fallback)",
    #     "uses_on_demand": True,   # Can fall back to on-demand when deadline at risk
    #     "uses_retry": True,
    #     "uses_slack_aware": True,  # Uses slack-based configuration selection
    # },
    # Bag-of-Tasks (IEEE TCC 2023): Burstable VM scheduling with spot hibernation
    # "bag_of_tasks":     {
    #     "display_name": "Bag-of-Tasks (Burstable+Hibernation)",
    #     "uses_on_demand": False,
    #     "uses_retry": True,
    #     "uses_hibernation": True,  # Uses hibernation instead of simple checkpointing
    # },
}
BASELINES = list(BASELINES_CONFIG.keys())

# Emulator defaults
EMULATOR_AZ = "us-west-2a"
EMULATOR_INSTANCE = "v100"

background_processes = []
current_experiment = None
_signal_handled = False
_current_run_log_path = None


def _handle_shutdown_signal(signum, _frame):
    global _signal_handled
    if _signal_handled:
        return
    _signal_handled = True
    try:
        signame = signal.Signals(signum).name
    except Exception:
        signame = str(signum)
    print(f"\n❌ Runner received signal {signame} ({signum}). Initiating cleanup...")
    if get_discord_notifier():
        try:
            exp = current_experiment or "startup"
            get_discord_notifier().notify_error("Test Runner", f"Signal {signame}", f"Received {signame} during {exp}")
            get_discord_notifier().notify_stage("Global", exp, "Runner", f"Received {signame}. Cleaning up.")
            get_discord_notifier().notify_termination("Signal", signame, current_experiment)
        except Exception:
            pass
    try:
        stop_all(cleanup_fission=True)
    except Exception:
        pass
    try:
        if _current_run_log_path:
            with open(_current_run_log_path, "a") as f:
                f.write(f"\n❌ Runner received signal {signame} ({signum}). Shutdown initiated.\n")
    except Exception:
        pass
    sys.exit(1)


def _install_signal_handlers():
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT):
        try:
            signal.signal(sig, _handle_shutdown_signal)
        except Exception:
            pass

# --- Cost Calculation Helpers ---
DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}

def normalize_baseline(b):
    if pd.isna(b): return ""
    return str(b).strip().lower().replace("-", "_")

def task_from_podname(pn):
    if pd.isna(pn): return ""
    m = re.search(r'(task\d+)', str(pn))
    return m.group(1) if m else ""

def compute_cost_for_experiment(workflow, baseline):
    """
    Compute total cost for a specific workflow+baseline experiment.
    Returns (total_cost, cost_breakdown_str) or (None, error_msg) on failure.
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(**DB_CONFIG)
        
        # Get cost logs
        cost_query = """
            SELECT timestamp, baseline, workflow_name, pod_name, pod_age_seconds, 
                   pod_type, price_per_hour, interval_cost, check_interval_seconds
            FROM cost_logs 
            WHERE workflow_name = %s AND baseline = %s
        """
        cost_df = pd.read_sql(cost_query, conn, params=(workflow, baseline))
        
        # Get execution logs for time window
        evals_query = """
            SELECT workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code, baseline
            FROM serverless_workflows
            WHERE workflow_name = %s AND baseline = %s
        """
        evals_df = pd.read_sql(evals_query, conn, params=(workflow, baseline))
        conn.close()
        
        if cost_df.empty:
            return 0.0, "No cost logs found"
        
        if evals_df.empty:
            return 0.0, "No execution logs found"
        
        # Normalize columns
        cost_df.columns = [x.strip() for x in cost_df.columns]
        cost_df["workflow_name"] = cost_df.get("workflow_name", "").astype(str).str.strip().str.lower()
        cost_df["baseline"] = cost_df.get("baseline", "").astype(str).map(normalize_baseline)
        
        if "timestamp" in cost_df.columns:
            cost_df["ts"] = pd.to_datetime(cost_df["timestamp"], errors="coerce")
        else:
            cost_df["ts"] = pd.NaT
            
        for col in ["pod_age_seconds", "price_per_hour"]:
            if col in cost_df.columns:
                cost_df[col] = pd.to_numeric(cost_df[col], errors="coerce")
        
        cost_df["task"] = cost_df["pod_name"].apply(task_from_podname)
        
        # Assign machine IDs: 3 pods share 1 machine
        def assign_machine_ids(df_group):
            pods = sorted(df_group["pod_name"].unique())
            pod_index = {p: i for i, p in enumerate(pods)}
            idx = df_group["pod_name"].map(pod_index).astype(int)
            return (idx // 3) + 1
        
        cost_df["machine_id"] = cost_df.groupby(["workflow_name", "task"], group_keys=False).apply(assign_machine_ids)
        
        # Get time window from evals
        evals_df["workflow_name"] = evals_df["workflow_name"].astype(str).str.strip().str.lower()
        evals_df["baseline"] = evals_df["baseline"].astype(str).map(normalize_baseline)
        t0 = evals_df["start_time"].min()
        t1 = evals_df["end_time"].max()
        t0_dt = pd.to_datetime(t0, unit="s", errors="coerce")
        t1_dt = pd.to_datetime(t1, unit="s", errors="coerce")
        
        # Filter to time window
        di = cost_df.copy()
        if di["ts"].notna().any() and pd.notna(t0_dt) and pd.notna(t1_dt):
            di_filtered = di[(di["ts"] >= t0_dt) & (di["ts"] <= t1_dt)].copy()
            if not di_filtered.empty:
                di = di_filtered
        
        di = di.sort_values(["pod_name", "ts"], kind="mergesort")
        di["prev_age"] = di.groupby("pod_name")["pod_age_seconds"].shift(1)
        di["delta_age_sec"] = (di["pod_age_seconds"] - di["prev_age"]).clip(lower=0)
        
        if "check_interval_seconds" in di.columns:
            sel = di["delta_age_sec"].isna() | (di["delta_age_sec"] == 0)
            di.loc[sel, "delta_age_sec"] = di.loc[sel, "check_interval_seconds"].fillna(0)
        else:
            di["delta_age_sec"] = di["delta_age_sec"].fillna(0)
        
        di["price_per_hour"] = pd.to_numeric(di["price_per_hour"], errors="coerce").fillna(0)
        
        # Machine-level stats
        machine_stats = di.groupby(["task", "machine_id"], observed=False).agg(
            min_age=("pod_age_seconds", "min"),
            max_age=("pod_age_seconds", "max"),
            max_delta_sec=("delta_age_sec", "max")
        ).reset_index()
        
        # Get price from oldest pod on machine
        price_map = (
            di.sort_values(["task", "machine_id", "pod_age_seconds"], ascending=[True, True, False])
              .groupby(["task", "machine_id"], observed=False)
              .first()
              .reset_index()[["task", "machine_id", "price_per_hour"]]
        )
        machine_stats = machine_stats.merge(price_map, on=["task", "machine_id"], how="left")
        machine_stats["price_per_hour"] = pd.to_numeric(machine_stats["price_per_hour"], errors="coerce").fillna(0)
        
        # Use oldest pod's age as machine lifetime
        machine_stats["effective_sec"] = machine_stats["max_age"].fillna(0)
        machine_stats["spot_hours"] = machine_stats["effective_sec"] / 3600.0
        machine_stats["cost"] = machine_stats["price_per_hour"] * machine_stats["spot_hours"]
        
        total_cost = float(machine_stats["cost"].sum())
        
        # Build breakdown
        n_machines = len(machine_stats)
        n_pods = di["pod_name"].nunique()
        breakdown = f"{n_pods} pods, {n_machines} machines"
        
        return total_cost, breakdown
        
    except Exception as e:
        return None, f"Error: {e}"

def run_bg(cmd, cwd=None, log_file=None, name="proc"):
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        log_handle = open(log_file, 'w')
    else:
        log_handle = open(os.devnull, 'w')
    p = subprocess.Popen(cmd, cwd=cwd, stdout=log_handle, stderr=log_handle, preexec_fn=os.setsid)
    background_processes.append({"process": p, "name": name})
    return p

def stop_all(cleanup_fission=False, wf_id=None):
    print("\n🔄 Terminating background processes...")
    for proc in background_processes:
        try:
            p = proc["process"]
            if p and p.poll() is None:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                time.sleep(2)
                if p.poll() is None:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                print(f"✅ {proc['name']} terminated.")
        except Exception:
            pass
    background_processes.clear()

    if cleanup_fission:
        try:
            if get_discord_notifier():
                get_discord_notifier().notify_stage("Global", wf_id or "Global", "Cleanup", "Starting cleanup of Fission resources.")
            cmd = [CLEANUP_SCRIPT]
            if wf_id:
                print(f"🧹 Targeted cleanup for {wf_id} ...")
                cmd.append(wf_id)
            else:
                print("🧹 Blanket cleanup of all Fission resources ...")
            subprocess.run(cmd, check=True, text=True, stderr=subprocess.PIPE)
            print("✅ Fission cleanup successful.")
            if get_discord_notifier():
                get_discord_notifier().notify_stage("Global", wf_id or "Global", "Cleanup", "Cleanup complete.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Cleanup error:\n{e.stderr}")
            if get_discord_notifier():
                get_discord_notifier().notify_error("Cleanup", wf_id or "Global", f"Cleanup error:\n```\n{e.stderr[:500]}\n```")

def signal_handler(signum, frame):
    _handle_shutdown_signal(signum, frame)

def build_on_demand_map():
    # For on-demand baselines, pick final tasks as "on-demand"
    on_demand = {}
    wfs = sorted([d for d in os.listdir(WORKFLOWS_DIR) if d.startswith("wf-")])
    for wf in wfs:
        tasks_path = os.path.join(WORKFLOWS_DIR, wf, "tasks.json")
        if not os.path.exists(tasks_path):
            continue
        try:
            with open(tasks_path) as f:
                tasks = json.load(f)["tasks"]
            all_succ = set()
            for meta in tasks.values():
                all_succ.update(meta.get("successors", []))
            finals = [t for t in tasks.keys() if t not in all_succ]
            if finals:
                on_demand[wf] = ",".join(finals)
        except Exception as e:
            print(f"  ⚠️ {wf} parse error: {e}")
    return on_demand

def clear_stale_decisions(workflow=None, baseline=None):
    """
    Clear old baseline_decisions to prevent stale decisions from previous runs
    from causing runaway scaling.
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            user=os.getenv('DB_USER', ''), password=os.getenv('DB_PASSWORD', ''), host=os.getenv('DB_HOST', 'localhost'), database=os.getenv('DB_NAME', '')
        )
        cursor = conn.cursor()
        if workflow and baseline:
            # Clear decisions for specific workflow+baseline
            cursor.execute(
                "UPDATE baseline_decisions SET is_applied = TRUE WHERE workflow_name = %s AND baseline = %s AND is_applied = FALSE",
                (workflow, baseline)
            )
            print(f"🧹 Cleared {cursor.rowcount} stale decisions for {workflow}/{baseline}")
        else:
            # Clear ALL unapplied decisions
            cursor.execute("UPDATE baseline_decisions SET is_applied = TRUE WHERE is_applied = FALSE")
            print(f"🧹 Cleared {cursor.rowcount} stale decisions (all workflows)")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Could not clear stale decisions: {e}")

def run_success_rate(workflow, baseline):
    """
    Call the evaluation-scripts/success_rate.py for a specific workflow+baseline.
    Also computes and displays cost.
    """
    success_rate = None
    total_cost = None
    
    # Success rate evaluation
    try:
        if get_discord_notifier():
            get_discord_notifier().notify_stage(baseline, workflow, "Evaluation", "Starting success-rate analysis.")
        cmd = ["python3", EVAL_SCRIPT, "--workflow", workflow, "--baseline", baseline, "--use-paths"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("\n--- Success Rate Output ---")
        print(result.stdout.strip())
        print("--- End Success Rate Output ---\n")

        for line in result.stdout.splitlines():
            if "Success Rate:" in line:
                try:
                    success_rate = float(line.split(":")[1].strip().rstrip('%'))
                except Exception:
                    pass
        if get_discord_notifier() and success_rate is not None:
            get_discord_notifier().notify_success_rate(baseline, workflow, success_rate)
            get_discord_notifier().notify_stage(baseline, workflow, "Evaluation", f"Success-rate complete: {success_rate:.2f}%")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Success-rate evaluation failed for {workflow} | {baseline}:\n{e.stderr}")
        if get_discord_notifier():
            get_discord_notifier().notify_error(baseline, workflow, f"Success-rate failed:\n```\n{e.stderr[:500]}\n```")
    
    # Cost calculation
    try:
        total_cost, breakdown = compute_cost_for_experiment(workflow, baseline)
        print(f"\n--- Cost Output ---")
        if total_cost is not None:
            print(f"Workflow: {workflow} | Baseline: {baseline}")
            print(f"Total Cost: ${total_cost:.6f}")
            print(f"Breakdown: {breakdown}")
        else:
            print(f"Cost calculation failed: {breakdown}")
        print(f"--- End Cost Output ---\n")
    except Exception as e:
        print(f"⚠️ Cost calculation failed for {workflow} | {baseline}: {e}")
    
    # Summary
    print(f"\n{'='*50}")
    print(f"📊 EXPERIMENT SUMMARY: {workflow} | {baseline}")
    print(f"{'='*50}")
    if success_rate is not None:
        print(f"   Success Rate: {success_rate:.2f}%")
    else:
        print(f"   Success Rate: N/A")
    if total_cost is not None:
        print(f"   Total Cost:   ${total_cost:.6f}")
    else:
        print(f"   Total Cost:   N/A")
    print(f"{'='*50}\n")
    
    # Discord notification with both success rate and cost
    if get_discord_notifier():
        get_discord_notifier().notify_experiment_summary(baseline, workflow, success_rate, total_cost)

def main():
    _install_signal_handlers()

    discord = init_discord_notifications()
    if get_discord_notifier():
        get_discord_notifier().notify_stage("Global", "Global", "Runner", "Test runner started.")
    on_demand_map = build_on_demand_map()

    # All workflows for all baselines
    all_workflows = sorted([d for d in os.listdir(WORKFLOWS_DIR) if d.startswith("wf-") and os.path.exists(os.path.join(WORKFLOWS_DIR, d, "tasks.json"))])
    default_workflows = all_workflows if all_workflows else ['wf-4', 'wf-5', 'wf-7', 'wf-8']
    BASELINE_WORKFLOWS = {
        # "mscheduler": ['wf-3'],
        "snape": [workflow for workflow in all_workflows],
        # "bag_of_tasks": [workflow for workflow in all_workflows],
        # "ofp_tm": [workflow for workflow in all_workflows],
    }  # No per-baseline overrides; all baselines run all workflows
    
    # Calculate total experiments
    total = sum(len(BASELINE_WORKFLOWS.get(b, default_workflows)) for b in BASELINES)
    count = 0

    print(f"\n{'='*20} INITIAL BLANKET CLEANUP {'='*20}")
    if get_discord_notifier():
        get_discord_notifier().notify_stage("Global", "Global", "Initial Cleanup", "Starting blanket cleanup.")
    stop_all(cleanup_fission=True)
    clear_stale_decisions()  # Clear ALL stale decisions from previous runs

    try:
        for baseline in BASELINES:
            baseline_wfs = BASELINE_WORKFLOWS.get(baseline, default_workflows)
            for wf in baseline_wfs:
                count += 1
                global current_experiment
                current_experiment = f"{baseline}_{wf}"
                print(f"\n{'='*20} Experiment {count}/{total}: {current_experiment} {'='*20}")

                if get_discord_notifier():
                    get_discord_notifier().notify_experiment_start(baseline, wf)
                    get_discord_notifier().notify_progress(count, total, baseline, wf)
                    get_discord_notifier().notify_stage(baseline, wf, "Prep", "Targeted cleanup before deploy.")
                # Targeted cleanup before redeploy (prevents "already exists")
                stop_all(cleanup_fission=True, wf_id=wf)
                clear_stale_decisions(wf, baseline)  # Clear stale decisions for this workflow

                # Deploy
                baseline_info = BASELINES_CONFIG[baseline]
                features = []
                if baseline_info.get("uses_on_demand"):
                    features.append("on-demand fallback")
                if baseline_info.get("uses_hibernation"):
                    features.append("hibernation")
                if baseline_info.get("uses_slack_aware"):
                    features.append("slack-aware")
                if baseline_info.get("uses_retry"):
                    features.append("retry service")
                features_str = ", ".join(features) if features else "basic"
                
                print(f"🚀 Deploying {wf} for baseline {baseline} [{features_str}]...")
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "Deploy", f"Deploying workflow ({features_str}).")
                deploy_script = os.path.join(WORKFLOWS_DIR, wf, f"deploy_{baseline}.sh")
                try:
                    subprocess.run([deploy_script], cwd=os.path.join(WORKFLOWS_DIR, wf), check=True, text=True, stderr=subprocess.PIPE)
                    if get_discord_notifier():
                        get_discord_notifier().notify_stage(baseline, wf, "Deploy", "Deployment complete.")
                        get_discord_notifier().notify_deployment_complete(baseline, wf)
                except subprocess.CalledProcessError as e:
                    print(f"\n❌ Deployment failed for {current_experiment}:\n{e.stderr}")
                    if get_discord_notifier():
                        get_discord_notifier().notify_error(baseline, wf, f"Deployment failed:\n```\n{e.stderr[:500]}\n```")
                    continue

                # Wait for pods to come up - longer for complex baselines
                stabilize_time = 30
                if BASELINES_CONFIG[baseline].get("uses_hibernation") or BASELINES_CONFIG[baseline].get("uses_slack_aware"):
                    stabilize_time = 45  # Extra time for state initialization
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "Deploy", f"Stabilizing ({stabilize_time}s).")
                print(f"⏳ Waiting {stabilize_time}s for pods to stabilize...")
                time.sleep(stabilize_time)

                # Emulator (with cost segregation)
                print("🌀 Starting emulator (with cost segregation)...")
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "Emulator", "Starting emulator with cost logging.")
                emulator_log = os.path.join(os.path.dirname(EMULATOR_SCRIPT), "emulator-logs", f"emulator_{current_experiment}.log")
                on_demand_tasks = on_demand_map.get(wf, "") if BASELINES_CONFIG[baseline].get("uses_on_demand") else ""
                emu_cmd = [
                    "python3", EMULATOR_SCRIPT,
                    "--instance", EMULATOR_INSTANCE,
                    "--az", EMULATOR_AZ,
                    "--on_demand_tasks", on_demand_tasks,
                    "--baseline", baseline,
                    "--workflow", wf
                ]
                run_bg(emu_cmd, cwd=os.path.dirname(EMULATOR_SCRIPT), log_file=emulator_log, name="Emulator")
                if get_discord_notifier():
                    get_discord_notifier().notify_emulator_started(baseline, wf)

                # Retry service (only for checkpointing baselines)
                if BASELINES_CONFIG[baseline].get("uses_retry"):
                    service_type = "hibernation resume" if BASELINES_CONFIG[baseline].get("uses_hibernation") else "checkpoint resume"
                    print(f"🧰 Starting retry service ({service_type})...")
                    if get_discord_notifier():
                        get_discord_notifier().notify_stage(baseline, wf, "Retry Service", f"Starting {service_type} service.")
                    retry_log = os.path.join(BASE_DIR, "retry-logs", f"retry_{current_experiment}.log")
                    rs_cmd = ["python3", RETRY_SERVICE, "--baseline", baseline, "--workflow", wf]
                    run_bg(rs_cmd, cwd=BASE_DIR, log_file=retry_log, name="RetryService")

                # Load generator
                print("⚡ Starting load generator...")
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "LoadGen", "Starting load generator.")
                url = ff"{os.getenv('ROUTER_BASE_URL', 'http://localhost:32363')}/{wf}"
                loadgen_log = os.path.join(BASE_DIR, "loadgen-logs", f"loadgen_{current_experiment}.log")
                lg_cmd = ["python3", LOADGEN_SCRIPT, "--url", url, "--concurrency", "8", "--total", "500"]
                lg_proc = run_bg(lg_cmd, cwd=BASE_DIR, log_file=loadgen_log, name="LoadGen")
                if get_discord_notifier():
                    get_discord_notifier().notify_loadgen_started(baseline, wf)

                lg_proc.wait()
                print("✅ Load generator complete.")
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "LoadGen", "Load generation complete.")
                    get_discord_notifier().notify_experiment_complete(baseline, wf, 0)

                # Teardown
                print("🛑 Teardown...")
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "Teardown", "Stopping services and cleaning up.")
                stop_all(cleanup_fission=True, wf_id=wf)
                if get_discord_notifier():
                    get_discord_notifier().notify_stage(baseline, wf, "Teardown", "Teardown complete.")

                # Per-workflow+baseline success-rate evaluation
                print("📊 Evaluating success rate...")
                run_success_rate(wf, baseline)

                time.sleep(3)
                current_experiment = None

        print("\n🎉 All baselines & workflows finished.")
        if get_discord_notifier():
            get_discord_notifier().notify_all_complete(total, 0)
    except Exception as e:
        print(f"\n❌ Runner error: {e}")
        if get_discord_notifier():
            get_discord_notifier().notify_error("Test Runner", "FATAL", str(e))
    finally:
        print("--- Final cleanup ---")
        stop_all(cleanup_fission=True)
        if get_discord_notifier():
            get_discord_notifier().notify_stage("Global", "Global", "Runner", "Test runner finished. Final cleanup done.")

if __name__ == "__main__":
    main()
