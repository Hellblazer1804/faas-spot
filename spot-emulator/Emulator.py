import argparse
import os
import time
import json
import random
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from kubernetes import client, config
from datetime import datetime, timezone
import subprocess

NAMESPACE = "default"
FISSION_POD_SELECTOR = "functionName"

# Machine packing simulation: only protect a fraction of pods for "protected" tasks
# This simulates real-world scenario where not all pods are on protected VMs
# Protection ratio is now DYNAMIC based on slack/survival (see get_dynamic_protection_ratio)
DEFAULT_PROTECTION_RATIO = 0.5  # Fallback if dynamic calculation fails
MIN_PROTECTION_RATIO = 0.2  # Never protect less than 20% 
MAX_PROTECTION_RATIO = 0.8  # Never protect more than 80% (some pods always on spot for cost savings)
# Set to track which pods have been assigned protection (deterministic across iterations)
PROTECTED_POD_CACHE = set()

# ---------- Helper Functions ----------
# Shared MySQL - use small pool to avoid "Too many connections" with retry-service/scaler/workflows
_emulator_mysql_pool = None

def mysql_connection():
    global _emulator_mysql_pool
    if _emulator_mysql_pool is None:
        _emulator_mysql_pool = pooling.MySQLConnectionPool(
            pool_name='emulator_pool',
            pool_size=2,
            pool_reset_session=True,
            user=os.getenv('DB_USER', ''), password=os.getenv('DB_PASSWORD', ''), host=os.getenv('DB_HOST', 'localhost'), database=os.getenv('DB_NAME', '')
        )
    try:
        return _emulator_mysql_pool.get_connection()
    except Exception:
        return mysql.connector.connect(
            user=os.getenv('DB_USER', ''), password=os.getenv('DB_PASSWORD', ''), host=os.getenv('DB_HOST', 'localhost'), database=os.getenv('DB_NAME', '')
        )

def load_avg_prices(az, instance):
    cost_csv_path = f"./tracing/spot-cost-csv/{az}_{instance}_cost.csv"
    if not os.path.exists(cost_csv_path):
        raise FileNotFoundError(f"Spot cost file not found: {cost_csv_path}")
    df = pd.read_csv(cost_csv_path)
    avg_spot_price = df["SpotPrice ($)"].mean()
    avg_savings_pct = df["Savings (%)"].mean()
    avg_ondemand_price = avg_spot_price / (1 - avg_savings_pct / 100) if avg_savings_pct < 100 else float('inf')
    
    # Burstable price: ~40% savings vs on-demand (between spot and on-demand)
    # Based on AWS T3 pricing which is typically 30-50% cheaper than equivalent on-demand
    avg_burstable_price = avg_ondemand_price * 0.60  # 40% savings
    
    print(f"[INFO] Prices loaded: Spot=${avg_spot_price:.4f}/hr, Burstable=${avg_burstable_price:.4f}/hr, On-Demand=${avg_ondemand_price:.4f}/hr")
    return avg_spot_price, avg_ondemand_price, avg_burstable_price

def get_deflection_points(cdf_path):
    if not os.path.exists(cdf_path):
        raise FileNotFoundError(f"CDF file not found: {cdf_path}")
    df = pd.read_csv(cdf_path)
    p10_lifetime_sec = df[df['CDF'] >= 0.10].iloc[0]['Lifetime']
    p90_lifetime_sec = df[df['CDF'] >= 0.90].iloc[0]['Lifetime']
    print(f"[INFO] Lifetime range for preemption (from trace): p10={p10_lifetime_sec:.2f}s, p90={p90_lifetime_sec:.2f}s")
    return p10_lifetime_sec, p90_lifetime_sec

def get_pod_age_seconds(pod):
    creation_ts = pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc)
    now_ts = datetime.now(timezone.utc)
    return (now_ts - creation_ts).total_seconds()

def group_pods_into_machines(pods, pods_per_machine=3):
    sorted_pods = sorted(pods, key=lambda p: p.metadata.name)
    return [sorted_pods[i:i + pods_per_machine] for i in range(0, len(sorted_pods), pods_per_machine)]

def ensure_cost_table():
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS cost_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                baseline VARCHAR(128),
                workflow_name VARCHAR(128),
                pod_name VARCHAR(255),
                pod_age_seconds FLOAT,
                pod_type VARCHAR(50),
                price_per_hour FLOAT,
                interval_cost FLOAT,
                check_interval_seconds INT,
                notes VARCHAR(255) DEFAULT 'main_experiment'
            )"""
        )
        try:
            cursor.execute(
                """SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME='cost_logs' AND COLUMN_NAME='notes'""",
                (DB_CONFIG.get("database"),),
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE cost_logs ADD COLUMN notes VARCHAR(255) DEFAULT 'main_experiment'")
        except Exception:
            pass
        conn.commit()

def get_dynamic_on_demand_tasks(baseline, workflow):
    """
    Get tasks that have been dynamically switched to on-demand mode.
    Used by Hourglass baseline when slack is low and it decides to fallback to on-demand.
    
    Returns: set of task_ids that should be treated as on-demand
    """
    dynamic_od_tasks = set()
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        # Check for ON_DEMAND_FALLBACK decisions from Hourglass
        cursor.execute(
            """SELECT DISTINCT task_id FROM baseline_decisions 
               WHERE baseline = %s AND workflow_name = %s 
               AND decision = 'ON_DEMAND_FALLBACK'
               AND timestamp > (NOW() - INTERVAL 30 MINUTE)""",
            (baseline, workflow)
        )
        dynamic_od_tasks = {row[0] for row in cursor.fetchall() if row[0]}
        cursor.close()
        conn.close()
    except Exception as e:
        # Table might not exist or query might fail
        pass
    return dynamic_od_tasks


def get_burstable_tasks(baseline, workflow):
    """
    Get tasks running on burstable instances for Bag-of-Tasks baseline.
    
    Burstable instances (like AWS T2/T3):
    - Are NOT preempted (reliable like on-demand)
    - Cost ~40% less than on-demand
    - Have CPU credit-based burst capability
    
    Per the paper: Tasks can be on burstable via:
    1. Resumed from hibernation (migrated from spot)
    2. Proactive placement (risky tasks placed directly on burstable)
    
    Returns: set of task_ids running on burstable instances
    """
    burstable_tasks = set()
    
    # For bag_of_tasks baseline: Check for vm_type = "burstable"
    if baseline == "bag_of_tasks":
        try:
            conn = mysql_connection()
            cursor = conn.cursor()
            # Query for tasks with vm_type = "burstable" (resumed OR proactive placement)
            cursor.execute(
                """SELECT DISTINCT task_id FROM bot_scheduling_log 
                   WHERE baseline = %s AND workflow_name = %s 
                   AND JSON_EXTRACT(details, '$.vm_type') = 'burstable'
                   AND timestamp > (NOW() - INTERVAL 30 MINUTE)""",
                (baseline, workflow)
            )
            burstable_tasks = {row[0] for row in cursor.fetchall() if row[0]}
            cursor.close()
            conn.close()
            
            if burstable_tasks:
                print(f"[BoT] {len(burstable_tasks)} tasks on BURSTABLE: {burstable_tasks}")
        except Exception as e:
            print(f"[BoT] Warning: Could not query burstable tasks: {e}")
        
        return burstable_tasks
    
    # For other baselines: Check bot_scheduling_log for explicit burstable decisions
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT DISTINCT task_id FROM bot_scheduling_log 
               WHERE baseline = %s AND workflow_name = %s 
               AND JSON_EXTRACT(details, '$.burst_used') = true
               AND timestamp > (NOW() - INTERVAL 30 MINUTE)""",
            (baseline, workflow)
        )
        burstable_tasks = {row[0] for row in cursor.fetchall() if row[0]}
        cursor.close()
        conn.close()
    except Exception:
        pass
    
    return burstable_tasks


def get_bot_ondemand_tasks(baseline, workflow):
    """
    Get tasks that fell back to on-demand due to credit exhaustion (BoT baseline).
    
    When burstable credits are exhausted, tasks fall back to on-demand pricing.
    These tasks are protected from preemption and charged at on-demand rates.
    
    Returns: set of task_ids on on-demand (credit exhaustion fallback)
    """
    od_tasks = set()
    
    if baseline != "bag_of_tasks":
        return od_tasks
    
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT DISTINCT task_id FROM bot_scheduling_log 
               WHERE baseline = %s AND workflow_name = %s 
               AND JSON_EXTRACT(details, '$.vm_type') = 'on-demand'
               AND timestamp > (NOW() - INTERVAL 30 MINUTE)""",
            (baseline, workflow)
        )
        od_tasks = {row[0] for row in cursor.fetchall() if row[0]}
        cursor.close()
        conn.close()
        
        if od_tasks:
            print(f"[BoT] {len(od_tasks)} tasks on ON-DEMAND (credit exhaustion): {od_tasks}")
    except Exception as e:
        print(f"[BoT] Warning: Could not query on-demand tasks: {e}")
    
    return od_tasks


def get_dynamic_protection_ratio(baseline, workflow):
    """
    Dynamically calculate protection ratio based on current state.
    
    Hourglass (EuroSys 2019): Based on SLACK (time buffer to deadline)
    - High slack (> 0.3) → Low protection (more spot, save cost)
    - Low slack (< 0.1) → High protection (meet deadline)
    
    Bag-of-Tasks: Based on average SURVIVAL PROBABILITY
    - High survival → Low protection (spot is safe)
    - Low survival → High protection (preemption imminent)
    
    Returns: float protection ratio between MIN_PROTECTION_RATIO and MAX_PROTECTION_RATIO
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        
        if baseline == "hourglass":
            # Get average slack from recent decisions
            cursor.execute(
                """SELECT AVG(JSON_EXTRACT(details, '$.slack')) as avg_slack
                   FROM baseline_decisions 
                   WHERE baseline = %s AND workflow_name = %s 
                   AND timestamp > (NOW() - INTERVAL 5 MINUTE)""",
                (baseline, workflow)
            )
            result = cursor.fetchone()
            if result and result[0] is not None:
                avg_slack = float(result[0])
                # Inverse relationship: low slack = high protection
                # protection_ratio = 1 - slack (clamped to min/max)
                protection_ratio = max(MIN_PROTECTION_RATIO, 
                                      min(MAX_PROTECTION_RATIO, 1.0 - avg_slack))
                print(f"[PROTECTION] Hourglass dynamic: slack={avg_slack:.2f} → ratio={protection_ratio:.2f}")
                cursor.close()
                conn.close()
                return protection_ratio
                
        elif baseline == "bag_of_tasks":
            # Get average WORKFLOW COMPLETION probability from recent scheduling decisions
            # This is more accurate than per-task survival for multi-task workflows
            cursor.execute(
                """SELECT AVG(JSON_EXTRACT(details, '$.workflow_completion_prob')) as avg_completion,
                          AVG(JSON_EXTRACT(details, '$.survival_probability')) as avg_survival
                   FROM bot_scheduling_log 
                   WHERE baseline = %s AND workflow_name = %s 
                   AND timestamp > (NOW() - INTERVAL 5 MINUTE)""",
                (baseline, workflow)
            )
            result = cursor.fetchone()
            if result:
                # Prefer workflow_completion_prob, fallback to survival_probability
                avg_completion = float(result[0]) if result[0] is not None else None
                avg_survival = float(result[1]) if result[1] is not None else None
                
                metric = avg_completion if avg_completion is not None else avg_survival
                if metric is not None:
                    # Inverse relationship: low completion prob = high protection
                    protection_ratio = max(MIN_PROTECTION_RATIO, 
                                          min(MAX_PROTECTION_RATIO, 1.0 - metric))
                    metric_name = "workflow_completion" if avg_completion is not None else "survival"
                    print(f"[PROTECTION] BoT dynamic: {metric_name}={metric:.2f} → ratio={protection_ratio:.2f}")
                    cursor.close()
                    conn.close()
                    return protection_ratio
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[PROTECTION] Could not calculate dynamic ratio: {e}")
    
    # Fallback to default
    print(f"[PROTECTION] Using default ratio: {DEFAULT_PROTECTION_RATIO}")
    return DEFAULT_PROTECTION_RATIO


def monitor_and_log_costs(pods, on_demand_tasks, spot_price, ondemand_price, interval_sec, baseline, workflow, burstable_price=None):
    """
    Calculates and logs incremental cost of all running pods for the last interval,
    split by baseline+workflow.

    Time compression: 1 emulator minute = 1 real second.
    So an interval of `interval_sec` real seconds equals 60 * interval_sec emulator seconds.
    Costs are per *emulated* hour.
    
    Pod types:
    - spot: Cheapest, can be preempted
    - burstable: Medium price (~40% savings vs OD), NOT preempted, used by BoT baseline
    - on-demand: Most expensive, NOT preempted, used by Hourglass fallback
    """
    if not pods:
        print("[COST] No active pods to monitor.")
        return
    
    # Default burstable price if not provided
    if burstable_price is None:
        burstable_price = ondemand_price * 0.60
        
    try:
        ensure_cost_table()
        conn = mysql_connection()
        cursor = conn.cursor()

        # Map real-time interval to emulator-time interval
        emu_interval_seconds = interval_sec * 60.0                      # 1s real = 60s emulator
        emu_interval_hours   = emu_interval_seconds / 3600.0            # convert to hours for $/hr pricing

        # For Hourglass: Get dynamically switched on-demand tasks
        dynamic_od_tasks = set()
        if baseline == "hourglass":
            dynamic_od_tasks = get_dynamic_on_demand_tasks(baseline, workflow)
            if dynamic_od_tasks:
                print(f"[COST] Dynamic on-demand tasks (Hourglass fallback): {dynamic_od_tasks}")
        
        # For Bag-of-Tasks: Get tasks on burstable and on-demand (credit exhaustion)
        burstable_tasks = set()
        bot_od_tasks = set()
        if baseline == "bag_of_tasks":
            burstable_tasks = get_burstable_tasks(baseline, workflow)
            bot_od_tasks = get_bot_ondemand_tasks(baseline, workflow)
            if burstable_tasks:
                print(f"[COST] Burstable tasks (BoT): {burstable_tasks}")
            if bot_od_tasks:
                print(f"[COST] On-demand tasks (BoT credit exhaustion): {bot_od_tasks}")
        
        # Combine all on-demand tasks (static + dynamic + BoT credit exhaustion)
        all_od_tasks = on_demand_tasks | dynamic_od_tasks | bot_od_tasks

        total_interval_cost = 0.0
        spot_count = 0
        od_count = 0
        burstable_count = 0
        
        for pod in pods:
            pod_age = get_pod_age_seconds(pod)
            pod_name = pod.metadata.name
            
            # Determine pod type and price
            # Priority: on-demand > burstable > spot
            is_on_demand = any(t and t in pod_name for t in all_od_tasks)
            is_burstable = any(t and t in pod_name for t in burstable_tasks) if not is_on_demand else False
            
            if is_on_demand:
                pod_type = 'on-demand'
                price_to_use = ondemand_price
                od_count += 1
            elif is_burstable:
                pod_type = 'burstable'
                price_to_use = burstable_price
                burstable_count += 1
            else:
                pod_type = 'spot'
                price_to_use = spot_price
                spot_count += 1

            # Accrue cost using EMULATOR time, not real time
            interval_cost = price_to_use * emu_interval_hours
            total_interval_cost += interval_cost

            experiment_tag = os.getenv("EXPERIMENT_TAG", "main_experiment")
            cursor.execute(
                "INSERT INTO cost_logs (timestamp, baseline, workflow_name, pod_name, pod_age_seconds, pod_type, price_per_hour, interval_cost, check_interval_seconds, notes) "
                "VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (baseline, workflow, pod_name, pod_age, pod_type, price_to_use, interval_cost, interval_sec, experiment_tag)
            )

        conn.commit()
        conn.close()
        
        # Build type summary
        type_parts = []
        if spot_count > 0:
            type_parts.append(f"spot={spot_count}")
        if burstable_count > 0:
            type_parts.append(f"burst={burstable_count}")
        if od_count > 0:
            type_parts.append(f"od={od_count}")
        type_summary = ", ".join(type_parts) if type_parts else f"{len(pods)} spot"
        
        print(f"[COST] ({baseline}|{workflow}) Logged interval cost for {len(pods)} pods [{type_summary}] "
              f"(emu {emu_interval_seconds:.0f}s over {interval_sec}s real): ${total_interval_cost:.6f}")
    except Exception as e:
        print(f"⚠️ Error in cost monitoring: {e}")


def apply_scaling_decisions():
    # Safety caps to prevent runaway scaling
    MAX_MINSCALE_CAP = 20  # Never scale minscale beyond this
    MAX_MAXSCALE_CAP = 50  # Never scale maxscale beyond this
    
    try:
        conn = mysql_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM baseline_decisions WHERE is_applied = FALSE")
        decisions = cursor.fetchall()
        if not decisions:
            return

        print(f"\n[CONTROLLER] Found {len(decisions)} new scaling decision(s).")
        for d in decisions:
            try:
                details_dict = json.loads(d['details'])
                target_func = details_dict['target_function']
            except (TypeError, json.JSONDecodeError, KeyError) as e:
                print(f"  ⚠️ Could not parse 'details' JSON for decision ID {d['id']}: {e}. Skipping.")
                cursor.execute("UPDATE baseline_decisions SET is_applied = TRUE WHERE id = %s", (d['id'],))
                conn.commit()
                continue
            decision = d['decision']
            
            # Skip scaling decisions for bag_of_tasks baseline (burstable VMs don't need scaling)
            baseline = d.get('baseline', '')
            if baseline == 'bag_of_tasks' and decision in ['SCALE_UP_SPOT', 'SCALE_UP', 'SCALE_DOWN_SPOT']:
                print(f"  ⏭️ Skipping scaling decision for bag_of_tasks baseline (burstable VMs are fixed)")
                cursor.execute("UPDATE baseline_decisions SET is_applied = TRUE WHERE id = %s", (d['id'],))
                conn.commit()
                continue
            
            try:
                result = subprocess.run(['kubectl', 'get', 'function', target_func, '-o', 'json'], capture_output=True, text=True, check=True)
                func_spec = json.loads(result.stdout)
                current_minscale = func_spec['spec']['InvokeStrategy']['ExecutionStrategy']['MinScale']
            except Exception as e:
                print(f"  ⚠️ Could not get current scale for {target_func}: {e}. Marking as applied.")
                cursor.execute("UPDATE baseline_decisions SET is_applied = TRUE WHERE id = %s", (d['id'],))
                conn.commit()
                continue

            new_minscale = current_minscale
            if decision in ['SCALE_UP_SPOT', 'SCALE_UP']:
                new_minscale = max(1, int(current_minscale * 1.5) + 1)
            elif decision == 'SCALE_DOWN_SPOT':
                new_minscale = 1

            # Apply safety caps to prevent runaway scaling
            if new_minscale > MAX_MINSCALE_CAP:
                print(f"  ⚠️ Capping minscale at {MAX_MINSCALE_CAP} (was {new_minscale})")
                new_minscale = MAX_MINSCALE_CAP
            
            new_maxscale = min(new_minscale * 2 + 1, MAX_MAXSCALE_CAP)

            if new_minscale != current_minscale:
                print(f"  🚀 Applying '{decision}': Scaling {target_func} from {current_minscale} to {new_minscale} pods (maxscale={new_maxscale}).")
                try:
                    subprocess.run(
                        ['fission', 'fn', 'update', '--name', target_func, '--minscale', str(new_minscale), '--maxscale', str(new_maxscale)],
                        check=True, capture_output=True, text=True
                    )
                except subprocess.CalledProcessError as e:
                    print(f"  ❌ Fission update failed for {target_func}: {e.stderr}")
            cursor.execute("UPDATE baseline_decisions SET is_applied = TRUE WHERE id = %s", (d['id'],))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error in scaling controller: {e}")

def run_preemption_logic(v1, pod_to_assigned_lifetime, p10_sec, p90_sec, pods_per_machine, 
                         on_demand_tasks=None, baseline=None, workflow=None):
    """
    Simulate spot instance preemption based on trace-driven lifetimes.
    
    Protected pod types (NEVER preempted):
    - On-demand pods: Used by Hourglass when deadline is at risk
    - Burstable pods: Used by Bag-of-Tasks (T2/T3 instances are reliable)
    
    Only spot pods can be preempted.
    """
    on_demand_tasks = on_demand_tasks or set()
    
    try:
        pods = v1.list_namespaced_pod(namespace=NAMESPACE, label_selector=FISSION_POD_SELECTOR).items
        if not pods:
            print("[PREEMPTOR] No Fission pods found.")
            return

        # For Hourglass: Get dynamically switched on-demand tasks
        dynamic_od_tasks = set()
        if baseline == "hourglass":
            dynamic_od_tasks = get_dynamic_on_demand_tasks(baseline, workflow)
        
        # For Bag-of-Tasks: Get burstable and on-demand (credit exhaustion) tasks
        burstable_tasks = set()
        bot_od_tasks = set()
        if baseline == "bag_of_tasks":
            burstable_tasks = get_burstable_tasks(baseline, workflow)
            bot_od_tasks = get_bot_ondemand_tasks(baseline, workflow)
        
        # Combine all protected tasks (on-demand + burstable + BoT credit exhaustion)
        all_od_tasks = on_demand_tasks | dynamic_od_tasks | bot_od_tasks
        all_protected_tasks = all_od_tasks | burstable_tasks

        # Group pods by task to apply PROTECTION_RATIO per task
        task_pods = {}
        for pod in pods:
            labels = pod.metadata.labels or {}
            func_name = labels.get("functionName", "unknown")
            if func_name not in task_pods:
                task_pods[func_name] = []
            task_pods[func_name].append(pod)
        
        # Get DYNAMIC protection ratio based on current slack/survival
        protection_ratio = get_dynamic_protection_ratio(baseline, workflow)
        
        # Determine which specific pods get protection (partial protection per task)
        actually_protected_pods = set()
        for task_name in all_protected_tasks:
            if task_name in task_pods:
                pods_for_task = task_pods[task_name]
                num_to_protect = max(1, int(len(pods_for_task) * protection_ratio))
                # Sort by name for deterministic selection
                sorted_pods = sorted(pods_for_task, key=lambda p: p.metadata.name)
                for i, pod in enumerate(sorted_pods):
                    if i < num_to_protect:
                        actually_protected_pods.add(pod.metadata.name)
                        PROTECTED_POD_CACHE.add(pod.metadata.name)
        
        # Log protection stats
        if all_protected_tasks:
            total_task_pods = sum(len(task_pods.get(t, [])) for t in all_protected_tasks)
            print(f"[PREEMPTOR] Dynamic protection ratio {protection_ratio:.2f}: {len(actually_protected_pods)}/{total_task_pods} pods actually protected")
        
        for pod in pods:
            pod_name = pod.metadata.name
            
            # Check if this specific pod is protected (not just the task)
            is_protected = pod_name in actually_protected_pods or pod_name in PROTECTED_POD_CACHE
            if is_protected:
                # Protected pods have infinite lifetime (no preemption)
                pod_to_assigned_lifetime[pod_name] = float('inf')
                continue
                
            if pod_name not in pod_to_assigned_lifetime:
                # Assign from p10..p90, then compress to minutes-as-seconds
                trace_lifetime_seconds = random.uniform(p10_sec, p90_sec)
                trace_lifetime_minutes = trace_lifetime_seconds / 60.0
                pod_lifetime_seconds = trace_lifetime_minutes
                pod_to_assigned_lifetime[pod_name] = pod_lifetime_seconds
                print(f"[PREEMPTOR] Assigned lifetime {pod_lifetime_seconds:.2f}s to {pod_name}")

        machines = group_pods_into_machines(pods, pods_per_machine)
        machines_to_preempt = []

        for machine_group in machines:
            # Skip machine if ANY pod is ACTUALLY protected (per-pod, not per-task)
            # This uses the partial protection ratio - not all pods of a "protected" task are protected
            has_protected_pod = any(
                p.metadata.name in actually_protected_pods or p.metadata.name in PROTECTED_POD_CACHE
                for p in machine_group
            )
            if has_protected_pod:
                continue
                
            for pod in machine_group:
                pod_age = get_pod_age_seconds(pod)
                assigned_lifetime = pod_to_assigned_lifetime.get(pod.metadata.name, float('inf'))
                if pod_age >= assigned_lifetime:
                    machines_to_preempt.append(machine_group)
                    pod_names = [p.metadata.name for p in machine_group]
                    print(f"🚩 [PREEMPTOR] Machine preempted by {pod.metadata.name}; deleting {pod_names}")
                    break

        for machine_group in machines_to_preempt:
            for pod in machine_group:
                try:
                    v1.delete_namespaced_pod(name=pod.metadata.name, namespace=NAMESPACE)
                    print(f"  🔥 Deleting pod {pod.metadata.name}...")
                    pod_to_assigned_lifetime.pop(pod.metadata.name, None)
                except client.ApiException as e:
                    if e.status != 404:
                        print(f"  ⚠️ Error deleting pod {pod.metadata.name}: {e}")
                        
        # Log protected pods count (actual protected, not theoretical)
        if actually_protected_pods:
            od_protected = sum(1 for p in pods if p.metadata.name in actually_protected_pods and any(t and t in p.metadata.name for t in all_od_tasks))
            burst_protected = sum(1 for p in pods if p.metadata.name in actually_protected_pods and any(t and t in p.metadata.name for t in burstable_tasks))
            if od_protected > 0 or burst_protected > 0:
                parts = []
                if od_protected > 0:
                    parts.append(f"{od_protected} on-demand")
                if burst_protected > 0:
                    parts.append(f"{burst_protected} burstable")
                print(f"[PREEMPTOR] Protected from preemption: {', '.join(parts)}")
                
    except Exception as e:
        print(f"An error occurred during preemption: {e}")

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(description="Spot emulator + controller + cost monitor")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--az", required=True)
    parser.add_argument("--on_demand_tasks", default="")
    parser.add_argument("--pods_per_machine", type=int, default=3)
    parser.add_argument("--check_interval", type=int, default=45)
    # NEW: segregate cost by baseline + workflow
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--workflow", required=True)
    args = parser.parse_args()

    on_demand_tasks = set([x for x in args.on_demand_tasks.split(",") if x]) if args.on_demand_tasks else set()
    spot_price, ondemand_price, burstable_price = load_avg_prices(args.az, args.instance)
    cdf_path = f'./tracing/spot-traces-csv/{args.az}_{args.instance}_1_cdf.csv'
    p10_sec, p90_sec = get_deflection_points(cdf_path)

    config.load_kube_config()
    v1 = client.CoreV1Api()

    pod_to_assigned_lifetime = {}
    iteration = 1

    print(f"[EMULATOR] Started (baseline={args.baseline}, workflow={args.workflow}). On-demand tasks: {on_demand_tasks or 'None'}")
    time.sleep(20)

    while True:
        print(f"\n===== Emulator Iteration {iteration} [{args.baseline}|{args.workflow}] =====")
        try:
            all_pods = v1.list_namespaced_pod(namespace=NAMESPACE, label_selector=FISSION_POD_SELECTOR).items
        except Exception as e:
            print(f"Could not list pods: {e}")
            time.sleep(args.check_interval)
            continue

        apply_scaling_decisions()
        run_preemption_logic(v1, pod_to_assigned_lifetime, p10_sec, p90_sec, args.pods_per_machine,
                            on_demand_tasks=on_demand_tasks, baseline=args.baseline, workflow=args.workflow)
        monitor_and_log_costs(all_pods, on_demand_tasks, spot_price, ondemand_price, args.check_interval, 
                             args.baseline, args.workflow, burstable_price=burstable_price)

        iteration += 1
        time.sleep(args.check_interval)

if __name__ == "__main__":
    main()
