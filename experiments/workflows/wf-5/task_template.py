import os
import time
import json
import uuid
import random
import glob
import requests
import mysql.connector
from flask import Flask, request
from typing import Any, Dict, Optional, Tuple

app = Flask(__name__)

# Fission mounts:
#   ConfigMaps -> /configs/<namespace>/<configmap>/<key>
#   Secrets    -> /secrets/<namespace>/<secret>/<key>
CONFIGS_ROOT = "/configs"
SECRETS_ROOT = "/secrets"

# --- Global Cache for Pod Start Times ---
POD_START_TIME_CACHE: Dict[str, float] = {}

# ==============================================================================
# HOURGLASS CONSTANTS (EuroSys 2019)
# Slack-aware provisioning with on-demand fallback
# ==============================================================================
HOURGLASS_DEADLINE_FACTOR = 1.5  # Deadline = 1.5x critical path time
HOURGLASS_SAFE_SLACK_THRESHOLD = 0.3  # >30% slack = safe, use spot aggressively
HOURGLASS_RISKY_SLACK_THRESHOLD = 0.1  # <10% slack = risky, consider on-demand
HOURGLASS_CHECKPOINT_PROB_THRESHOLD = 0.7  # Checkpoint when survival < 70%

# ==============================================================================
# BAG-OF-TASKS CONSTANTS (IEEE TCC 2023)
# Burstable VM scheduling with spot hibernation
# ==============================================================================
BOT_INITIAL_CREDITS = 100.0  # Starting CPU credits
BOT_CREDIT_EARN_RATE = 5.0   # Credits earned per minute at baseline
BOT_CREDIT_BURST_COST = 20.0  # Credits consumed per burst minute
BOT_MIN_CREDITS_FOR_BURST = 30.0  # Minimum credits to allow bursting
BOT_HIBERNATION_THRESHOLD = 0.90  # Hibernate when workflow completion < 90%
BOT_CHECKPOINT_THRESHOLD = 0.95  # Checkpoint when workflow completion < 95%
# Probabilistic placement: proactively place risky tasks on burstable
BOT_PROACTIVE_BURSTABLE_THRESHOLD = 0.5  # Start on burstable if survival for task duration < 50%
# Credit exhaustion: fall back to on-demand when credits depleted
BOT_CREDIT_EXHAUSTION_THRESHOLD = 10.0  # Switch to on-demand pricing when credits < 10

# Global credit tracking per pod
POD_CREDIT_CACHE: Dict[str, float] = {}
POD_LAST_CREDIT_UPDATE: Dict[str, float] = {}

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

def read_secret(key: str, default: str = "") -> str:
    file_val = _read_first(os.path.join(SECRETS_ROOT, "*", "*", key))
    if file_val is not None:
        return file_val
    return os.environ.get(key, default)

def mysql_connection():
    # Suggestion: mount DB creds as a Secret. These are fallbacks.
    user = read_secret("DB_USER", "root")
    pwd  = read_secret("DB_PASS", "mysql123")
    host = read_secret("DB_HOST", "172.22.162.176")
    db   = read_secret("DB_NAME", "wms")
    return mysql.connector.connect(user=user, password=pwd, host=host, database=db)

def checkpoint_to_db(task_id: str, uid: str, metadata: Dict[str, Any], level: str, workflow: str, baseline: str):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO task_checkpoints (uid, workflow_name, baseline, task_id, result, checkpoint_level, timestamp) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
            (uid, workflow, baseline, task_id, json.dumps(metadata), level)
        )
        conn.commit()
        conn.close()
        print(f"✅ [{level.upper()}] Checkpointed {task_id} for UID {uid}")
    except Exception as e:
        print(f"❌ Checkpoint failed: {e}")

def log_execution(workflow, task_id, uid, start_time, end_time, status_code, baseline):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO serverless_workflows (workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code, baseline) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (workflow, task_id, start_time, end_time, uid, status_code, baseline)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Execution logging failed: {e}")

def log_decision(workflow, task_id, uid, baseline, decision, details):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS baseline_decisions (
                id INT AUTO_INCREMENT PRIMARY KEY, workflow_name VARCHAR(255), task_id VARCHAR(255), 
                uuid VARCHAR(255), baseline VARCHAR(100), decision VARCHAR(100),
                details JSON, timestamp DATETIME, is_applied BOOLEAN DEFAULT FALSE
            )"""
        )
        cursor.execute(
            "INSERT INTO baseline_decisions (workflow_name, task_id, uuid, baseline, decision, details, timestamp) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
            (workflow, task_id, uid, baseline, decision, json.dumps(details))
        )
        conn.commit()
        conn.close()
        print(f"🧠 [{baseline.upper()}] Logged decision '{decision}' for {details.get('target_function', task_id)}")
    except Exception as e:
        print(f"⚠️ Decision logging failed: {e}")

def get_recent_lifetimes_from_db(az, instance_type, window=50):
    lifetimes = []
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        query = "SELECT lifetime_minutes FROM spot_traces WHERE az = %s AND instance_type = %s ORDER BY id DESC LIMIT %s"
        cursor.execute(query, (az, instance_type, window))
        lifetimes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error fetching recent lifetimes from DB: {e}")
    return lifetimes

def load_cdf_from_db(az, instance_type):
    cdf = {}
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        query = "SELECT lifetime_minutes FROM spot_traces WHERE az = %s AND instance_type = %s"
        cursor.execute(query, (az, instance_type))
        lifetimes = sorted([row[0] for row in cursor.fetchall()])
        cursor.close()
        conn.close()
        if not lifetimes:
            print("⚠️ Could not load CDF from DB: No trace data found.")
            return cdf
        total_samples = len(lifetimes)
        for i, lifetime in enumerate(lifetimes):
            cdf[lifetime] = (i + 1) / total_samples
    except Exception as e:
        print(f"⚠️ Error loading CDF from DB: {e}")
    return cdf

def get_survival_prob(cdf, t_s):
    if not cdf or t_s <= 0: return 1.0
    possible_lifetimes = [t for t in cdf if t <= t_s]
    if not possible_lifetimes: return 1.0
    closest_t = max(possible_lifetimes)
    return 1.0 - cdf[closest_t]

# ==============================================================================
# HOURGLASS IMPLEMENTATION (EuroSys 2019)
# Slack-aware provisioning for time-constrained jobs
# ==============================================================================
def hourglass_calculate_slack(
    current_time: float,
    deadline: float,
    remaining_exec_time: float
) -> float:
    """
    Calculate temporal slack (Hourglass paper Section 3.2).
    Slack = (deadline - current_time - remaining_exec_time) / remaining_exec_time
    
    Returns:
        Normalized slack ratio. >0.3 = safe, <0.1 = risky
    """
    time_remaining = deadline - current_time
    if remaining_exec_time <= 0:
        return 1.0  # Task complete, infinite slack
    slack = (time_remaining - remaining_exec_time) / remaining_exec_time
    return max(0.0, slack)

def hourglass_select_configuration(
    slack: float,
    survival_prob: float,
    current_config: str
) -> Tuple[str, str]:
    """
    Select VM configuration based on slack (Hourglass paper Algorithm 1).
    
    Returns:
        (config_decision, reason)
        - "SPOT_AGGRESSIVE": Use spot, accept higher risk
        - "SPOT_CONSERVATIVE": Use spot with checkpointing
        - "ON_DEMAND_FALLBACK": Switch to on-demand to meet deadline
    """
    if slack > HOURGLASS_SAFE_SLACK_THRESHOLD:
        # High slack - can be aggressive with spot instances
        if survival_prob > 0.5:
            return "SPOT_AGGRESSIVE", f"High slack ({slack:.2f}), good survival ({survival_prob:.2f})"
        else:
            return "SPOT_CONSERVATIVE", f"High slack ({slack:.2f}) but low survival ({survival_prob:.2f}), checkpoint"
    
    elif slack > HOURGLASS_RISKY_SLACK_THRESHOLD:
        # Medium slack - be cautious
        if survival_prob > 0.7:
            return "SPOT_CONSERVATIVE", f"Medium slack ({slack:.2f}), checkpoint for safety"
        else:
            return "ON_DEMAND_FALLBACK", f"Medium slack ({slack:.2f}), low survival ({survival_prob:.2f}), fallback to OD"
    
    else:
        # Low slack - deadline at risk, use on-demand
        return "ON_DEMAND_FALLBACK", f"Low slack ({slack:.2f}), must use on-demand to meet deadline"

def hourglass_get_workflow_deadline(workflow: str, task_id: str, total_exec_time: float) -> float:
    """
    Calculate workflow deadline (Hourglass paper Section 4.1).
    Deadline = workflow_start_time + (critical_path_time * DEADLINE_FACTOR)
    
    IMPORTANT: Only consider RECENT entries (last 30 minutes) to avoid using
    stale data from previous experiment runs.
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        # Get workflow start time from RECENT task executions only
        cursor.execute(
            """SELECT MIN(start_time) FROM serverless_workflows 
               WHERE workflow_name = %s 
               AND start_time > (UNIX_TIMESTAMP() - 1800)""",  # Last 30 minutes
            (workflow,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0]:
            workflow_start = float(result[0])
            # Critical path approximation: sum of all task exec times
            deadline = workflow_start + (total_exec_time * 60 * HOURGLASS_DEADLINE_FACTOR)
            return deadline
    except Exception as e:
        print(f"⚠️ [Hourglass] Error getting deadline: {e}")
    
    # Fallback: deadline is 1.5x total exec time from now
    return time.time() + (total_exec_time * 60 * HOURGLASS_DEADLINE_FACTOR)

def hourglass_log_state_change(
    workflow: str, task_id: str, uid: str, baseline: str,
    old_config: str, new_config: str, slack: float, reason: str
):
    """Log Hourglass configuration state transitions."""
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourglass_state_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                workflow_name VARCHAR(255),
                task_id VARCHAR(255),
                uuid VARCHAR(255),
                baseline VARCHAR(100),
                old_config VARCHAR(50),
                new_config VARCHAR(50),
                slack FLOAT,
                reason TEXT,
                timestamp DATETIME
            )
        """)
        cursor.execute(
            """INSERT INTO hourglass_state_log 
               (workflow_name, task_id, uuid, baseline, old_config, new_config, slack, reason, timestamp)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (workflow, task_id, uid, baseline, old_config, new_config, slack, reason)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"⏳ [Hourglass] State: {old_config} → {new_config} | Slack: {slack:.2f} | {reason}")
    except Exception as e:
        print(f"⚠️ [Hourglass] State log failed: {e}")

# ==============================================================================
# BAG-OF-TASKS IMPLEMENTATION (IEEE TCC 2023)
# Burstable VM scheduling with spot hibernation
# ==============================================================================
def bot_get_cpu_credits(pod_name: str) -> float:
    """
    Get current CPU credits for pod (BoT paper Section 3.1).
    Simulates T2/T3 burstable instance credit system.
    """
    current_time = time.time()
    
    if pod_name not in POD_CREDIT_CACHE:
        POD_CREDIT_CACHE[pod_name] = BOT_INITIAL_CREDITS
        POD_LAST_CREDIT_UPDATE[pod_name] = current_time
        return BOT_INITIAL_CREDITS
    
    # Calculate credits earned since last update
    last_update = POD_LAST_CREDIT_UPDATE.get(pod_name, current_time)
    elapsed_minutes = (current_time - last_update) / 60.0
    earned_credits = elapsed_minutes * BOT_CREDIT_EARN_RATE
    
    credits = POD_CREDIT_CACHE[pod_name] + earned_credits
    credits = min(credits, BOT_INITIAL_CREDITS * 2)  # Cap at 2x initial
    
    POD_CREDIT_CACHE[pod_name] = credits
    POD_LAST_CREDIT_UPDATE[pod_name] = current_time
    
    return credits

def bot_consume_credits(pod_name: str, burst_minutes: float) -> float:
    """Consume CPU credits for burst execution."""
    credits = bot_get_cpu_credits(pod_name)
    cost = burst_minutes * BOT_CREDIT_BURST_COST
    new_credits = max(0, credits - cost)
    POD_CREDIT_CACHE[pod_name] = new_credits
    return new_credits

def bot_can_burst(pod_name: str) -> Tuple[bool, float]:
    """
    Check if pod can use burst CPU (BoT paper Algorithm 2).
    
    Returns:
        (can_burst, available_credits)
    """
    credits = bot_get_cpu_credits(pod_name)
    return credits >= BOT_MIN_CREDITS_FOR_BURST, credits

def bot_hibernate_state(
    workflow: str, task_id: str, uid: str, baseline: str,
    state_data: Dict[str, Any], pod_name: str
) -> bool:
    """
    Hibernate task state to database (BoT paper Section 3.3).
    Simulates spot instance hibernation feature.
    
    Unlike regular checkpointing, hibernation preserves:
    - Full memory state (simulated via comprehensive state dict)
    - CPU credits
    - Execution progress
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_hibernation_state (
                id INT AUTO_INCREMENT PRIMARY KEY,
                workflow_name VARCHAR(255),
                task_id VARCHAR(255),
                uuid VARCHAR(255),
                baseline VARCHAR(100),
                pod_name VARCHAR(255),
                cpu_credits FLOAT,
                state_data JSON,
                hibernated_at DATETIME,
                resumed_at DATETIME DEFAULT NULL,
                UNIQUE KEY unique_hibernate (workflow_name, task_id, uuid)
            )
        """)
        
        # Get current credits
        credits = bot_get_cpu_credits(pod_name)
        
        # Store comprehensive state
        full_state = {
            **state_data,
            "cpu_credits": credits,
            "pod_name": pod_name,
            "hibernation_time": time.time()
        }
        
        cursor.execute(
            """INSERT INTO bot_hibernation_state 
               (workflow_name, task_id, uuid, baseline, pod_name, cpu_credits, state_data, hibernated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
               ON DUPLICATE KEY UPDATE 
               cpu_credits = VALUES(cpu_credits),
               state_data = VALUES(state_data),
               hibernated_at = NOW(),
               resumed_at = NULL""",
            (workflow, task_id, uid, baseline, pod_name, credits, json.dumps(full_state))
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💤 [BoT] Hibernated {task_id} for UID {uid} | Credits: {credits:.1f}")
        return True
    except Exception as e:
        print(f"❌ [BoT] Hibernation failed: {e}")
        return False

def bot_resume_state(workflow: str, task_id: str, uid: str) -> Optional[Dict[str, Any]]:
    """
    Resume task from hibernation (BoT paper Section 3.3).
    
    Returns:
        Restored state dict or None if no hibernation found
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """SELECT * FROM bot_hibernation_state 
               WHERE workflow_name = %s AND task_id = %s AND uuid = %s AND resumed_at IS NULL
               ORDER BY hibernated_at DESC LIMIT 1""",
            (workflow, task_id, uid)
        )
        result = cursor.fetchone()
        
        if result:
            # Mark as resumed
            cursor.execute(
                "UPDATE bot_hibernation_state SET resumed_at = NOW() WHERE id = %s",
                (result['id'],)
            )
            conn.commit()
            
            # Restore CPU credits
            pod_name = result.get('pod_name', 'unknown')
            credits = result.get('cpu_credits', BOT_INITIAL_CREDITS)
            POD_CREDIT_CACHE[pod_name] = credits
            POD_LAST_CREDIT_UPDATE[pod_name] = time.time()
            
            state_data = json.loads(result['state_data']) if result['state_data'] else {}
            print(f"🔄 [BoT] Resumed {task_id} for UID {uid} | Credits restored: {credits:.1f}")
            cursor.close()
            conn.close()
            return state_data
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ [BoT] Resume failed: {e}")
    
    return None

def bot_log_scheduling_decision(
    workflow: str, task_id: str, uid: str, baseline: str,
    decision: str, credits: float, survival_prob: float, details: Dict[str, Any]
):
    """Log Bag-of-Tasks scheduling decision."""
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_scheduling_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                workflow_name VARCHAR(255),
                task_id VARCHAR(255),
                uuid VARCHAR(255),
                baseline VARCHAR(100),
                decision VARCHAR(50),
                cpu_credits FLOAT,
                survival_prob FLOAT,
                details JSON,
                timestamp DATETIME
            )
        """)
        cursor.execute(
            """INSERT INTO bot_scheduling_log 
               (workflow_name, task_id, uuid, baseline, decision, cpu_credits, survival_prob, details, timestamp)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (workflow, task_id, uid, baseline, decision, credits, survival_prob, json.dumps(details))
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ [BoT] Scheduling log failed: {e}")

def mscheduler_decide(makespan_s: float, interval_s: float, dump_s: float, price_od: float, price_spot: float, overhead_sr: float):
    """Tiny Alg-1 style: cost = price * effective_makespan. SPOT adds ckpt overhead."""
    if makespan_s <= 0: makespan_s = 1.0
    if interval_s <= 0: interval_s = 1800.0
    n_ckp = int(makespan_s // interval_s)
    time_ckp = n_ckp * (dump_s * max(overhead_sr, 1.0))
    makespan_spot = makespan_s + time_ckp
    makespan_od   = makespan_s

    cost_spot = price_spot * (makespan_spot / 3600.0)
    cost_od   = price_od   * (makespan_od   / 3600.0)

    choice = 'spot' if cost_spot < cost_od else 'od'
    details = {
        "n_checkpoints": n_ckp,
        "time_ckp_s": round(time_ckp, 3),
        "makespan_spot_s": round(makespan_spot, 3),
        "makespan_od_s": round(makespan_od, 3),
        "cost_spot": round(cost_spot, 5),
        "cost_od": round(cost_od, 5),
        "overhead_sr": overhead_sr,
        "interval_s": interval_s,
        "dump_s": dump_s,
        "price_spot": price_spot,
        "price_od": price_od,
    }
    return choice, details

@app.route('/', methods=['POST'])
def main():
    # Core identity + chain (ConfigMap-first)
    task_id        = read_cfg("TASK_ID", "taskX")
    workflow       = read_cfg("WORKFLOW_ID", "unknown")
    baseline       = read_cfg("BASELINE", "static").lower()
    next_task_urls = read_cfg("NEXT_TASK_URL", "")

    # Baseline toggles (ConfigMap-first, then env)
    snape_enabled      = read_cfg("SNAPE_ENABLED", "false").lower() == "true"
    ofp_tm_enabled     = read_cfg("OFP_TM_ENABLED", "false").lower() == "true"
    bag_ckpt_strategy  = read_cfg("CKPT_STRATEGY", "")
    protean_scaling    = read_cfg("PROTEAN_SCALING", "false").lower() == "true"
    mscheduler_enabled = read_cfg("MSCHEDULER_ENABLED", "false").lower() == "true"
    multi_level_ckpt   = read_cfg("MULTI_LEVEL_CKPT", "false").lower() == "true"

    # Parameters
    AZ                 = read_cfg("AZ", "us-west-2a")
    INSTANCE_TYPE      = read_cfg("INSTANCE_TYPE", "v100")
    TASK_EXEC_MINUTES  = float(read_cfg("TASK_EXEC_TIME", "0.25"))
    SCALE_FACTOR       = float(read_cfg("SCALE_FACTOR", "1.25"))  # Protean knob

    # MScheduler knobs (defaults aligned to common settings)
    MSCHED_MAKESPAN_S  = float(read_cfg("MSCHED_MAKESPAN_S", str(TASK_EXEC_MINUTES * 60)))
    MSCHED_INTERVAL_S  = float(read_cfg("MSCHED_INTERVAL_S", "1800"))  # 30m
    MSCHED_DUMP_S      = float(read_cfg("MSCHED_DUMP_S", "36.0"))
    MSCHED_OVERHEAD_SR = float(read_cfg("MSCHED_OVERHEAD_SR", "1.0"))
    MSCHED_PRICE_OD    = float(read_cfg("MSCHED_PRICE_OD", "3.00"))
    MSCHED_PRICE_SPOT  = float(read_cfg("MSCHED_PRICE_SPOT", "0.90"))

    # Pod identity: HOSTNAME is reliable
    pod_name = os.environ.get("HOSTNAME", read_cfg("POD_NAME", "unknown-pod"))

    # Per-pod start time cache
    if pod_name not in POD_START_TIME_CACHE:
        POD_START_TIME_CACHE[pod_name] = time.time()
    pod_start_time = POD_START_TIME_CACHE[pod_name]

    input_data = request.get_json(force=True, silent=True) or {}
    uid = input_data.get("uid", str(uuid.uuid4()))
    print(f"🔁 [{baseline.upper()}] Executing {task_id} | UID: {uid} | Pod: {pod_name} | Expected Exec Time: {TASK_EXEC_MINUTES:.2f} min")

    start_time = int(time.time())

    # Emulated exec duration with optional Protean scaling
    exec_duration_seconds = random.uniform(5, 10)
    if protean_scaling:
        exec_duration_seconds = exec_duration_seconds / max(SCALE_FACTOR, 0.0001)
        print(f"⚡ [Protean] Scaling factor applied ({SCALE_FACTOR}), new exec time: {exec_duration_seconds:.2f}s")
    time.sleep(exec_duration_seconds)

    result = {"output": f"result from {task_id}", "uid": uid}
    target_func = task_id
    details = {"target_function": target_func}
    did_checkpoint = False

    # --- Baselines logic ---
    # MScheduler: pick market (spot vs OD) using cost + checkpoint overheads
    if mscheduler_enabled:
        choice, msched_details = mscheduler_decide(
            makespan_s=MSCHED_MAKESPAN_S,
            interval_s=MSCHED_INTERVAL_S,
            dump_s=MSCHED_DUMP_S,
            price_od=MSCHED_PRICE_OD,
            price_spot=MSCHED_PRICE_SPOT,
            overhead_sr=MSCHED_OVERHEAD_SR,
        )
        msched_details.update({"AZ": AZ, "INSTANCE_TYPE": INSTANCE_TYPE})
        decision = "CHOOSE_SPOT" if choice == "spot" else "CHOOSE_OD"
        log_decision(workflow, task_id, uid, baseline, decision, msched_details)
        print(f"🧭 [MScheduler] Decision: {decision} ({msched_details})")

    # Snape: rolling-window eviction estimate -> spot scaling decisions (no checkpoints)
    if snape_enabled:
        rolling_window_minutes = 60
        recent_lifetimes = get_recent_lifetimes_from_db(AZ, INSTANCE_TYPE, window=200)
        shuffled = recent_lifetimes[:]; random.shuffle(shuffled)
        rolling_lifetimes, total_time = [], 0
        for lt in shuffled:
            if total_time + lt > rolling_window_minutes * 60: break
            rolling_lifetimes.append(lt); total_time += lt
        if len(rolling_lifetimes) > 3:
            for _ in range(random.randint(0, min(2, len(rolling_lifetimes)//4))):
                del rolling_lifetimes[random.randrange(len(rolling_lifetimes))]
        eviction_rate_prediction = 0.0
        if rolling_lifetimes:
            jitter = random.uniform(-0.05, 0.05) * TASK_EXEC_MINUTES * 60
            eviction_threshold = TASK_EXEC_MINUTES * 60 + jitter
            evictions_in_window = sum(1 for lt in rolling_lifetimes if lt < eviction_threshold)
            eviction_rate_prediction = evictions_in_window / len(rolling_lifetimes)
            print(f"[Snape] Rolling window eviction rate prediction: {eviction_rate_prediction:.4f}")
        details["predicted_eviction_rate"] = round(eviction_rate_prediction, 4)
        if eviction_rate_prediction > 0.4:
            log_decision(workflow, task_id, uid, baseline, "SCALE_DOWN_SPOT", details)
        elif eviction_rate_prediction > 0.1:
            log_decision(workflow, task_id, uid, baseline, "SCALE_UP_SPOT", details)

    # ==========================================================================
    # HOURGLASS BASELINE (EuroSys 2019)
    # Slack-aware provisioning with on-demand fallback
    # ==========================================================================
    elif bag_ckpt_strategy == "hourglass":
        cdf = load_cdf_from_db(AZ, INSTANCE_TYPE)
        age_min = (time.time() - pod_start_time) / 60.0
        survival_prob = get_survival_prob(cdf, age_min + TASK_EXEC_MINUTES)
        
        # Get total workflow execution time for deadline calculation
        total_workflow_exec = float(read_cfg("TOTAL_WORKFLOW_EXEC_TIME", str(TASK_EXEC_MINUTES * 6)))
        
        # Calculate deadline and slack
        deadline = hourglass_get_workflow_deadline(workflow, task_id, total_workflow_exec)
        remaining_exec = total_workflow_exec * 60 - (time.time() - pod_start_time)
        remaining_exec = max(remaining_exec, TASK_EXEC_MINUTES * 60)  # At least this task
        
        slack = hourglass_calculate_slack(time.time(), deadline, remaining_exec)
        
        # Get current configuration state
        current_config = read_cfg("HOURGLASS_CONFIG", "SPOT_AGGRESSIVE")
        
        # Select new configuration based on slack and survival
        new_config, reason = hourglass_select_configuration(slack, survival_prob, current_config)
        
        details.update({
            "survival_probability": round(survival_prob, 4),
            "slack": round(slack, 4),
            "deadline": deadline,
            "remaining_exec_sec": round(remaining_exec, 2),
            "config": new_config
        })
        
        # Log state transition if config changed
        if new_config != current_config:
            hourglass_log_state_change(
                workflow, task_id, uid, baseline,
                current_config, new_config, slack, reason
            )
        
        # Take action based on configuration
        if new_config == "ON_DEMAND_FALLBACK":
            # Deadline at risk - checkpoint and signal need for on-demand
            print(f"⚠️ [Hourglass] DEADLINE RISK - slack={slack:.2f}, switching to on-demand mode")
            checkpoint_to_db(task_id, uid, result, "hourglass_deadline_critical", workflow, baseline)
            did_checkpoint = True
            log_decision(workflow, task_id, uid, baseline, "ON_DEMAND_FALLBACK", details)
            
        elif new_config == "SPOT_CONSERVATIVE":
            # Medium risk - checkpoint for safety
            if survival_prob < HOURGLASS_CHECKPOINT_PROB_THRESHOLD:
                print(f"⌛ [Hourglass] Conservative mode - checkpointing (survival={survival_prob:.2f})")
                checkpoint_to_db(task_id, uid, result, "hourglass_conservative", workflow, baseline)
                did_checkpoint = True
            log_decision(workflow, task_id, uid, baseline, "SPOT_CONSERVATIVE", details)
            
        else:  # SPOT_AGGRESSIVE
            # High slack - proceed without extra checkpointing
            print(f"✅ [Hourglass] Aggressive mode - slack={slack:.2f}, survival={survival_prob:.2f}")
            log_decision(workflow, task_id, uid, baseline, "SPOT_AGGRESSIVE", details)

    # ==========================================================================
    # BAG-OF-TASKS BASELINE (IEEE TCC 2023)
    # Burstable VM scheduling with spot hibernation + probabilistic placement
    # ==========================================================================
    elif bag_ckpt_strategy == "probabilistic":
        cdf = load_cdf_from_db(AZ, INSTANCE_TYPE)
        age_min = (time.time() - pod_start_time) / 60.0
        survival_prob = get_survival_prob(cdf, age_min + TASK_EXEC_MINUTES)
        
        # Calculate survival for task duration (for logging)
        task_duration_survival = get_survival_prob(cdf, TASK_EXEC_MINUTES)
        
        # Calculate WORKFLOW COMPLETION probability
        # KEY FIX: Use survival_prob (which includes pod age) not task_duration_survival
        # The CDF has minimum lifetime of ~10 min, so short tasks always return 1.0
        # But pod age accumulates, so survival_prob decreases over time
        total_tasks = int(read_cfg("TOTAL_WORKFLOW_TASKS", "6"))  # Default 6 tasks
        try:
            current_task_num = int(task_id.replace("task", "")) if task_id else 1
        except:
            current_task_num = 1
        remaining_tasks = max(1, total_tasks - current_task_num + 1)
        
        # Workflow completion probability: use survival_prob (accounts for pod age!)
        # This ensures older pods have lower completion probability
        workflow_completion_prob = survival_prob ** remaining_tasks
        
        # Check for hibernated state to resume
        resumed_state = bot_resume_state(workflow, task_id, uid)
        if resumed_state:
            print(f"🔄 [BoT] Resuming from hibernation: {resumed_state.get('progress', 'unknown')}")
        
        # Get CPU credits
        can_burst, credits = bot_can_burst(pod_name)
        
        details.update({
            "survival_probability": round(survival_prob, 4),
            "task_duration_survival": round(task_duration_survival, 4),
            "workflow_completion_prob": round(workflow_completion_prob, 4),
            "remaining_tasks": remaining_tasks,
            "cpu_credits": round(credits, 2),
            "can_burst": can_burst,
            "resumed_from_hibernation": resumed_state is not None
        })
        
        # ==========================================================================
        # VM TYPE DETERMINATION
        # Philosophy: Always START on SPOT (cheap), migrate to burstable via hibernation
        # Only use burstable if resumed from hibernation, never proactively
        # This maximizes cost savings while using hibernation for protection
        # ==========================================================================
        vm_type = "spot"  # Always start on spot
        
        if resumed_state is not None:
            # Resumed from hibernation → BURSTABLE VM (protected)
            vm_type = "burstable"
            details["placement_reason"] = "resumed_from_hibernation"
            print(f"🛡️ [BoT] Running on BURSTABLE VM (resumed from hibernation)")
        else:
            # Default: start on SPOT (cheap, can be preempted)
            # Migration to burstable happens via hibernation when completion_prob drops
            vm_type = "spot"
            details["placement_reason"] = "default_spot"
            print(f"⚡ [BoT] Running on SPOT VM (survival={survival_prob:.2f}, workflow_completion={workflow_completion_prob:.2f})")
        
        # Check for credit exhaustion on burstable → fall back to on-demand
        if vm_type == "burstable" and credits < BOT_CREDIT_EXHAUSTION_THRESHOLD:
            vm_type = "on-demand"
            details["placement_reason"] = "credit_exhaustion"
            print(f"💸 [BoT] Credits exhausted ({credits:.1f} < {BOT_CREDIT_EXHAUSTION_THRESHOLD}) → ON-DEMAND fallback")
        
        # Set flags based on VM type
        details["vm_type"] = vm_type
        details["burst_used"] = (vm_type == "burstable")
        details["is_burstable_vm"] = (vm_type == "burstable")
        details["is_on_demand"] = (vm_type == "on-demand")
        
        # ==========================================================================
        # SCHEDULING DECISION (hibernate, checkpoint, or normal)
        # KEY: Use workflow_completion_prob for decisions (accounts for remaining tasks)
        # ==========================================================================
        if workflow_completion_prob < BOT_HIBERNATION_THRESHOLD and vm_type == "spot":
            # Low workflow completion probability on SPOT → hibernate for migration to burstable
            # This triggers even for short tasks if many tasks remain
            print(f"💤 [BoT] Low completion prob ({workflow_completion_prob:.2f} < {BOT_HIBERNATION_THRESHOLD}, {remaining_tasks} tasks left) - hibernating")
            state_to_hibernate = {
                "result": result,
                "progress": "pre_completion",
                "exec_duration": exec_duration_seconds,
                "input_data": input_data
            }
            bot_hibernate_state(workflow, task_id, uid, baseline, state_to_hibernate, pod_name)
            did_checkpoint = True
            bot_log_scheduling_decision(workflow, task_id, uid, baseline, "HIBERNATE", credits, survival_prob, details)
            log_decision(workflow, task_id, uid, baseline, "HIBERNATE", details)
            
        elif workflow_completion_prob < BOT_CHECKPOINT_THRESHOLD:
            # Medium completion probability - checkpoint for safety
            print(f"⌛ [BoT] Medium completion prob ({workflow_completion_prob:.2f}) - checkpointing ({vm_type.upper()} VM)")
            checkpoint_to_db(task_id, uid, result, "bot_checkpoint", workflow, baseline)
            did_checkpoint = True
            
            # CPU burst only on burstable VMs with sufficient credits
            if vm_type == "burstable" and can_burst and credits >= BOT_MIN_CREDITS_FOR_BURST:
                bot_consume_credits(pod_name, TASK_EXEC_MINUTES)
                details["cpu_burst_active"] = True
                print(f"⚡ [BoT] Using CPU burst (credits: {credits:.1f} → {bot_get_cpu_credits(pod_name):.1f})")
            
            decision = "CHECKPOINT_BURST" if details.get("cpu_burst_active") else "CHECKPOINT"
            bot_log_scheduling_decision(workflow, task_id, uid, baseline, decision, credits, survival_prob, details)
            
        else:
            # Good survival - proceed normally
            # Opportunistic burst on burstable (30% chance)
            if vm_type == "burstable" and can_burst and credits >= BOT_MIN_CREDITS_FOR_BURST and random.random() < 0.3:
                bot_consume_credits(pod_name, TASK_EXEC_MINUTES * 0.5)
                details["cpu_burst_active"] = True
                print(f"⚡ [BoT] Opportunistic CPU burst (credits: {credits:.1f})")
            
            bot_log_scheduling_decision(workflow, task_id, uid, baseline, "NORMAL", credits, survival_prob, details)
            print(f"✅ [BoT] Normal execution on {vm_type.upper()} VM - survival={survival_prob:.2f}")

    # OFP-TM baseline
    elif ofp_tm_enabled:
        cdf = load_cdf_from_db(AZ, INSTANCE_TYPE)
        age_min = (time.time() - pod_start_time) / 60.0
        survival_prob = get_survival_prob(cdf, age_min + TASK_EXEC_MINUTES)
        details["survival_probability"] = round(survival_prob, 4)

        if survival_prob < 0.5:
            log_decision(workflow, task_id, uid, baseline, "SCALE_UP", details)

    # Multi-level checkpoint baseline: local + remote
    if multi_level_ckpt:
        checkpoint_to_db(task_id, uid, result, "local_memory", workflow, baseline)
        time.sleep(0.1)
        checkpoint_to_db(task_id, uid, result, "remote_persistent", workflow, baseline)
        did_checkpoint = True

    # Static checkpoint baseline (optional extra mid-task point — comment in/out as desired)
    if baseline == "static_cp":
        # Mid-task checkpoint label — keeps distinct from final default
        checkpoint_to_db(task_id, uid, result, "static_cp_mid", workflow, baseline)
        did_checkpoint = True

    # Always keep a final "default" checkpoint so retry_service can resume deterministically
    checkpoint_to_db(task_id, uid, result, "default", workflow, baseline)

    end_time = int(time.time())
    log_execution(workflow, task_id, uid, start_time, end_time, 200, baseline)

    final_status = 200
    if next_task_urls:
        try:
            primary_url = random.choice([u.strip() for u in next_task_urls.split(',') if u.strip()])
            # If MScheduler decided, append market for traceability
            if mscheduler_enabled:
                market = "spot" if 'decision' in locals() and decision == "CHOOSE_SPOT" else "od"
                sep = "&" if "?" in primary_url else "?"
                primary_url = f"{primary_url}{sep}market={market}"
                print(f"🧩 [MScheduler] Forwarding with market={market}")
            requests.post(primary_url, json={"uid": uid}, timeout=30)
            print(f"➡️  Forwarded to {primary_url}")
        except Exception as e:
            print(f"❌ Forwarding failed: {e}")
            final_status = 500

    return f"{task_id} completed with UID {uid}", final_status

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8888, debug=False)
