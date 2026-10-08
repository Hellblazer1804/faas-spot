#!/usr/bin/env python3
import time
import json
import subprocess
import argparse
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import math
import os
import mysql.connector
import pandas as pd
import numpy as np
import signal
import sys
import atexit

# ============================================================================
# RL ADAPTIVE SCALER IMPORTS
# ============================================================================
try:
    from rl_config import (
        ACTIONS, N_ACTIONS, COLD_START_CONFIG, TIMING_CONFIG,
        get_allowed_actions as rl_get_allowed_actions, LOGGING_CONFIG as RL_LOGGING_CONFIG
    )
    from rl_state import (
        get_state_tracker, reset_state_tracker, get_context_vector,
        is_cold_start_complete, get_allowed_warmup_actions
    )
    from rl_bandit import (
        get_bandit, reset_bandit, save_bandit_state, calculate_reward
    )
    RL_AVAILABLE = True
    print("✅ RL Adaptive Scaler modules loaded successfully")
except ImportError as e:
    RL_AVAILABLE = False
    print(f"⚠️ RL modules not available: {e}. RL mode disabled.")

# ============================================================================
# SIGNAL HANDLING FOR GRACEFUL SHUTDOWN (saves RL state)
# ============================================================================
def _rl_cleanup_handler(signum=None, frame=None):
    """Save RL state on exit."""
    if RL_AVAILABLE:
        try:
            save_bandit_state()
            print("\n💾 RL bandit state saved on exit")
        except Exception as e:
            print(f"\n⚠️ Failed to save RL state on exit: {e}")
    if signum is not None:
        sys.exit(0)

# Register cleanup handlers
if RL_AVAILABLE:
    signal.signal(signal.SIGINT, _rl_cleanup_handler)
    signal.signal(signal.SIGTERM, _rl_cleanup_handler)
    atexit.register(_rl_cleanup_handler)

NAMESPACE = "default"

# Database configuration for success rate monitoring
DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}
EXPERIMENT_TAG = os.getenv("EXPERIMENT_TAG", "main_experiment")

# ============================================================================
# ENV OVERRIDES (Sensitivity / Ablation support)
# ============================================================================
def _env_flag(name: str) -> bool:
    val = os.getenv(name, "").strip().lower()
    return val in {"1", "true", "yes", "y", "on"}

def _parse_env_percent(raw: str):
    if not raw:
        return None
    try:
        val = float(raw)
    except Exception:
        return None
    if val > 1.0:
        val = val / 100.0
    return max(0.0, min(1.0, val))

def _parse_env_int(raw: str, default: int):
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default

def _parse_env_workflow_set(raw: str):
    if not raw:
        return set()
    return {w.strip() for w in raw.split(",") if w.strip()}

CHECKSCALE_BUDGET_OVERRIDE_PERCENT = _parse_env_percent(os.getenv("CHECKSCALE_BUDGET_OVERRIDE_PERCENT"))
CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS = _parse_env_workflow_set(os.getenv("CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS"))
CHECKSCALE_KM_THRESHOLD_OVERRIDE = _parse_env_percent(os.getenv("CHECKSCALE_KM_THRESHOLD"))
CHECKSCALE_KM_WARNING_OVERRIDE = _parse_env_percent(os.getenv("CHECKSCALE_KM_WARNING_THRESHOLD"))
CHECKSCALE_KM_CRITICAL_OVERRIDE = _parse_env_percent(os.getenv("CHECKSCALE_KM_CRITICAL_THRESHOLD"))
# ============================================================================
# SCALING EFFICIENCY TRACKING - Prevents runaway scaling without improvement
# ============================================================================
# Track success rate history to detect if scaling is actually helping
scaling_efficiency_tracker = {
    'last_success_rate': None,
    'last_total_pods': 0,
    'ticks_without_improvement': 0,
    'max_ticks_without_improvement': 10,  # Only flag after 10 ticks (relaxed, monitoring only)
    'scaling_paused': False,
    'pause_reason': None
}

# Track Pareto-based savings when scaling is deferred due to cost
pareto_savings_tracker = {
    'saved_dollars_per_hour': 0.0,
    'skipped_scale_events': 0
}

# ============================================================================
# ROI-BASED SCALING DECISION FRAMEWORK
# ============================================================================
# Track return on investment for scaling decisions to make data-driven choices
# ROI = (success_improvement) / (cost_increase)
# A positive ROI means scaling is helping; negative means we're wasting resources

roi_tracker = {
    # History of scaling decisions and their outcomes
    'history': [],  # List of {timestamp, pods_before, pods_after, success_before, success_after, cost_before, cost_after}
    'window_size': 5,  # Look at last 5 data points for ROI calculation
    
    # Cumulative metrics for current experiment
    'total_pods_added': 0,
    'total_success_gained': 0.0,  # Percentage points
    'total_cost_spent': 0.0,  # Dollars
    
    # ROI thresholds for decision making
    'min_roi_threshold': 0.5,  # Require at least 0.5% success gain per $1 spent
    'negative_roi_ticks': 0,   # Count of consecutive negative ROI ticks
    'max_negative_roi_ticks': 3,  # After 3 negative ROI ticks, throttle scaling
    
    # Current state
    'last_snapshot': None,  # {pods, success, cost, timestamp}
    'scaling_throttled': False,
    'throttle_reason': None,
    'throttle_factor': 1.0,  # 1.0 = full scaling, 0.5 = 50% scaling
}

def reset_roi_tracker():
    """Reset ROI tracker at start of experiment."""
    global roi_tracker
    roi_tracker = {
        'history': [],
        'window_size': 5,
        'total_pods_added': 0,
        'total_success_gained': 0.0,
        'total_cost_spent': 0.0,
        'min_roi_threshold': 0.5,
        'negative_roi_ticks': 0,
        'max_negative_roi_ticks': 3,
        'last_snapshot': None,
        'scaling_throttled': False,
        'throttle_reason': None,
        'throttle_factor': 1.0,
    }

# ============================================================================
# GLOBAL SCALER COOLDOWN - Wait for retry service, then cooldown after scaling
# ============================================================================
# Strategy: Let retry service do its job first, then when we scale, take a break
# This reduces churn and gives the system time to stabilize

# After ANY scaling event, cooldown the entire scaler (not per-task)
GLOBAL_SCALER_COOLDOWN_SECONDS = 120  # 2 minutes global cooldown after any scaling

# Minimum UIDs that must have hit retry limit before scaler can activate
MIN_RETRY_EXHAUSTED_UIDS = 12  # At least 12 UIDs must have exhausted retries

# Retry-gate overrides (sensitivity/ablation support)
RETRY_EXHAUST_DISABLE_WORKFLOWS = {'wf-5'}
RETRY_EXHAUST_DISABLE_WORKFLOWS |= _parse_env_workflow_set(os.getenv("CHECKSCALE_RETRY_EXHAUST_DISABLE_WORKFLOWS"))
RETRY_EXHAUST_DISABLE_ALL = _env_flag("CHECKSCALE_RETRY_EXHAUST_DISABLE_ALL")
RETRY_EXHAUST_DISABLE_MIN_RUNS = _parse_env_int(os.getenv("CHECKSCALE_RETRY_EXHAUST_DISABLE_MIN_RUNS"), 20)

# Adaptive bypass: allow scaling early for critical workflows when success lags
RETRY_EXHAUST_BYPASS_WORKFLOWS = {'wf-5', 'wf-9'}
RETRY_EXHAUST_BYPASS_FRAC = 0.85
RETRY_EXHAUST_BYPASS_MIN_RUNS = 20

global_scaler_cooldown_tracker = {
    'last_scaling_time': None,           # Timestamp of last scaling event
    'retry_service_exhausted': False,    # True when retry service has hit its limit
    'exhausted_uids_count': 0,           # Number of UIDs that have exhausted retries
    'scaling_enabled': False,            # Master switch - only True after retry exhaustion
    'total_scaling_events': 0,           # Count of scaling events this experiment
}

def reset_global_scaler_cooldown():
    """Reset global scaler cooldown tracker at start of experiment."""
    global global_scaler_cooldown_tracker
    global_scaler_cooldown_tracker = {
        'last_scaling_time': None,
        'retry_service_exhausted': False,
        'exhausted_uids_count': 0,
        'scaling_enabled': False,
        'total_scaling_events': 0,
    }
    print(f"🔄 Global scaler cooldown reset: waiting for retry service to exhaust {MIN_RETRY_EXHAUSTED_UIDS} UIDs")

def check_retry_service_exhausted(workflow: str, baseline: str = None) -> tuple:
    """
    Check if the retry service has UIDs that have exhausted their retry limit.
    
    Returns:
        (is_exhausted, exhausted_count): True if enough UIDs have hit retry limit
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Query: find UIDs where retry_count >= max retries (workflow-specific)
        # Default max retries is 3, but some workflows have higher limits
        workflow_max_retries = {
            'wf-1': 10, 'wf-2': 15, 'wf-3': 12, 'wf-4': 12,
            'wf-5': 20, 'wf-6': 20, 'wf-7': 12, 'wf-8': 25, 'wf-9': 25
        }
        max_retries = workflow_max_retries.get(workflow, 3)
        
        query = """
            SELECT COUNT(DISTINCT uid) as exhausted_count
            FROM checkpoint_retries
            WHERE workflow_name = %s
            AND retry_count >= %s
        """
        params = [workflow, max_retries]
        
        if baseline:
            query += " AND baseline = %s"
            params.append(baseline)
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        exhausted_count = row[0] if row else 0
        
        cursor.close()
        conn.close()
        
        return exhausted_count >= MIN_RETRY_EXHAUSTED_UIDS, exhausted_count
        
    except Exception as e:
        print(f"⚠️ Error checking retry exhaustion: {e}")
        # If we can't check, be conservative and allow scaling after warmup
        return False, 0

def record_global_scaling_event():
    """Record that a scaling event occurred, triggering global cooldown."""
    global global_scaler_cooldown_tracker
    global_scaler_cooldown_tracker['last_scaling_time'] = time.time()
    global_scaler_cooldown_tracker['total_scaling_events'] += 1
    print(f"⏸️ GLOBAL COOLDOWN: Scaling event #{global_scaler_cooldown_tracker['total_scaling_events']} - "
          f"scaler on cooldown for {GLOBAL_SCALER_COOLDOWN_SECONDS}s")

def can_scale_globally(workflow: str = None, baseline: str = None) -> tuple:
    """
    Check if global scaler cooldown allows scaling.
    
    Returns:
        (can_scale, reason): Whether scaling is allowed and why
    """
    global global_scaler_cooldown_tracker
    
    # First, check if retry service has exhausted enough UIDs
    if not global_scaler_cooldown_tracker['scaling_enabled']:
        retry_gate_disabled = RETRY_EXHAUST_DISABLE_ALL or (workflow in RETRY_EXHAUST_DISABLE_WORKFLOWS)
        if retry_gate_disabled:
            last_success_runs = budget_envelope_tracker.get('last_success_runs', 0)
            if last_success_runs >= RETRY_EXHAUST_DISABLE_MIN_RUNS:
                global_scaler_cooldown_tracker['scaling_enabled'] = True
                global_scaler_cooldown_tracker['retry_service_exhausted'] = False
                print(f"⚠️ RETRY EXHAUST DISABLED: {workflow} scaling enabled "
                      f"(runs={last_success_runs}≥{RETRY_EXHAUST_DISABLE_MIN_RUNS})")

        if not global_scaler_cooldown_tracker['scaling_enabled']:
            # Adaptive bypass for longest workflows when success is lagging
            if workflow in RETRY_EXHAUST_BYPASS_WORKFLOWS:
                last_success_rate = budget_envelope_tracker.get('last_success_rate')
                last_success_runs = budget_envelope_tracker.get('last_success_runs', 0)
                target_success = get_target_success_rate(workflow)
                if (last_success_rate is not None and target_success and
                        last_success_runs >= RETRY_EXHAUST_BYPASS_MIN_RUNS and
                        last_success_rate < target_success * RETRY_EXHAUST_BYPASS_FRAC):
                    global_scaler_cooldown_tracker['scaling_enabled'] = True
                    global_scaler_cooldown_tracker['retry_service_exhausted'] = False
                    print(f"⚠️ RETRY EXHAUST BYPASS: {workflow} success "
                          f"{last_success_rate*100:.1f}%<{target_success*RETRY_EXHAUST_BYPASS_FRAC*100:.1f}% "
                          f"(runs={last_success_runs}) → scaler enabled early")
                    return True, "Bypass retry-exhaust gate (success lagging)"

            is_exhausted, exhausted_count = check_retry_service_exhausted(workflow, baseline)
            global_scaler_cooldown_tracker['exhausted_uids_count'] = exhausted_count
            
            if is_exhausted:
                global_scaler_cooldown_tracker['retry_service_exhausted'] = True
                global_scaler_cooldown_tracker['scaling_enabled'] = True
                print(f"✅ RETRY SERVICE LIMIT REACHED: {exhausted_count} UIDs exhausted retries - scaler now active")
            else:
                return False, f"Waiting for retry service ({exhausted_count}/{MIN_RETRY_EXHAUSTED_UIDS} UIDs exhausted)"
    
    # Now check global cooldown
    if global_scaler_cooldown_tracker['last_scaling_time'] is None:
        return True, "No previous scaling - allowed"
    
    time_since_scaling = time.time() - global_scaler_cooldown_tracker['last_scaling_time']
    if time_since_scaling < GLOBAL_SCALER_COOLDOWN_SECONDS:
        remaining = int(GLOBAL_SCALER_COOLDOWN_SECONDS - time_since_scaling)
        return False, f"Global cooldown ({remaining}s remaining)"
    
    return True, "Cooldown expired - allowed"

def get_global_scaler_status() -> dict:
    """Get current status of global scaler cooldown."""
    return {
        'enabled': global_scaler_cooldown_tracker['scaling_enabled'],
        'retry_exhausted': global_scaler_cooldown_tracker['retry_service_exhausted'],
        'exhausted_uids': global_scaler_cooldown_tracker['exhausted_uids_count'],
        'last_scaling_time': global_scaler_cooldown_tracker['last_scaling_time'],
        'total_events': global_scaler_cooldown_tracker['total_scaling_events'],
        'in_cooldown': (global_scaler_cooldown_tracker['last_scaling_time'] is not None and 
                       time.time() - global_scaler_cooldown_tracker['last_scaling_time'] < GLOBAL_SCALER_COOLDOWN_SECONDS)
    }

# ============================================================================
# BUDGET ENVELOPE + CPSP (Cost Per Success Point) HYBRID MODEL
# ============================================================================
# Economics-inspired cost control that:
# 1. Sets budget as 50-70% of ON-DEMAND cost (calculated from spot cost traces)
# 2. Tracks cost efficiency per task (CPSP)
# 3. BLOCKS scaling when budget exhausted (not just throttle)
# 4. Prioritizes tasks with best historical CPSP
# 5. Retry service is the hero - scaler should be conservative

# Budget as percentage of on-demand cost (calculated from spot traces)
# Target: 50% of on-demand cost to win on cost (includes scaling, retry, churn)
# Reference: Total on-demand cost for full experiment is ~$60, so budget cap is ~$30
BUDGET_MIN_PERCENT = 0.40  # Minimum 40% of on-demand (strict cost control)
BUDGET_MAX_PERCENT = 0.65  # Maximum 60% of on-demand (for complex workflows)
BUDGET_DEFAULT_PERCENT = 0.50  # Default 50% of on-demand (strict cap per user requirement)

# Per-workflow budget overrides (complex workflows get more headroom)
WORKFLOW_BUDGET_PERCENT = {
    'wf-5': 0.65,  # 60% - complex deep DAG, needs more scaling headroom
    'wf-9': 0.65,  # 60% - most complex workflow, deepest DAG
    # All other workflows use BUDGET_DEFAULT_PERCENT (50%)
}

# Adaptive budget: boost only when success is lagging (avoid overscaling)
ADAPTIVE_BUDGET_ENABLED = True
ADAPTIVE_BUDGET_BOOST = 0.05  # +5% headroom when under target
ADAPTIVE_BUDGET_TRIGGER_FRAC = 0.85  # Trigger when success < 85% of target
ADAPTIVE_BUDGET_MIN_RUNS = 20  # Avoid boosting before enough samples
ADAPTIVE_BUDGET_WORKFLOWS = {'wf-5', 'wf-9'}

# Churn overhead: Previously used to account for pod respawning after emulator kills
# Now set to 1.0 because 50% budget cap already includes all costs (scaling, retry, churn)
CHURN_OVERHEAD_FACTOR = 1.0  # No additional overhead - 50% is the hard cap

# Estimated total experiment runtime (in REAL hours) - used to calculate cost budgets
# Costs are calculated using real time (pod_age_seconds), so budget must use real time too
# UPDATED from run8.log: Load generator sends 500 requests at concurrency=4, taking much longer than expected
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

# Default estimated MACHINES per workflow (from total_minscale / 3)
# Machine packing: 3 pods per machine, so cost = machines × price
# These are calculated from tasks.json total minscale values
WORKFLOW_ESTIMATED_MACHINES = {
    'wf-1': 2,    # total_minscale=4, machines=ceil(4/3)=2
    'wf-2': 3,    # total_minscale=7, machines=ceil(7/3)=3
    'wf-3': 4,    # total_minscale=11, machines=ceil(11/3)=4
    'wf-4': 4,    # total_minscale=11, machines=ceil(11/3)=4
    'wf-5': 11,   # total_minscale=31, machines=ceil(31/3)=11
    'wf-6': 12,   # total_minscale=36, machines=ceil(36/3)=12
    'wf-7': 5,    # total_minscale=15, machines=ceil(15/3)=5
    'wf-8': 14,   # total_minscale=42, machines=ceil(42/3)=14
    'wf-9': 16,   # total_minscale=46, machines=ceil(46/3)=16
}

# Machine packing factor (same as Emulator.py)
PODS_PER_MACHINE = 3

# Global on-demand price cache (loaded from spot cost CSV)
_ondemand_price_per_hour = None
_spot_price_per_hour = None

def load_spot_prices(az: str, instance: str) -> tuple:
    """
    Load spot and on-demand prices from the spot cost CSV.
    
    On-demand price is calculated from: SpotPrice / (1 - Savings/100)
    
    Args:
        az: Availability zone (e.g., 'us-west-2a')
        instance: Instance type (e.g., 'v100', 'k80')
    
    Returns:
        (avg_spot_price, avg_ondemand_price) in $/hour
    """
    global _ondemand_price_per_hour, _spot_price_per_hour
    
    # Check if already loaded
    if _ondemand_price_per_hour is not None:
        return _spot_price_per_hour, _ondemand_price_per_hour
    
    # Try to find the cost CSV file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cost_csv_path = os.path.join(script_dir, "spot-cost-csv", f"{az}_{instance}_cost.csv")
    
    if not os.path.exists(cost_csv_path):
        print(f"⚠️ Spot cost file not found: {cost_csv_path}")
        print(f"   Using default prices: spot=$1.00/hr, on-demand=$3.00/hr")
        _spot_price_per_hour = 1.0
        _ondemand_price_per_hour = 3.0
        return _spot_price_per_hour, _ondemand_price_per_hour
    
    try:
        df = pd.read_csv(cost_csv_path)
        avg_spot_price = df["SpotPrice ($)"].mean()
        avg_savings_pct = df["Savings (%)"].mean()
        
        # Calculate on-demand price from spot price and savings percentage
        # On-demand = SpotPrice / (1 - Savings/100)
        if avg_savings_pct < 100:
            avg_ondemand_price = avg_spot_price / (1 - avg_savings_pct / 100)
        else:
            avg_ondemand_price = avg_spot_price * 3  # Fallback: assume 3x spot
        
        _spot_price_per_hour = avg_spot_price
        _ondemand_price_per_hour = avg_ondemand_price
        
        print(f"💰 Prices loaded from {az}_{instance}_cost.csv:")
        print(f"   Spot: ${avg_spot_price:.4f}/hr, On-Demand: ${avg_ondemand_price:.4f}/hr")
        print(f"   Average savings: {avg_savings_pct:.1f}%")
        
        return avg_spot_price, avg_ondemand_price
        
    except Exception as e:
        print(f"⚠️ Error loading spot cost CSV: {e}")
        print(f"   Using default prices: spot=$1.00/hr, on-demand=$3.00/hr")
        _spot_price_per_hour = 1.0
        _ondemand_price_per_hour = 3.0
        return _spot_price_per_hour, _ondemand_price_per_hour

def get_budget_override_percent(workflow_name: str):
    if CHECKSCALE_BUDGET_OVERRIDE_PERCENT is None:
        return None
    if CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS and workflow_name not in CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS:
        return None
    return CHECKSCALE_BUDGET_OVERRIDE_PERCENT

def get_workflow_budget(workflow_name: str, ondemand_price_per_hour: float = None) -> float:
    """
    Calculate budget as 50-70% of estimated on-demand cost.
    
    Budget = OnDemandPrice * EstimatedRuntime * EstimatedMachines * BudgetPercent
    
    Machine packing: 3 pods share 1 machine, so we charge per machine, not per pod.
    
    Args:
        workflow_name: Workflow identifier
        ondemand_price_per_hour: On-demand price in $/hour (loaded from CSV if not provided)
    
    Returns:
        Budget in dollars
    """
    global _ondemand_price_per_hour
    
    if ondemand_price_per_hour is None:
        ondemand_price_per_hour = _ondemand_price_per_hour or 3.0  # Default fallback
    
    # Get workflow-specific estimates
    runtime_hours = WORKFLOW_ESTIMATED_RUNTIME_HOURS.get(workflow_name, 0.15)
    estimated_machines = WORKFLOW_ESTIMATED_MACHINES.get(workflow_name, 6)
    
    # Calculate on-demand cost estimate
    # OnDemandCost = Price/hr * Runtime(hrs) * Machines
    # Note: Price is per MACHINE (not per pod) since 3 pods share 1 machine
    ondemand_cost = ondemand_price_per_hour * runtime_hours * estimated_machines
    
    # Get per-workflow budget percentage (wf-5, wf-9 get 60%, others get 50%)
    budget_percent = WORKFLOW_BUDGET_PERCENT.get(workflow_name, BUDGET_DEFAULT_PERCENT)
    override_percent = get_budget_override_percent(workflow_name)
    if override_percent is not None:
        budget_percent = override_percent
    
    # Budget as percentage of on-demand cost
    budget = ondemand_cost * budget_percent * CHURN_OVERHEAD_FACTOR
    
    return budget, budget_percent  # Return both for logging

def compute_adaptive_budget_percent(workflow_name: str, base_percent: float,
                                    success_rate: float = None, total_runs: int = None) -> float:
    """Return adaptive budget percent based on success gap (to avoid overscaling)."""
    if not ADAPTIVE_BUDGET_ENABLED:
        return base_percent
    if workflow_name not in ADAPTIVE_BUDGET_WORKFLOWS:
        return base_percent
    if success_rate is None or total_runs is None:
        return base_percent
    if total_runs < ADAPTIVE_BUDGET_MIN_RUNS:
        return base_percent
    target_success = get_target_success_rate(workflow_name)
    if not target_success:
        return base_percent
    if success_rate < target_success * ADAPTIVE_BUDGET_TRIGGER_FRAC:
        return min(1.0, base_percent + ADAPTIVE_BUDGET_BOOST)
    return base_percent

def apply_adaptive_budget_envelope(workflow_name: str, success_rate: float, total_runs: int):
    """Adjust budget envelope mid-run if success lags."""
    global budget_envelope_tracker, _ondemand_price_per_hour
    base_percent = WORKFLOW_BUDGET_PERCENT.get(workflow_name, BUDGET_DEFAULT_PERCENT)
    override_percent = get_budget_override_percent(workflow_name)
    if override_percent is not None:
        base_percent = override_percent
    new_percent = compute_adaptive_budget_percent(workflow_name, base_percent, success_rate, total_runs)
    current_percent = budget_envelope_tracker.get('budget_percent', base_percent)
    if abs(new_percent - current_percent) < 1e-6:
        return
    estimated_ondemand_cost = budget_envelope_tracker.get('estimated_ondemand_cost')
    if estimated_ondemand_cost is None:
        ondemand_price = budget_envelope_tracker.get('ondemand_price_per_hour') or _ondemand_price_per_hour or 3.0
        runtime_hours = WORKFLOW_ESTIMATED_RUNTIME_HOURS.get(workflow_name, 0.15)
        estimated_machines = WORKFLOW_ESTIMATED_MACHINES.get(workflow_name, 6)
        estimated_ondemand_cost = ondemand_price * runtime_hours * estimated_machines
        budget_envelope_tracker['estimated_ondemand_cost'] = estimated_ondemand_cost
    budget_envelope_tracker['budget_percent'] = new_percent
    budget_envelope_tracker['budget'] = estimated_ondemand_cost * new_percent * CHURN_OVERHEAD_FACTOR
    print(f"💰 ADAPTIVE BUDGET: {workflow_name} budget adjusted to "
          f"{new_percent*100:.0f}% (success={success_rate*100:.1f}%, runs={total_runs})")

# CPSP thresholds: refuse scaling if historical cost per success point exceeds this
MAX_CPSP_THRESHOLD = 3.0  # $3 per percentage point of success is too expensive (tightened from $5)
CPSP_WINDOW_SIZE = 5      # Look at last 5 scaling events per task
CPSP_MIN_DATA_POINTS = 3  # Require at least 3 data points before blocking (tightened from 5)

# Success emergency threshold - ONLY bypass cost controls when success is CRITICALLY low
# Lowered threshold - only bypass when truly desperate (not during ramp-up)
SUCCESS_EMERGENCY_THRESHOLD = 0.10  # 10% - very strict, only bypass in dire situations
MIN_COMPLETIONS_FOR_EMERGENCY = 10  # Don't trigger emergency mode until we have some data

budget_envelope_tracker = {
    'workflow': None,
    'budget': 0.0,
    'spent': 0.0,
    'start_time': None,
    'budget_exhausted': False,
    'last_cost_rate': 0.0,  # $/hour
    'pods_reset_on_exhaustion': False,  # Track if we've reset pods after budget exhausted
}

cpsp_tracker = {
    # Per-task tracking: task_id -> {'total_cost': X, 'total_success_gain': Y, 'events': [...]}
    'tasks': {},
    'global_cpsp': 0.0,  # Overall cost per success point
}

def reset_budget_envelope(workflow_name, az: str = None, instance: str = None):
    """Reset budget envelope tracker at start of experiment."""
    global budget_envelope_tracker, _ondemand_price_per_hour
    
    # Load prices from spot cost CSV if az/instance provided
    if az and instance:
        spot_price, ondemand_price = load_spot_prices(az, instance)
    else:
        ondemand_price = _ondemand_price_per_hour or 3.0  # Default
    
    # Calculate budget based on on-demand price from spot traces
    budget, budget_percent = get_workflow_budget(workflow_name, ondemand_price)
    
    # Calculate estimated on-demand cost for logging (using machines, not pods)
    runtime_hours = WORKFLOW_ESTIMATED_RUNTIME_HOURS.get(workflow_name, 0.15)
    estimated_machines = WORKFLOW_ESTIMATED_MACHINES.get(workflow_name, 6)
    estimated_ondemand_cost = ondemand_price * runtime_hours * estimated_machines
    
    budget_envelope_tracker = {
        'workflow': workflow_name,
        'budget': budget,
        'budget_percent': budget_percent,
        'spent': 0.0,
        'start_time': time.time(),
        'budget_exhausted': False,
        'last_cost_rate': 0.0,
        'ondemand_price_per_hour': ondemand_price,
        'estimated_ondemand_cost': estimated_ondemand_cost,
        'pods_reset_on_exhaustion': False,  # Track if we've reset pods after budget exhausted
        'estimated_runtime_hours': runtime_hours,  # For budget pacing
    }
    print(f"💰 BUDGET ENVELOPE: {workflow_name} initialized:")
    print(f"   On-demand price: ${ondemand_price:.4f}/hr (per machine, 3 pods/machine)")
    print(f"   Estimated runtime: {runtime_hours*60:.1f} min, machines: {estimated_machines}")
    print(f"   Estimated on-demand cost: ${estimated_ondemand_cost:.2f}")
    print(f"   Budget: ${budget:.2f} ({budget_percent*100:.0f}% of on-demand, includes scaling+retry+churn)")
    print(f"   Budget pacing: ${budget/runtime_hours:.2f}/hr target rate")

def reset_cpsp_tracker():
    """Reset CPSP tracker at start of experiment."""
    global cpsp_tracker
    cpsp_tracker = {
        'tasks': {},
        'global_cpsp': 0.0,
    }

def get_budget_pacing_throttle(workflow: str = None, baseline: str = None):
    """
    Calculate throttle factor based on budget pacing and global scaler cooldown.
    
    If we're spending faster than expected (ahead of pace), throttle scaling.
    This ensures budget is spread across the entire experiment, not exhausted early.
    
    KEY INSIGHT: Most budget blowup happens in the first 2-3 minutes when:
    - Success is low, triggering aggressive scaling
    - Cost data hasn't caught up yet
    - Multiple boost paths compound
    
    Solution: 
    1. Wait for retry service to exhaust its retries first
    2. Global cooldown after any scaling event
    3. Budget pacing protection
    
    Returns:
        throttle_factor (0.0 to 1.0): 1.0 = no throttle, 0.0 = full block
        pace_ratio: actual/expected spending ratio (>1 = ahead of pace)
    """
    if not budget_envelope_tracker.get('start_time'):
        return 1.0, 1.0
    
    # Get workflow from tracker if not provided
    if workflow is None:
        workflow = budget_envelope_tracker.get('workflow')
    
    # =========================================================================
    # GLOBAL SCALER COOLDOWN CHECK
    # =========================================================================
    # 1. Wait for retry service to exhaust UIDs before allowing any scaling
    # 2. After scaling, respect global cooldown period
    can_scale, reason = can_scale_globally(workflow, baseline)
    if not can_scale:
        print(f"⏸️ GLOBAL SCALER BLOCK: {reason}")
        return 0.0, 0.0  # Full block
    
    # =========================================================================
    # MINIMUM WARMUP PERIOD: Even after retry exhaustion, wait 60s for stability
    # =========================================================================
    MIN_WARMUP_SECONDS = 60  # Reduced from 180s since we now wait for retry exhaustion
    elapsed_seconds = time.time() - budget_envelope_tracker['start_time']
    if elapsed_seconds < MIN_WARMUP_SECONDS:
        print(f"🕐 WARMUP: Scaling blocked for first {MIN_WARMUP_SECONDS}s ({elapsed_seconds:.0f}s/{MIN_WARMUP_SECONDS}s)")
        return 0.0, 0.0  # Full block during warmup
    
    budget = budget_envelope_tracker.get('budget', 0)
    spent = budget_envelope_tracker.get('spent', 0)
    runtime_hours = budget_envelope_tracker.get('estimated_runtime_hours', 0.167)
    
    if budget <= 0 or runtime_hours <= 0:
        return 1.0, 1.0
    
    # Calculate elapsed time as fraction of expected runtime
    elapsed_seconds = time.time() - budget_envelope_tracker['start_time']
    elapsed_hours = elapsed_seconds / 3600.0
    time_fraction = min(1.0, elapsed_hours / runtime_hours)
    
    # =========================================================================
    # EARLY-STAGE BUDGET PROTECTION
    # =========================================================================
    # In the first 25% of experiment, be VERY conservative with budget
    # Only allow 25% of budget to be spent in first quarter (back-loaded pacing)
    # This prevents the typical pattern of: scale up aggressively → exhaust budget → limp to finish
    EARLY_STAGE_CUTOFF = 0.25  # First 25% of experiment
    EARLY_STAGE_BUDGET_FRACTION = 0.25  # Only allow 25% of budget in early stage
    
    if time_fraction < EARLY_STAGE_CUTOFF:
        # In early stage - use strict budget cap
        early_budget_cap = budget * EARLY_STAGE_BUDGET_FRACTION
        if spent >= early_budget_cap:
            print(f"🛑 EARLY-STAGE BLOCK: ${spent:.2f} spent >= ${early_budget_cap:.2f} early cap (25% of ${budget:.2f})")
            return 0.0, spent / early_budget_cap  # Full block
        
        # Calculate expected spend within the early stage
        early_time_fraction = time_fraction / EARLY_STAGE_CUTOFF  # 0-1 within early stage
        expected_in_early = early_budget_cap * early_time_fraction
        
        if expected_in_early <= 0:
            expected_in_early = early_budget_cap * 0.1  # Allow at least 10% of early cap
        
        pace_ratio = spent / expected_in_early
        
        # In early stage, be VERY strict: block at 1.3x instead of throttle
        if pace_ratio > 1.30:
            print(f"🛑 EARLY-STAGE THROTTLE: {pace_ratio:.1f}x ahead → BLOCKING (early cap ${early_budget_cap:.2f})")
            return 0.0, pace_ratio  # Full block
        elif pace_ratio > 1.10:
            # Heavy throttle if 10-30% ahead
            throttle = max(0.20, 1.0 / pace_ratio)
            return throttle, pace_ratio
        
        return 1.0, pace_ratio
    
    # =========================================================================
    # NORMAL-STAGE BUDGET PACING (after first 25%)
    # =========================================================================
    # After early stage, use linear pacing for remaining 75% of budget
    remaining_budget_fraction = 1.0 - EARLY_STAGE_BUDGET_FRACTION  # 75%
    remaining_time_fraction = time_fraction - EARLY_STAGE_CUTOFF  # 0 to 0.75
    remaining_runtime_fraction = 1.0 - EARLY_STAGE_CUTOFF  # 0.75
    
    # How far through the normal stage are we? (0-1)
    normal_stage_progress = remaining_time_fraction / remaining_runtime_fraction
    
    # Expected spending in normal stage (75% of budget over 75% of time)
    expected_normal_spent = budget * (EARLY_STAGE_BUDGET_FRACTION + remaining_budget_fraction * normal_stage_progress)
    
    # Allow 15% buffer in normal stage (tighter than before)
    PACING_BUFFER = 1.15
    
    pace_ratio = spent / expected_normal_spent if expected_normal_spent > 0 else 0
    
    if pace_ratio <= PACING_BUFFER:
        # On pace or behind - no throttle
        return 1.0, pace_ratio
    
    # CRITICAL: If pace > 1.5x, BLOCK entirely (not just throttle)
    # This prevents the death spiral of: throttle → scale anyway → more cost → repeat
    if pace_ratio > 1.50:
        return 0.0, pace_ratio  # Full block
    
    # Ahead of pace - calculate throttle (more aggressive)
    # At 120% of expected: 60% throttle
    # At 140% of expected: 30% throttle
    overspend_ratio = pace_ratio - PACING_BUFFER
    throttle = max(0.15, 1.0 / (1.0 + overspend_ratio * 3))  # More aggressive (was *2)
    
    return throttle, pace_ratio


def update_budget_spent(cost_per_hour, time_delta_seconds):
    """
    Update cumulative budget spent.
    
    Args:
        cost_per_hour: Current cost rate in $/hour
        time_delta_seconds: Time elapsed since last update in seconds
    
    Returns:
        (remaining_budget, is_exhausted)
    """
    global budget_envelope_tracker
    
    if budget_envelope_tracker['budget_exhausted']:
        return 0.0, True
    
    # Calculate cost for this interval
    cost_for_interval = cost_per_hour * (time_delta_seconds / 3600.0)
    budget_envelope_tracker['spent'] += cost_for_interval
    budget_envelope_tracker['last_cost_rate'] = cost_per_hour
    
    remaining = budget_envelope_tracker['budget'] - budget_envelope_tracker['spent']
    
    if remaining <= 0:
        budget_envelope_tracker['budget_exhausted'] = True
        print(f"🛑 BUDGET EXHAUSTED: ${budget_envelope_tracker['spent']:.2f} spent >= ${budget_envelope_tracker['budget']:.2f} budget")
        print(f"   → Scaling BLOCKED. Retry service will lead recovery.")
        return 0.0, True
    
    # Check budget pacing
    pacing_throttle, pace_ratio = get_budget_pacing_throttle()
    if pacing_throttle < 1.0:
        print(f"⏱️ BUDGET PACING: {pace_ratio:.1f}x ahead of schedule, throttle={pacing_throttle*100:.0f}%")
    
    # Warn when approaching budget limit
    if remaining < budget_envelope_tracker['budget'] * 0.2:  # Less than 20% remaining
        print(f"⚠️ BUDGET WARNING: ${remaining:.2f} remaining (${budget_envelope_tracker['spent']:.2f}/${budget_envelope_tracker['budget']:.2f})")
    
    return remaining, False

def is_budget_exhausted(current_success_rate=None, workflow_name=None):
    """
    Check if budget is exhausted.
    
    IMPORTANT: Now returns (is_exhausted, should_block) tuple.
    - If success is critically low, budget exhaustion becomes a SOFT throttle, not a block.
    
    Args:
        current_success_rate: Current workflow success rate (0.0-1.0) for emergency bypass
        workflow_name: Workflow name to compute workflow-specific target
    
    Returns:
        (is_exhausted, should_block) - is_exhausted is True if budget spent, 
        should_block is False if success emergency allows bypass
    """
    is_exhausted = budget_envelope_tracker.get('budget_exhausted', False)
    
    if not is_exhausted:
        return False, False
    
    # Emergency bypass: allow limited scaling if far below target success.
    if (
        BUDGET_EMERGENCY_BYPASS_ENABLED
        and current_success_rate is not None
        and workflow_name
    ):
        target_success = get_target_success_rate(workflow_name)
        if target_success and current_success_rate < target_success * BUDGET_EMERGENCY_BYPASS_THRESHOLD:
            print(
                f"🚨 BUDGET EMERGENCY BYPASS: success {current_success_rate*100:.1f}% "
                f"< {target_success*BUDGET_EMERGENCY_BYPASS_THRESHOLD*100:.1f}% "
                f"({workflow_name}) → allow limited scaling"
            )
            return True, False
    
    return True, True  # Budget exhausted and should block

def get_budget_status():
    """Get current budget status for logging."""
    return {
        'budget': budget_envelope_tracker.get('budget', 0),
        'spent': budget_envelope_tracker.get('spent', 0),
        'remaining': max(0, budget_envelope_tracker.get('budget', 0) - budget_envelope_tracker.get('spent', 0)),
        'exhausted': budget_envelope_tracker.get('budget_exhausted', False),
    }

def should_scale_based_on_cpsp(task_id, min_data_points=None, current_success_rate=None):
    """
    Determine if task should be scaled based on its historical CPSP.
    
    IMPORTANT: This is now a SOFT gate - it returns a throttle factor instead of blocking.
    When success is critically low, CPSP is bypassed entirely.
    
    Args:
        task_id: Task identifier
        min_data_points: Minimum events needed before throttling (default: CPSP_MIN_DATA_POINTS)
        current_success_rate: Current workflow success rate (0.0-1.0) for emergency bypass
    
    Returns:
        (should_scale, reason) - should_scale is True/False, but caller should treat False as "throttle" not "block"
    """
    if min_data_points is None:
        min_data_points = CPSP_MIN_DATA_POINTS
    
    # NO EMERGENCY BYPASS FOR CPSP - cost controls must apply
    # Emergency mode was causing cost blowup by bypassing all controls early in run
    
    task_data = cpsp_tracker['tasks'].get(task_id)
    
    # No history → allow scaling (give it a chance)
    if task_data is None or len(task_data.get('events', [])) < min_data_points:
        return True, f"insufficient_data ({len(task_data.get('events', [])) if task_data else 0}/{min_data_points})"
    
    cpsp = task_data.get('cpsp', 0)
    
    # CPSP too high → suggest throttling (but don't hard block)
    if cpsp > MAX_CPSP_THRESHOLD:
        # Return False but caller should throttle, not block entirely
        return False, f"cpsp_high (${cpsp:.2f}/ppt > ${MAX_CPSP_THRESHOLD:.2f}) - throttle recommended"
    
    # Infinite CPSP (spent money, no gain) → suggest throttling
    if cpsp == float('inf'):
        return False, "no_success_gain - throttle recommended"
    
    return True, f"cpsp_ok (${cpsp:.2f}/ppt)"

def update_roi_tracker(current_pods, current_success, current_cost_per_hour, workflow_name):
    """
    Update ROI tracker with new data point and calculate ROI.
    
    ROI is calculated as: (success_gain_pct) / (cost_spent_dollars)
    Where cost_spent is the ACTUAL cost spent during the interval, not just the change.
    
    Returns: (roi_value, should_scale, throttle_factor)
    - roi_value: calculated ROI (success_gain_pct / cost_dollars), can be negative
    - should_scale: True if scaling is recommended based on ROI
    - throttle_factor: 0.0-1.0, how much to scale (1.0 = full, 0.5 = half)
    """
    global roi_tracker
    import time
    
    current_time = time.time()
    target_success = get_target_success_rate(workflow_name)
    success_gap = target_success - current_success  # How far below target
    
    # Initialize last snapshot if this is first call
    if roi_tracker['last_snapshot'] is None:
        roi_tracker['last_snapshot'] = {
            'pods': current_pods,
            'success': current_success,
            'cost_per_hour': current_cost_per_hour,
            'timestamp': current_time,
            'cumulative_cost': 0.0  # Track total cost spent
        }
        return 0.0, True, 1.0  # First tick, allow scaling
    
    # Calculate changes since last snapshot
    last = roi_tracker['last_snapshot']
    time_delta = max(current_time - last['timestamp'], 1.0)  # Avoid division by zero
    time_delta_hours = time_delta / 3600.0
    
    pods_change = current_pods - last['pods']
    success_change = current_success - last['success']  # Decimal (e.g., 0.05 = 5%)
    
    # Calculate ACTUAL cost spent during this interval (not just the change in rate)
    # Cost = average_cost_per_hour * time_spent
    avg_cost_per_hour = (current_cost_per_hour + last['cost_per_hour']) / 2.0
    cost_spent = avg_cost_per_hour * time_delta_hours
    
    # Also track incremental cost (cost added by scaling up)
    cost_increase = max(0, current_cost_per_hour - last['cost_per_hour']) * time_delta_hours
    
    # Record in history
    roi_tracker['history'].append({
        'timestamp': current_time,
        'pods_before': last['pods'],
        'pods_after': current_pods,
        'success_before': last['success'],
        'success_after': current_success,
        'cost_spent': cost_spent,  # Actual cost spent during interval
        'cost_increase': cost_increase,  # Cost added by scaling
        'time_delta': time_delta
    })
    
    # Keep only last N entries
    if len(roi_tracker['history']) > roi_tracker['window_size'] * 2:
        roi_tracker['history'] = roi_tracker['history'][-roi_tracker['window_size']:]
    
    # Calculate ROI over window
    window = roi_tracker['history'][-roi_tracker['window_size']:]
    total_success_change = sum(h['success_after'] - h['success_before'] for h in window)
    total_pods_change = sum(h['pods_after'] - h['pods_before'] for h in window)
    
    # Use cost_increase (cost added by scaling) for ROI calculation
    # This measures: "how much success did we gain per dollar spent on ADDITIONAL pods"
    total_cost_increase = sum(h.get('cost_increase', h.get('cost_change', 0.01)) for h in window)
    
    # If pods were added but cost didn't register properly, estimate cost
    # This can happen with short time intervals where pro-rated cost is tiny
    if total_pods_change > 0:
        estimated_window_time = sum(h['time_delta'] for h in window) / 3600.0
        # Minimum 5 minutes of "effective" time for cost calculation
        # This prevents artificial ROI inflation from very short intervals
        effective_window_time = max(estimated_window_time, 5.0 / 60.0)  # At least 5 minutes
        
        # Estimate cost based on pods added (assume $0.10/pod/hour baseline)
        estimated_cost = total_pods_change * COST_BASELINE_PER_POD_HOUR * effective_window_time
        
        # Use the higher of actual or estimated cost
        if total_cost_increase < estimated_cost:
            total_cost_increase = estimated_cost
    
    total_cost_increase = max(total_cost_increase, 0.05)  # Minimum $0.05 to avoid division issues
    
    # ROI = success_gain (%) / cost_increase ($)
    # e.g., ROI=5 means we gain 5 percentage points of success per $1 spent on additional pods
    roi_value = (total_success_change * 100) / total_cost_increase
    
    # Update cumulative metrics
    if pods_change > 0:
        roi_tracker['total_pods_added'] += pods_change
    roi_tracker['total_success_gained'] += max(success_change * 100, 0)  # Only count gains
    roi_tracker['total_cost_spent'] += cost_spent  # Track actual cost spent
    
    # Decision logic based on ROI
    should_scale = True
    throttle_factor = 1.0
    
    # Debug: show ROI calculation breakdown
    if abs(total_success_change) > 0.001 or total_pods_change != 0:
        print(f"   📈 ROI breakdown: success Δ{total_success_change*100:+.2f}%, pods Δ{total_pods_change:+d}, "
              f"cost_incr=${total_cost_increase:.3f} → ROI={roi_value:.2f}")
    
    # Check if ROI is negative (scaling is hurting, not helping)
    # Also check for "stalled" scaling: pods increased but success improvement is too small
    # Use relative threshold: need at least 0.5% success gain per 10 pods added
    min_expected_gain = total_pods_change * 0.005 / 10.0  # 0.05% per pod
    is_stalled = total_pods_change > 5 and total_success_change < min_expected_gain  # Added 5+ pods but insufficient improvement
    
    if roi_value < 0 and pods_change > 0:
        roi_tracker['negative_roi_ticks'] += 1
        print(f"📉 ROI-TRACKER: Negative ROI ({roi_value:.2f}), tick {roi_tracker['negative_roi_ticks']}/{roi_tracker['max_negative_roi_ticks']}")
    elif is_stalled:
        # Stalled: scaling without sufficient improvement
        roi_tracker['negative_roi_ticks'] += 1
        print(f"📉 ROI-TRACKER: Stalled scaling (added {total_pods_change} pods, got {total_success_change*100:.2f}% vs expected {min_expected_gain*100:.2f}%)")
    elif roi_value > 0:
        roi_tracker['negative_roi_ticks'] = max(0, roi_tracker['negative_roi_ticks'] - 1)  # Decay negative count
    
    # Throttle scaling if ROI has been consistently negative or stalled
    # AGGRESSIVE: Let retry service handle recovery when scaling isn't helping
    if roi_tracker['negative_roi_ticks'] >= roi_tracker['max_negative_roi_ticks']:
        roi_tracker['scaling_throttled'] = True
        roi_tracker['throttle_reason'] = f"Negative ROI for {roi_tracker['negative_roi_ticks']} ticks"
        throttle_factor = 0.15  # Only 15% of normal scaling (was 30%) - let retry service lead
        print(f"🛑 ROI-TRACKER: Scaling throttled to {throttle_factor*100:.0f}% - {roi_tracker['throttle_reason']} (retry service is hero)")
    
    # Low ROI but not negative - reduce scaling intensity significantly
    elif roi_value < roi_tracker['min_roi_threshold'] and roi_value >= 0:
        throttle_factor = 0.35  # Only 35% of normal scaling (was 60%) - let retry service help
        print(f"⚠️ ROI-TRACKER: Low ROI ({roi_value:.2f} < {roi_tracker['min_roi_threshold']}), throttling to {throttle_factor*100:.0f}% (retry service is hero)")
    
    # Good ROI - allow full scaling
    elif roi_value >= roi_tracker['min_roi_threshold']:
        roi_tracker['scaling_throttled'] = False
        roi_tracker['throttle_reason'] = None
        throttle_factor = 1.0
        if roi_value > 2.0:  # Very good ROI
            print(f"✅ ROI-TRACKER: Good ROI ({roi_value:.2f}), full scaling allowed")
    
    # Special case: if we're far below target, relax throttling
    if success_gap > 0.30:  # More than 30% below target
        throttle_factor = max(throttle_factor, 0.7)  # At least 70% scaling
        if roi_tracker['scaling_throttled']:
            print(f"🔓 ROI-TRACKER: Relaxing throttle (success {current_success*100:.1f}% << target {target_success*100:.1f}%)")
    
    # Update snapshot for next tick
    cumulative_cost = last.get('cumulative_cost', 0.0) + cost_spent
    roi_tracker['last_snapshot'] = {
        'pods': current_pods,
        'success': current_success,
        'cost_per_hour': current_cost_per_hour,
        'timestamp': current_time,
        'cumulative_cost': cumulative_cost
    }
    
    roi_tracker['throttle_factor'] = throttle_factor
    
    return roi_value, should_scale, throttle_factor

def get_roi_throttle_factor():
    """Get the current ROI-based throttle factor for scaling decisions."""
    return roi_tracker.get('throttle_factor', 1.0)

def get_roi_summary():
    """Get a summary of ROI tracking for logging."""
    return {
        'total_pods_added': roi_tracker['total_pods_added'],
        'total_success_gained_pct': roi_tracker['total_success_gained'],
        'total_cost_spent': roi_tracker['total_cost_spent'],
        'current_throttle': roi_tracker['throttle_factor'],
        'is_throttled': roi_tracker['scaling_throttled'],
        'negative_roi_ticks': roi_tracker['negative_roi_ticks'],
    }

def get_recent_success_gain(window=None):
    """Return recent success gain over the ROI history window."""
    history = roi_tracker.get('history', [])
    if not history:
        return 0.0
    if window is None:
        window = COST_WIN_WINDOW
    window_entries = history[-max(1, int(window)):]
    return sum(entry.get('success_after', 0.0) - entry.get('success_before', 0.0) for entry in window_entries)

def get_cost_win_throttle(success_rate, target_success, normalized_cost, window=None, workflow_name=None):
    """Return a throttle factor for cost-win mode when gains are marginal and costs are high.
    
    TUNED: For complex workflows below target, don't throttle as aggressively - they need
    more scaling capacity to recover from preemptions.
    """
    if success_rate is None or target_success is None or target_success <= 0:
        return 1.0
    if normalized_cost is None or normalized_cost <= COST_WIN_NORM_THRESHOLD:
        return 1.0
    
    gap = max(0.0, target_success - success_rate)
    near_target = gap <= target_success * COST_WIN_GAP_FRAC
    recent_gain = get_recent_success_gain(window=window)
    marginal_gain = recent_gain < COST_WIN_MIN_GAIN
    
    # For complex workflows significantly below target, don't throttle - they need scaling
    if workflow_name and workflow_name in COMPLEX_WORKFLOWS:
        if success_rate < target_success * 0.80:  # More than 20% below target
            return 1.0  # No throttling - let them scale
        elif success_rate < target_success * 0.95:  # 5-20% below target
            # Mild throttling only
            if normalized_cost >= COST_WIN_STRONG_NORM_THRESHOLD:
                return 0.7  # 70% instead of 20%
            return 0.85  # 85% instead of 35%
    
    if near_target or marginal_gain:
        if normalized_cost >= COST_WIN_STRONG_NORM_THRESHOLD:
            return COST_WIN_STRONG_THROTTLE
        return COST_WIN_THROTTLE
    return 1.0

def apply_cost_win_scale_down(all_function_specs, metadata, workflow_name, success_rate,
                              task_failure_rates=None, bottleneck_ratios=None,
                              critical_tasks=None, real_cost_per_pod_per_sec=None,
                              last_scaled=None, scale_cooldown=180):
    """Scale down low-risk, non-critical tasks to reclaim cost when gains are marginal."""
    if not all_function_specs or not metadata:
        return 0
    target_success = get_target_success_rate(workflow_name)
    if success_rate < target_success * 0.85:
        return 0
    normalized_cost = (real_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                       if real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0 else 1.0)
    cost_win_throttle = get_cost_win_throttle(success_rate, target_success, normalized_cost, workflow_name=workflow_name)
    if cost_win_throttle >= 1.0:
        return 0

    if last_scaled is None:
        last_scaled = {}

    candidates = []
    for task_id, func_spec in all_function_specs.items():
        base_minscale = metadata.get(task_id, {}).get("minscale", 1)
        current_minscale = func_spec.get("minscale", base_minscale)
        if current_minscale <= base_minscale:
            continue
        if critical_tasks and task_id in critical_tasks:
            continue
        failure_rate = (task_failure_rates or {}).get(task_id, 0.0)
        if failure_rate > 0.05:
            continue
        bottleneck_ratio = (bottleneck_ratios or {}).get(task_id, 1.0)
        if bottleneck_ratio > 1.2:
            continue
        candidates.append((task_id, current_minscale - base_minscale))

    if not candidates:
        return 0

    candidates.sort(key=lambda x: x[1], reverse=True)
    limited = candidates[:COST_WIN_SCALE_DOWN_MAX_TASKS]
    scale_down_factor = (COST_WIN_STRONG_SCALE_DOWN_FACTOR
                         if normalized_cost >= COST_WIN_STRONG_NORM_THRESHOLD else COST_WIN_SCALE_DOWN_FACTOR)

    scaled = 0
    now = time.time()
    for task_id, _ in limited:
        last_time = last_scaled.get(task_id)
        if last_time and now - last_time < scale_cooldown:
            continue
        current_minscale = all_function_specs[task_id].get("minscale", 1)
        base_minscale = metadata.get(task_id, {}).get("minscale", 1)
        raw_new_scale = max(base_minscale, int(current_minscale * scale_down_factor))
        
        # MACHINE-ALIGNED SCALE-DOWN: Round down to machine boundary
        if MACHINE_ALIGNED_SCALING:
            new_scale = align_to_machine_boundary(raw_new_scale, round_up=False)
            new_scale = max(base_minscale, new_scale)  # Never go below base
        else:
            new_scale = raw_new_scale
        
        if new_scale < current_minscale:
            # Scale-downs don't trigger global cooldown
            scale_function_spec(task_id, new_scale, record_global_cooldown=False)
            last_scaled[task_id] = now
            scaled += 1
            print(f"   🖥️ COST-WIN (machine-aligned): scale down {task_id} {current_minscale} → {new_scale} "
                  f"(success={success_rate*100:.1f}%, norm_cost={normalized_cost:.2f})")

    return scaled


def reset_pods_to_base(all_function_specs, metadata, workflow_name, reason="cost_control"):
    """
    Aggressively reset ALL pods to their base minscale when cost controls are blocking.
    
    This prevents the scenario where pods stay at elevated levels (e.g., 5x base)
    after scaling is blocked, continuing to incur costs without providing benefit.
    
    Args:
        all_function_specs: Current function specs dict
        metadata: Task metadata with base minscale values
        workflow_name: Workflow name for logging
        reason: Why we're resetting (for logging)
    
    Returns:
        Number of tasks reset
    """
    if not all_function_specs or not metadata:
        return 0
    
    reset_count = 0
    total_pods_before = 0
    total_pods_after = 0
    
    for task_id, func_spec in all_function_specs.items():
        if task_id not in metadata:
            continue
        
        current_minscale = func_spec.get('minscale', 1)
        base_minscale = metadata.get(task_id, {}).get("minscale", 1)
        
        total_pods_before += current_minscale
        
        if current_minscale > base_minscale:
            # Scale-downs don't trigger global cooldown (only scale-ups do)
            scale_function_spec(task_id, base_minscale, record_global_cooldown=False)
            reset_count += 1
            print(f"   ⬇️ RESET: {task_id} {current_minscale} → {base_minscale} (base)")
            total_pods_after += base_minscale
        else:
            total_pods_after += current_minscale
    
    if reset_count > 0:
        print(f"🔄 COST-RESET ({reason}): Reset {reset_count} tasks to base "
              f"({total_pods_before} → {total_pods_after} pods, saving ${(total_pods_before - total_pods_after) * COST_BASELINE_PER_POD_HOUR / 60:.3f}/min)")
    
    return reset_count


def clamp_post_target_pods(all_function_specs, metadata, workflow_name, success_rate, target_success_rate,
                           total_runs, real_cost_per_pod_per_sec, reason="post_target_clamp"):
    """
    Clamp pods close to base when success is already on target but cost remains elevated.

    This is intentionally gentler than a full reset so wf-8/wf-9 keep some safety headroom
    while still cutting tail cost.
    """
    if workflow_name not in POST_TARGET_CLAMP_WORKFLOWS:
        return 0
    if total_runs < POST_TARGET_CLAMP_MIN_RUNS:
        return 0
    if success_rate is None or target_success_rate is None or success_rate < target_success_rate:
        return 0
    if not real_cost_per_pod_per_sec or real_cost_per_pod_per_sec <= 0:
        return 0

    normalized_cost = (real_cost_per_pod_per_sec * 3600) / COST_BASELINE_PER_POD_HOUR
    if normalized_cost < POST_TARGET_CLAMP_NORM_COST_THRESHOLD:
        return 0

    clamped = 0
    for task_id, func_spec in all_function_specs.items():
        if task_id not in metadata:
            continue
        current_minscale = func_spec.get('minscale', 1)
        base_minscale = metadata.get(task_id, {}).get("minscale", 1)
        clamp_cap = max(base_minscale, int(math.ceil(base_minscale * POST_TARGET_CLAMP_SCALE_MULTIPLE)))

        if current_minscale > clamp_cap:
            scale_function_spec(task_id, clamp_cap, record_global_cooldown=False)
            clamped += 1
            print(f"   CLAMP: {task_id} {current_minscale} -> {clamp_cap} (base={base_minscale})")

    if clamped > 0:
        print(f"POST-TARGET CLAMP ({reason}): clamped {clamped} tasks for {workflow_name} "
              f"(success={success_rate*100:.1f}%>=target {target_success_rate*100:.1f}%, norm_cost={normalized_cost:.2f})")
    return clamped


# Hard limits to prevent runaway scaling - WORKFLOW-SPECIFIC COST CONTROL
# Simple workflows: minimal scaling (let retry service handle recovery alone)
# Complex workflows: more headroom for bottleneck tasks
MAX_SCALE_MULTIPLE_SIMPLE = 2   # Simple workflows: 2x base minscale (minimal scaling)
MAX_SCALE_MULTIPLE_COMPLEX = 4  # Complex workflows: 4x base minscale (more headroom)
MAX_TOTAL_PODS = 60             # Simple workflows: tight cap (base totals are 4-15 pods)
MAX_TOTAL_PODS_COMPLEX = 150    # Complex workflows: higher cap (wf-5=31, wf-9=46 base pods)
CRITICAL_PATH_POD_BUDGET_BUFFER = 10  # Allow limited overage for critical-path recovery
MIN_COST_BENEFIT_FLOOR = 0.01  # Minimum threshold: require 1% cost-benefit ratio for any scaling
MIN_HARD_CAP_FLOOR = 6  # Minimum hard cap for any task

def get_max_scale_multiple(workflow_name):
    """Get the maximum scale multiple for a workflow."""
    if workflow_name in COMPLEX_WORKFLOWS:
        return MAX_SCALE_MULTIPLE_COMPLEX
    return MAX_SCALE_MULTIPLE_SIMPLE

def get_max_total_pods(workflow_name):
    """Get the maximum total pods budget for a workflow."""
    if workflow_name in COMPLEX_WORKFLOWS:
        return MAX_TOTAL_PODS_COMPLEX
    return MAX_TOTAL_PODS

# Pareto-based cost control: if success is slightly above target but cost is high,
# avoid scaling and let retry service lead recovery.
PARETO_SUCCESS_MARGIN = 0.03         # 3% above target qualifies as "small margin"
PARETO_COST_NORM_THRESHOLD = 1.2     # 20% above baseline cost per pod-hour

# Cost-win guard: prioritize lower cost when gains are marginal
# AGGRESSIVE THROTTLING: Let retry service be the hero for recovery
COST_WIN_NORM_THRESHOLD = 1.05       # Trigger at just 5% above baseline (was 10%)
COST_WIN_STRONG_NORM_THRESHOLD = 1.15  # Strong throttle at 15% above (was 30%)
COST_WIN_GAP_FRAC = 0.15             # Within 15% of target success (was 10%)
COST_WIN_MIN_GAIN = 0.003            # 0.3% absolute gain threshold (was 0.5%)
COST_WIN_THROTTLE = 0.35             # Only 35% scaling when cost high (was 55%)
COST_WIN_STRONG_THROTTLE = 0.20      # Only 20% scaling when cost very high (was 40%)
COST_WIN_WINDOW = 3
COST_WIN_SCALE_DOWN_FACTOR = 0.80    # Scale down to 80% (was 90%)
COST_WIN_STRONG_SCALE_DOWN_FACTOR = 0.70  # Scale down to 70% (was 85%)
COST_WIN_SCALE_DOWN_MAX_TASKS = 5    # Scale down up to 5 tasks (was 3)

# Machine packing: 3 pods share 1 machine (same as Emulator.py)
# When calculating costs, we charge per machine, not per pod
PODS_PER_MACHINE = 3

# ============================================================================
# MACHINE-ALIGNED SCALING (Cost Optimization)
# ============================================================================
# Instead of scaling by arbitrary factors (which wastes partial machine slots),
# scale in machine increments. This ensures we only pay for full machines.
#
# Example: Task with 6 pods (2 machines)
#   - Old way: scale by 1.2x → 7.2 → 8 pods (3 machines, 1 slot wasted)
#   - New way: add 1 machine → 9 pods (3 machines, fully utilized)
#
# Benefits:
#   1. No partial machine waste
#   2. Finer cost control (add 1 machine at a time)
#   3. Retry service handles remaining recovery
MACHINE_ALIGNED_SCALING = True  # Enable machine-aligned scaling

def pods_to_machines(pods: int) -> int:
    """Convert pod count to machine count (ceiling division)."""
    return (pods + PODS_PER_MACHINE - 1) // PODS_PER_MACHINE

def machines_to_pods(machines: int) -> int:
    """Convert machine count to pod count."""
    return machines * PODS_PER_MACHINE

def get_total_machines_from_metadata(metadata: dict) -> int:
    """
    Count total unique machines across all tasks from metadata.
    
    Machines are shared across tasks (cross-task colocation), so we count unique
    machine_ids across the entire workflow.
    
    Example: task1 on [machine_1, machine_2], task2 on [machine_2, machine_3]
             → Total: 3 unique machines (machine_2 is shared)
    
    Returns:
        Total number of unique machines
    """
    all_machines = set()
    for task_id, task_info in metadata.items():
        machine_ids = task_info.get("machine_ids", {})
        all_machines.update(machine_ids.keys())
    return len(all_machines)

def get_machine_occupancy(metadata: dict) -> dict:
    """
    Calculate current pod occupancy for each machine across all tasks.
    
    This accounts for cross-task colocation where pods from different tasks
    share the same machine.
    
    Example:
        task1: 4 pods → machine_1:3, machine_2:1
        task2: 6 pods → machine_2:2, machine_3:3, machine_4:1
        Result: {machine_1: 3, machine_2: 3, machine_3: 3, machine_4: 1}
    
    Returns:
        Dict mapping machine_id to total pod count on that machine
    """
    machine_pods = {}
    for task_id, task_info in metadata.items():
        machine_ids = task_info.get("machine_ids", {})
        for machine_id, pod_count in machine_ids.items():
            machine_pods[machine_id] = machine_pods.get(machine_id, 0) + pod_count
    return machine_pods

def get_available_capacity(metadata: dict) -> int:
    """
    Calculate total available pod slots across all existing machines.
    
    Each machine can hold PODS_PER_MACHINE pods. Returns how many more pods
    can fit before needing new machines.
    
    Returns:
        Number of available pod slots on existing machines
    """
    machine_pods = get_machine_occupancy(metadata)
    total_capacity = len(machine_pods) * PODS_PER_MACHINE
    total_used = sum(machine_pods.values())
    return total_capacity - total_used

def estimate_machines_after_scaling(task_id: str, current_pods: int, new_pods: int, metadata: dict) -> tuple:
    """
    Estimate how many machines a task will use after scaling.
    
    Accounts for cross-task colocation: new pods might fit on existing machines
    that have spare capacity (from any task's perspective).
    
    Example:
        Workflow has 4 machines, total 10 pods, capacity 12 → 2 spare slots
        Scaling task2 from 6→8 pods (+2) → fits in spare slots, no new machines
    
    Args:
        task_id: The task being scaled
        current_pods: Current pod count for this task
        new_pods: Target pod count after scaling
        metadata: Workflow metadata dictionary
    
    Returns:
        Tuple of (current_total_machines, new_total_machines, machines_added)
    """
    # Get current state across ALL tasks (cross-task colocation)
    current_total_machines = get_total_machines_from_metadata(metadata)
    available_capacity = get_available_capacity(metadata)
    
    if new_pods > current_pods:
        additional_pods = new_pods - current_pods
        
        if additional_pods <= available_capacity:
            # Can fit on existing machines (use spare capacity from any machine)
            machines_added = 0
            new_total_machines = current_total_machines
        else:
            # Need new machines for overflow
            overflow_pods = additional_pods - available_capacity
            machines_added = pods_to_machines(overflow_pods)
            new_total_machines = current_total_machines + machines_added
    else:
        # Scaling down - estimate machines freed
        # Note: actual machine reduction depends on which pods are removed
        # Conservative estimate: machines freed if we drop below threshold
        pods_removed = current_pods - new_pods
        # Rough estimate: if removing enough pods to free a machine
        machines_freed = pods_removed // PODS_PER_MACHINE
        new_total_machines = max(1, current_total_machines - machines_freed)
        machines_added = new_total_machines - current_total_machines  # Will be negative
    
    return current_total_machines, new_total_machines, machines_added

def align_to_machine_boundary(pods: int, round_up: bool = True) -> int:
    """
    Align pod count to machine boundary.
    
    Args:
        pods: Target pod count
        round_up: If True, round up to next machine boundary. If False, round down.
    
    Returns:
        Pod count aligned to machine boundary (multiple of PODS_PER_MACHINE)
    """
    if round_up:
        return machines_to_pods(pods_to_machines(pods))
    else:
        return (pods // PODS_PER_MACHINE) * PODS_PER_MACHINE

def calculate_machine_aligned_scale(current_pods: int, scale_factor: float, 
                                     min_pods: int, max_pods: int,
                                     task_id: str = None, metadata: dict = None) -> int:
    """
    Calculate new pod count with machine-aligned scaling.
    
    Instead of scaling by arbitrary factor, this adds/removes whole machines.
    For scale-up: adds minimum 1 machine (if factor > 1)
    For scale-down: removes minimum 1 machine (if factor < 1)
    
    If task_id and metadata are provided, uses actual machine distribution from
    metadata for more accurate machine counting, including cross-task colocation.
    
    Cross-task colocation means pods from different tasks share machines:
        task1: 4 pods → machine_1:3, machine_2:1
        task2: 6 pods → machine_2:2, machine_3:3, machine_4:1
        → 4 total machines, machine_2 shared between task1 and task2
    
    When scaling, we first try to use available capacity on existing machines
    before adding new machines.
    
    Args:
        current_pods: Current number of pods
        scale_factor: Desired scale factor (e.g., 1.2 for 20% increase)
        min_pods: Minimum allowed pods (base minscale)
        max_pods: Maximum allowed pods (hard cap)
        task_id: Optional task ID for metadata lookup
        metadata: Optional workflow metadata for actual machine distribution
    
    Returns:
        New pod count (may not be machine-aligned if using spare capacity)
    """
    # Calculate raw target
    raw_new_pods = int(current_pods * scale_factor)
    additional_pods_requested = raw_new_pods - current_pods if raw_new_pods > current_pods else 0
    
    if task_id and metadata and scale_factor > 1.0:
        # Check available capacity across ALL machines (cross-task colocation)
        available_capacity = get_available_capacity(metadata)
        
        if additional_pods_requested <= available_capacity:
            # Can fit in existing spare capacity - no new machines needed!
            # Just add the requested pods (no need to align to machine boundary)
            new_pods = raw_new_pods
            print(f"   📦 SPARE CAPACITY: {task_id} +{additional_pods_requested} pods fits in {available_capacity} spare slots (no new machines)")
        else:
            # Need new machines for overflow
            # Add pods to fill spare capacity first, then align overflow to machine boundary
            pods_using_spare = available_capacity
            overflow_pods = additional_pods_requested - pods_using_spare
            
            # Cap machines to add for cost control
            machines_for_overflow = pods_to_machines(overflow_pods)
            if scale_factor < 1.5:
                machines_to_add = min(machines_for_overflow, 1)  # At most 1 new machine
            elif scale_factor < 2.0:
                machines_to_add = min(machines_for_overflow, 2)  # At most 2 new machines
            else:
                machines_to_add = machines_for_overflow  # Allow all for critical scaling
            
            # Calculate final pod count
            # = current + spare_used + (new_machines * PODS_PER_MACHINE capacity, but only use what we need)
            pods_from_new_machines = min(overflow_pods, machines_to_add * PODS_PER_MACHINE)
            new_pods = current_pods + pods_using_spare + pods_from_new_machines
            
            if machines_to_add > 0:
                print(f"   🖥️ NEW MACHINES: {task_id} needs {machines_to_add} new machine(s) "
                      f"(used {pods_using_spare} spare slots, overflow {overflow_pods} pods)")
    elif scale_factor > 1.0:
        # Fallback to theoretical calculation (no metadata)
        current_machines = pods_to_machines(current_pods)
        raw_new_machines = pods_to_machines(raw_new_pods)
        machines_to_add = max(1, raw_new_machines - current_machines)
        
        if scale_factor < 1.5:
            machines_to_add = min(machines_to_add, 1)
        elif scale_factor < 2.0:
            machines_to_add = min(machines_to_add, 2)
        
        new_machines = current_machines + machines_to_add
        new_pods = machines_to_pods(new_machines)
    else:
        # Scale DOWN - align to machine boundary
        new_pods = max(min_pods, raw_new_pods)
    
    # Apply bounds
    new_pods = max(min_pods, min(max_pods, new_pods))
    
    return new_pods

# Cost normalization baseline: expected cost per pod-hour based on actual spot prices
# Spot price ~$1/hr per machine / 3 pods = ~$0.33/hr per pod
# Using $0.30 as baseline since spot can sometimes be cheaper
COST_BASELINE_PER_POD_HOUR = 0.30  # $0.30/pod-hour (realistic spot pricing)

# Time compression factor: Emulator uses 1 real second = 60 emulator seconds
EMULATOR_TIME_COMPRESSION = 60

# Workflow classification (moved up for early reference)
COMPLEX_WORKFLOWS = {'wf-5', 'wf-6', 'wf-8', 'wf-9'}  # Target 65-70% success
SIMPLE_WORKFLOWS = {'wf-1', 'wf-2', 'wf-3', 'wf-4', 'wf-7'}  # Target 95-100% success
BUDGET_EMERGENCY_BYPASS_ENABLED = True
BUDGET_EMERGENCY_BYPASS_THRESHOLD = 0.85  # Allow limited scaling if <85% of target
BUDGET_EMERGENCY_THROTTLE = 0.40          # Throttle scaling when bypassing budget gate
RL_HARD_IDLE_NORM_COST_THRESHOLD = 1.10  # If cost is high and success is at target, force idle in RL
SKIP_SCALE_UP_MIN_RUNS = {'wf-5': 20}    # Delay "skip scale-up" until enough real runs
POST_TARGET_CLAMP_WORKFLOWS = {'wf-8', 'wf-9'}  # High-cost complex workflows needing tail-cost clipping
POST_TARGET_CLAMP_NORM_COST_THRESHOLD = 1.10
POST_TARGET_CLAMP_SCALE_MULTIPLE = 1.20  # Keep small buffer above base, avoid full reset thrash
POST_TARGET_CLAMP_MIN_RUNS = 10

# ============================================================================
# RAY-INSPIRED OPTIMIZATIONS (from Ray: A Distributed Framework paper)
# ============================================================================
# 1. Speculative Replication: Proactively scale bottleneck tasks before failures
# 2. Lineage-Aware Scaling: Scale upstream tasks when downstream struggles
# 3. Burst Scaling: Rapid response to preemption signals with quick cooldown
# 4. Checkpoint Priority: Prefer scaling checkpoint-enabled tasks (cheaper recovery)

# RAY-INSPIRED OPTIMIZATIONS - REDUCED to let retry service be the hero
SPECULATIVE_REPLICATION_THRESHOLD = 0.35  # Trigger speculative scaling at 35%+ drop-off (more conservative)
SPECULATIVE_REPLICATION_FACTOR = 1.02     # Only 2% extra capacity - retry service handles rest
LINEAGE_PROPAGATION_FACTOR = 0.05         # Only 5% propagation - minimal aggressive scaling
CHECKPOINT_SCALING_PRIORITY = 1.02        # Only 2% boost for checkpoint tasks

# ============================================================================
# BURST SCALING DISABLED - Rely on retry service for recovery
# ============================================================================
# Burst scaling was causing cost explosions. Removed entirely.
# Retry service handles failure recovery more efficiently.

# ============================================================================
# PROACTIVE KAPLAN-MEIER SCALING
# ============================================================================
# Instead of waiting for survival < 0.5 (high risk), scale proactively:
# - PROACTIVE tier (0.55-0.70): Light scaling (+15-25%) to prevent issues
# - WARNING tier (0.45-0.55): Moderate scaling (+30-50%) 
# - CRITICAL tier (<0.45): Aggressive scaling (existing behavior)
# Also track survival trends to scale BEFORE risk materializes

PROACTIVE_KM_THRESHOLD = 0.55       # Start proactive scaling when survival < 55% (reduced from 65% for cost savings)
PROACTIVE_KM_SCALE_FACTOR = 1.20    # 20% extra capacity for proactive tier
WARNING_KM_THRESHOLD = 0.50         # Moderate scaling when survival < 50%
WARNING_KM_SCALE_FACTOR = 1.40      # 40% extra capacity for warning tier
CRITICAL_KM_THRESHOLD = 0.40        # Aggressive scaling when survival < 40%
CRITICAL_KM_SCALE_FACTOR = 1.70     # 70% extra capacity for critical tier

# Optional KM threshold overrides (for sensitivity runs)
KM_THRESHOLD_OVERRIDE_ACTIVE = False
KM_THRESHOLD_OVERRIDE_LOGGED = False
def apply_km_threshold_overrides():
    global PROACTIVE_KM_THRESHOLD, WARNING_KM_THRESHOLD, CRITICAL_KM_THRESHOLD
    global KM_THRESHOLD_OVERRIDE_ACTIVE
    if (CHECKSCALE_KM_THRESHOLD_OVERRIDE is None and
            CHECKSCALE_KM_WARNING_OVERRIDE is None and
            CHECKSCALE_KM_CRITICAL_OVERRIDE is None):
        return
    base = CHECKSCALE_KM_THRESHOLD_OVERRIDE
    if base is not None:
        PROACTIVE_KM_THRESHOLD = base
        WARNING_KM_THRESHOLD = CHECKSCALE_KM_WARNING_OVERRIDE if CHECKSCALE_KM_WARNING_OVERRIDE is not None else max(0.0, base - 0.05)
        CRITICAL_KM_THRESHOLD = CHECKSCALE_KM_CRITICAL_OVERRIDE if CHECKSCALE_KM_CRITICAL_OVERRIDE is not None else max(0.0, base - 0.15)
    else:
        if CHECKSCALE_KM_WARNING_OVERRIDE is not None:
            WARNING_KM_THRESHOLD = CHECKSCALE_KM_WARNING_OVERRIDE
        if CHECKSCALE_KM_CRITICAL_OVERRIDE is not None:
            CRITICAL_KM_THRESHOLD = CHECKSCALE_KM_CRITICAL_OVERRIDE
    KM_THRESHOLD_OVERRIDE_ACTIVE = True

apply_km_threshold_overrides()

# Trend detection: track survival history per machine
SURVIVAL_TREND_WINDOW = 5           # Track last 5 observations
SURVIVAL_DECLINE_THRESHOLD = 0.10   # 10% decline triggers proactive scaling

proactive_km_state = {
    'machine_survival_history': {},  # machine_id -> [(timestamp, survival_prob), ...]
    'proactive_scaled_tasks': set(),  # Tasks that received proactive scaling this tick
    'last_proactive_scale_time': {},  # task_id -> timestamp
}

def reset_proactive_km_state():
    """Reset proactive KM state at start of experiment."""
    global proactive_km_state
    proactive_km_state = {
        'machine_survival_history': {},
        'proactive_scaled_tasks': set(),
        'last_proactive_scale_time': {},
    }
    global KM_THRESHOLD_OVERRIDE_LOGGED
    if KM_THRESHOLD_OVERRIDE_ACTIVE and not KM_THRESHOLD_OVERRIDE_LOGGED:
        print(f"🧭 KM threshold override active: proactive={PROACTIVE_KM_THRESHOLD:.2f}, "
              f"warning={WARNING_KM_THRESHOLD:.2f}, critical={CRITICAL_KM_THRESHOLD:.2f}")
        KM_THRESHOLD_OVERRIDE_LOGGED = True

def update_survival_history(machine_id, survival_prob):
    """Track survival probability history for trend detection."""
    global proactive_km_state
    current_time = time.time()
    
    if machine_id not in proactive_km_state['machine_survival_history']:
        proactive_km_state['machine_survival_history'][machine_id] = []
    
    history = proactive_km_state['machine_survival_history'][machine_id]
    history.append((current_time, survival_prob))
    
    # Keep only last N observations
    if len(history) > SURVIVAL_TREND_WINDOW:
        proactive_km_state['machine_survival_history'][machine_id] = history[-SURVIVAL_TREND_WINDOW:]

def detect_survival_trend(machine_id):
    """
    Detect if survival probability is declining.
    
    Returns:
        (is_declining, trend_magnitude)
        - is_declining: True if survival is trending down
        - trend_magnitude: Rate of decline (0.0-1.0)
    """
    history = proactive_km_state['machine_survival_history'].get(machine_id, [])
    
    if len(history) < 3:  # Need at least 3 data points
        return False, 0.0
    
    # Calculate trend using simple linear regression
    recent = history[-3:]  # Last 3 observations
    survival_values = [s for _, s in recent]
    
    # Check if declining
    if survival_values[-1] < survival_values[0]:
        decline = survival_values[0] - survival_values[-1]
        return decline > 0.05, decline  # At least 5% decline
    
    return False, 0.0

def get_proactive_km_tier(survival_prob, is_declining, decline_magnitude, success_rate=None, target_success=None):
    """
    Determine which proactive scaling tier to use.
    
    Args:
        survival_prob: Current survival probability
        is_declining: Whether survival is trending down
        decline_magnitude: How fast survival is declining
        success_rate: Current workflow success rate (optional)
        target_success: Target success rate (optional)
    
    Returns:
        (tier_name, scale_factor)
    """
    # If survival is declining rapidly, be more aggressive
    effective_survival = survival_prob
    if is_declining:
        # Treat declining survival as if it's already lower
        effective_survival -= decline_magnitude * 0.5  # Anticipate further decline
    
    # Determine base tier
    if effective_survival < CRITICAL_KM_THRESHOLD:
        tier, base_factor = 'critical', CRITICAL_KM_SCALE_FACTOR
    elif effective_survival < WARNING_KM_THRESHOLD:
        tier, base_factor = 'warning', WARNING_KM_SCALE_FACTOR
    elif effective_survival < PROACTIVE_KM_THRESHOLD:
        tier, base_factor = 'proactive', PROACTIVE_KM_SCALE_FACTOR
    else:
        return 'safe', 1.0
    
    # Cost-aware adjustment: reduce scale factor if success is already at target
    # This prevents over-scaling when we're already doing well
    if success_rate is not None and target_success is not None and success_rate >= target_success:
        # Success at target: reduce proactive scaling aggressiveness
        if tier == 'proactive':
            return 'safe', 1.0  # Don't proactively scale if already at target
        elif tier == 'warning':
            base_factor = 1.0 + (base_factor - 1.0) * 0.5  # 50% of normal
        # Critical tier stays aggressive even at target (safety)
    
    return tier, base_factor

def should_proactive_scale(task_id, cooldown_seconds=45):
    """Check if task should receive proactive scaling (respecting cooldown)."""
    last_time = proactive_km_state['last_proactive_scale_time'].get(task_id, 0)
    return time.time() - last_time >= cooldown_seconds

def mark_proactive_scaled(task_id):
    """Mark task as having received proactive scaling."""
    proactive_km_state['proactive_scaled_tasks'].add(task_id)
    proactive_km_state['last_proactive_scale_time'][task_id] = time.time()

def get_all_function_specs():
    specs = {}
    try:
        result = subprocess.run(["fission", "fn", "list"], check=True, capture_output=True, text=True)
        lines = result.stdout.strip().split('\n')
        if len(lines) < 2:
            return {}
        header = [h.upper() for h in lines[0].split()]
        name_idx = header.index("NAME")
        minscale_idx = header.index("MINSCALE")
        for line in lines[1:]:
            parts = line.split()
            try:
                name = parts[name_idx]
                minscale = int(parts[minscale_idx])
                specs[name] = {'minscale': minscale}
            except (ValueError, IndexError):
                continue
        return specs
    except Exception as e:
        print(f"⚠️ Unable to fetch fission fn list: {e}")
        return {}

def lookup_survival(survival_curve, t):
    sorted_keys = sorted([int(k) for k in survival_curve.keys()])
    for key in reversed(sorted_keys):
        if t >= key:
            return survival_curve[str(key)]
    return 1.0

def get_machine_pods(core_api, machine_tasks, machine_id, max_retries=3, retry_delay=2):
    """
    Get pods for a machine with retry logic for handling Kubernetes API exceptions.
    
    Args:
        core_api: Kubernetes CoreV1Api client
        machine_tasks: Dictionary mapping machine_id to set of task names
        machine_id: The machine ID to get pods for
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Initial delay between retries in seconds (default: 2)
    
    Returns:
        List of pod items, or empty list if all retries fail
    """
    tasks_in_machine = machine_tasks.get(machine_id, set())
    if not tasks_in_machine: 
        return []
    
    label_selector = f"functionName in ({','.join(tasks_in_machine)})"
    
    for attempt in range(max_retries + 1):
        try:
            pods = core_api.list_namespaced_pod(NAMESPACE, label_selector=label_selector)
            return pods.items
        except ApiException as e:
            # Handle specific API exceptions
            if e.status == 504:  # Gateway Timeout
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"⚠️ Kubernetes API timeout (504) for {machine_id}, attempt {attempt+1}/{max_retries+1}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Kubernetes API timeout (504) for {machine_id} after {max_retries+1} attempts. Skipping this machine.")
                    return []
            elif e.status == 503:  # Service Unavailable
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"⚠️ Kubernetes API unavailable (503) for {machine_id}, attempt {attempt+1}/{max_retries+1}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Kubernetes API unavailable (503) for {machine_id} after {max_retries+1} attempts. Skipping this machine.")
                    return []
            elif e.status == 429:  # Too Many Requests
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt) * 2  # Longer backoff for rate limiting
                    print(f"⚠️ Kubernetes API rate limited (429) for {machine_id}, attempt {attempt+1}/{max_retries+1}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Kubernetes API rate limited (429) for {machine_id} after {max_retries+1} attempts. Skipping this machine.")
                    return []
            else:
                # Other API errors (400, 401, 403, 500, etc.)
                print(f"❌ Kubernetes API error ({e.status}) for {machine_id}: {e.reason}. Skipping this machine.")
                return []
        except Exception as e:
            # Handle other unexpected exceptions
            if attempt < max_retries:
                wait_time = retry_delay * (2 ** attempt)
                print(f"⚠️ Unexpected error getting pods for {machine_id}, attempt {attempt+1}/{max_retries+1}: {str(e)}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ Failed to get pods for {machine_id} after {max_retries+1} attempts: {str(e)}. Skipping this machine.")
                return []
    
    return []  # Fallback: return empty list if all retries exhausted

def get_pod_age(pod):
    creation_time = pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - creation_time).total_seconds()

def update_scaling_efficiency(current_success_rate, current_total_pods, all_function_specs, workflow_name=None):
    """
    Track scaling efficiency to detect and prevent runaway scaling.
    Returns True if scaling should continue, False if it should pause.
    """
    global scaling_efficiency_tracker
    
    tracker = scaling_efficiency_tracker
    
    # Calculate current total pods from function specs
    if all_function_specs:
        current_total_pods = sum(spec.get('minscale', 1) for spec in all_function_specs.values())
    
    # Check total pod budget (workflow-specific)
    max_pods = get_max_total_pods(workflow_name) if workflow_name else MAX_TOTAL_PODS
    if current_total_pods > max_pods:
        tracker['scaling_paused'] = True
        tracker['pause_reason'] = f"Total pods ({current_total_pods}) exceeds budget ({max_pods})"
        print(f"🛑 SCALING PAUSED: {tracker['pause_reason']}")
        return False
    
    # Check if success rate improved since last check
    if tracker['last_success_rate'] is not None:
        success_improved = current_success_rate > tracker['last_success_rate'] + 0.02  # 2% improvement threshold
        pods_increased = current_total_pods > tracker['last_total_pods'] * 1.1  # 10% more pods
        
        if pods_increased and not success_improved:
            tracker['ticks_without_improvement'] += 1
            print(f"⚠️ Scaling efficiency warning: pods increased ({tracker['last_total_pods']}→{current_total_pods}) but success rate unchanged ({tracker['last_success_rate']*100:.1f}%→{current_success_rate*100:.1f}%). Ticks without improvement: {tracker['ticks_without_improvement']}/{tracker['max_ticks_without_improvement']}")
        else:
            if success_improved:
                print(f"✅ Scaling efficiency: Success improved {tracker['last_success_rate']*100:.1f}%→{current_success_rate*100:.1f}%")
            tracker['ticks_without_improvement'] = 0
            tracker['scaling_paused'] = False
            tracker['pause_reason'] = None
        
        # Pause aggressive scaling if no improvement after many ticks
        if tracker['ticks_without_improvement'] >= tracker['max_ticks_without_improvement']:
            tracker['scaling_paused'] = True
            tracker['pause_reason'] = f"No improvement after {tracker['ticks_without_improvement']} ticks with increased pods"
            print(f"🛑 AGGRESSIVE SCALING PAUSED: {tracker['pause_reason']}")
            # Don't return False - allow minimal scaling, just not aggressive
    
    # Update tracked values
    tracker['last_success_rate'] = current_success_rate
    tracker['last_total_pods'] = current_total_pods
    
    return not tracker['scaling_paused']

def get_hard_scale_cap(task_id, base_minscale, workflow_name):
    """
    Calculate hard cap for a task's minscale to prevent runaway scaling.
    Returns the maximum allowed minscale for this task.
    
    Uses MIN_HARD_CAP_FLOOR to ensure tasks with low base minscale (1-2) can still
    scale adequately during emergencies - this was the root cause of wf-5/wf-9 failures
    where bottleneck tasks like task9 (base=1) couldn't scale beyond 5 replicas.
    """
    # Hard cap: no task can scale more than MAX_SCALE_MULTIPLE times its base
    # BUT ensure a minimum floor so low-base tasks aren't starved during preemptions
    max_scale_multiple = get_max_scale_multiple(workflow_name)
    base_cap = max(base_minscale * max_scale_multiple, MIN_HARD_CAP_FLOOR)
    
    # Additional caps based on workflow type - generous for complex workflows
    if workflow_name in COMPLEX_WORKFLOWS:
        # Complex workflows (wf-5,6,8,9): allow up to 10x base, max 80 pods per task
        # These need aggressive scaling to handle bottlenecks
        return min(base_cap, 80)
    else:
        # Simple workflows: allow up to 8x base, max 40 pods per task
        return min(base_cap, 40)

def reset_scaling_efficiency_tracker(workflow_name=None, az=None, instance=None):
    """Reset the scaling efficiency tracker (call at start of each workflow experiment)."""
    global scaling_efficiency_tracker
    scaling_efficiency_tracker = {
        'last_success_rate': None,
        'last_total_pods': 0,
        'ticks_without_improvement': 0,
        'max_ticks_without_improvement': 10,  # Relaxed
        'scaling_paused': False,
        'pause_reason': None
    }
    # Burst scaling removed - retry service handles recovery
    # Reset ROI tracker for new experiment
    reset_roi_tracker()
    print(f"📊 ROI tracker reset for new experiment")
    
    # Reset Budget Envelope tracker (loads spot prices from CSV)
    if workflow_name:
        reset_budget_envelope(workflow_name, az=az, instance=instance)
    
    # Reset CPSP tracker
    reset_cpsp_tracker()
    
    # Reset Proactive KM state
    reset_proactive_km_state()
    
    # Reset Global Scaler Cooldown
    reset_global_scaler_cooldown()
    
    # Reset RL Adaptive Scaler state (if available)
    if RL_AVAILABLE:
        reset_state_tracker(workflow=workflow_name, baseline=None)
        reset_bandit(workflow=workflow_name, baseline=None, load_saved=True)
        print(f"🎰 RL Adaptive Scaler initialized for {workflow_name}")

# ============================================================================
# RAY-INSPIRED: Speculative Replication
# ============================================================================
# Track which tasks have already received speculative replicas this tick
# This prevents duplicate speculation when the same task is evaluated multiple times
_speculative_replicas_added_this_tick = set()

def reset_speculative_tracking():
    """Reset speculative tracking at the start of each tick."""
    global _speculative_replicas_added_this_tick
    _speculative_replicas_added_this_tick = set()

def calculate_speculative_replicas(task_id, current_scale, task_drop_off, bottleneck_ratio, 
                                   workflow_name, success_rate, source='critical'):
    """
    Ray-inspired speculative replication: Proactively create extra replicas for tasks
    that are likely to fail or become bottlenecks. This is similar to Ray's approach
    of speculatively executing tasks to handle stragglers.
    
    Args:
        source: 'critical' for critical task scaling, 'km' for Kaplan-Meier scaling
                Only 'critical' is allowed to add speculative replicas (prevents duplication)
    
    Returns the number of speculative replicas to add (0 if not needed).
    """
    global _speculative_replicas_added_this_tick
    
    # Only allow speculation from critical task scaling, not KM loop (prevents 200+ adds)
    if source == 'km':
        return 0
    
    # Don't add speculative replicas for the same task twice in one tick
    if task_id in _speculative_replicas_added_this_tick:
        return 0
    
    speculative_replicas = 0
    
    # Only trigger speculative replication for struggling workflows
    target_success = get_target_success_rate(workflow_name)
    if success_rate >= target_success:
        return 0  # No speculation needed when hitting targets
    
    # For complex/deep DAG workflows, be MUCH more conservative with speculation
    # These workflows need targeted scaling, not blanket speculation
    is_complex = workflow_name in COMPLEX_WORKFLOWS
    MAX_SPECULATIVE_REPLICAS = 2 if is_complex else 3  # Reduced cap for complex workflows
    
    # Trigger 1: High drop-off rate (task is failing frequently)
    if task_drop_off >= SPECULATIVE_REPLICATION_THRESHOLD:
        drop_off_factor = min(task_drop_off / SPECULATIVE_REPLICATION_THRESHOLD, 1.5)  # was 2.0
        speculative_replicas = int(current_scale * (SPECULATIVE_REPLICATION_FACTOR - 1) * drop_off_factor)
        speculative_replicas = min(speculative_replicas, MAX_SPECULATIVE_REPLICAS)
        if speculative_replicas > 0:
            print(f"   🔮 RAY-SPECULATIVE: {task_id} drop-off {task_drop_off*100:.1f}% → +{speculative_replicas} speculative replicas")
    
    # Trigger 2: High bottleneck ratio - ONLY for actual bottlenecks (4x+)
    if bottleneck_ratio >= 4.0 and speculative_replicas == 0:  # was 3.0
        bottleneck_speculation = min(1, MAX_SPECULATIVE_REPLICAS)  # Just +1 for bottlenecks
        speculative_replicas = bottleneck_speculation
        print(f"   🔮 RAY-SPECULATIVE: {task_id} bottleneck {bottleneck_ratio:.1f}x → +{speculative_replicas} speculative replicas")
    
    # Trigger 3: Critical success rate - ONLY for non-complex workflows
    # Complex workflows need dependency-aware scaling, not blanket speculation
    if success_rate < 0.15 and not is_complex:  # Below 15% success
        emergency_speculation = min(int(current_scale * 0.15), MAX_SPECULATIVE_REPLICAS)
        speculative_replicas = max(speculative_replicas, emergency_speculation)
        if emergency_speculation > 0:
            print(f"   🔮 RAY-SPECULATIVE: Emergency mode (success {success_rate*100:.1f}%) → +{speculative_replicas} speculative replicas for {task_id}")
    
    if speculative_replicas > 0:
        _speculative_replicas_added_this_tick.add(task_id)
    
    return speculative_replicas

# ============================================================================
# RAY-INSPIRED: Lineage-Aware Scaling
# ============================================================================
def get_downstream_pressure(task_id, metadata, task_failure_rates, bottleneck_ratios):
    """
    Calculate pressure from downstream tasks in the lineage.
    Returns a multiplier to apply to this task's scaling.
    """
    successors = metadata.get(task_id, {}).get('successors', [])
    if not successors:
        return 1.0  # No downstream pressure for leaf tasks
    
    max_pressure = 1.0
    for successor_id in successors:
        drop_off = task_failure_rates.get(successor_id, 0) if task_failure_rates else 0
        bottleneck = bottleneck_ratios.get(successor_id, 1.0) if bottleneck_ratios else 1.0
        
        # Calculate pressure from this successor
        pressure = 1.0
        if drop_off > 0.10:
            pressure += drop_off * LINEAGE_PROPAGATION_FACTOR
        if bottleneck > 1.5:
            pressure += (bottleneck - 1) * LINEAGE_PROPAGATION_FACTOR * 0.5
        
        max_pressure = max(max_pressure, pressure)
    
    return max_pressure

def get_upstream_backpressure(task_id, metadata, task_failure_rates, bottleneck_ratios, tasks_data=None):
    """
    Calculate backpressure from THIS task's high failure rate that should propagate to predecessors.
    
    In a fan-in/fan-out DAG, when task5 (6 subtasks) feeds into task6 (2 subtasks):
    - task6 is the bottleneck (3:1 ratio)
    - task5 may fail due to backpressure (can't complete before preemption)
    - We need to boost task5's predecessors to ensure the pipeline keeps flowing
    
    Returns a multiplier to apply to predecessor task scaling.
    """
    # Get this task's failure rate
    my_drop_off = task_failure_rates.get(task_id, 0) if task_failure_rates else 0
    
    if my_drop_off < 0.20:  # Only propagate if this task has significant failures
        return 1.0
    
    # Check if this task feeds into a bottleneck (downstream has lower minscale)
    successors = metadata.get(task_id, {}).get('successors', [])
    feeds_into_bottleneck = False
    for successor_id in successors:
        if bottleneck_ratios and bottleneck_ratios.get(successor_id, 1.0) > 1.5:
            feeds_into_bottleneck = True
            break
    
    # Calculate backpressure multiplier
    # Higher drop-off + feeding into bottleneck = more backpressure
    backpressure = 1.0
    if my_drop_off > 0.30:  # >30% failure rate
        backpressure = 1.0 + my_drop_off * 0.5  # Up to 1.5x boost for 100% failure
        if feeds_into_bottleneck:
            backpressure *= 1.2  # Additional 20% boost if feeding into bottleneck
    elif my_drop_off > 0.20:  # 20-30% failure rate
        backpressure = 1.0 + my_drop_off * 0.3  # Up to 1.3x boost
    
    return backpressure

# ============================================================================
# BURST SCALING REMOVED - Stubs for backward compatibility
# ============================================================================
# Burst scaling was causing cost explosions. These are now no-op stubs.

def check_burst_scaling_trigger(survival_prob, success_rate, target_success, task_failure_rates):
    """Burst scaling disabled - always returns False."""
    return False, None

def activate_burst_scaling(reason, workflow_name=None):
    """Burst scaling disabled - always returns False."""
    return False

def get_burst_scale_factor(task_id, workflow_name=None):
    """Burst scaling disabled - always returns 1.0 (no boost)."""
    return 1.0

# ============================================================================
# RAY-INSPIRED: Checkpoint-Aware Scaling Priority
# ============================================================================
def get_checkpoint_scaling_priority(task_id, metadata):
    """
    Ray-inspired state reconstruction priority: Tasks with checkpoints enabled
    can recover faster from failures, so they get priority in scaling decisions.
    This is similar to Ray's lineage-based reconstruction where objects with
    shorter reconstruction paths are prioritized.
    
    Returns a multiplier for scaling priority (>1.0 means higher priority).
    """
    task_info = metadata.get(task_id, {})
    
    # Check if task has checkpoint enabled
    is_checkpoint = task_info.get('is_checkpoint', False)
    checkpoint_level = task_info.get('checkpoint_level', 0)
    
    if is_checkpoint or checkpoint_level > 0:
        # Checkpoint-enabled tasks get priority boost
        # Higher checkpoint levels = faster recovery = higher priority
        priority = CHECKPOINT_SCALING_PRIORITY
        if checkpoint_level >= 2:
            priority *= 1.1  # Extra 10% for high checkpoint levels
        print(f"   💾 RAY-CHECKPOINT: {task_id} has checkpoint (level={checkpoint_level}) → priority ×{priority:.2f}")
        return priority
    
    return 1.0

def mysql_connection():
    """Create MySQL database connection."""
    return mysql.connector.connect(**DB_CONFIG)

def get_workflow_success_rate(workflow_name, workflows_dir, baseline=None):
    """Get current success rate for a workflow from the database.
    A workflow is successful only if ALL tasks completed successfully (response_code = 200).
    Expected tasks are loaded from tasks.json in the workflows directory."""
    try:
        # Load expected tasks from tasks.json
        tasks_path = os.path.join(workflows_dir, workflow_name, "tasks.json")
        if not os.path.exists(tasks_path):
            print(f"⚠️ tasks.json not found for {workflow_name} at {tasks_path}")
            return 0.0, 0, 0
        
        with open(tasks_path) as f:
            tasks_data = json.load(f)
        
        expected_tasks = set(tasks_data.get("tasks", {}).keys())
        if not expected_tasks:
            print(f"⚠️ No tasks found in tasks.json for {workflow_name}")
            return 0.0, 0, 0
        
        expected_task_count = len(expected_tasks)
        
        conn = mysql_connection()
        cursor = conn.cursor()
        
        # Get all distinct UUIDs for this workflow
        sql_query = "SELECT DISTINCT uuid_passed FROM serverless_workflows WHERE workflow_name = %s"
        params = [workflow_name]
        if baseline:
            sql_query += " AND baseline = %s"
            params.append(baseline)
        if EXPERIMENT_TAG:
            sql_query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        
        cursor.execute(sql_query, tuple(params))
        all_uids = {str(row[0]) for row in cursor.fetchall() if row[0]}
        
        if not all_uids:
            conn.close()
            return 0.0, 0, 0
        
        # For each UUID, check if workflow succeeded
        # For branching workflows (like wf-2), success means completing EITHER branch, not all tasks
        successful_uids = set()
        
        # Special handling for branching workflows
        # wf-2: Path 1 = task1→task2→task3→task3a→taskr1, Path 2 = task1→task2→task3→task3b→taskr2
        # Success if common prefix (task1, task2, task3) + EITHER branch endpoint (taskr1 OR taskr2) completed
        branching_workflows = {
            'wf-2': {
                'common_prefix': {'task1', 'task2', 'task3'},
                'branch_endpoints': ['taskr1', 'taskr2']  # Either endpoint completes = success
            }
        }
        
        for uid in all_uids:
            sql_query = """SELECT DISTINCT workflow_stage 
                          FROM serverless_workflows 
                          WHERE uuid_passed = %s AND workflow_name = %s AND response_code = 200"""
            params = [uid, workflow_name]
            if baseline:
                sql_query += " AND baseline = %s"
                params.append(baseline)
            if EXPERIMENT_TAG:
                sql_query += " AND notes = %s"
                params.append(EXPERIMENT_TAG)
            
            cursor.execute(sql_query, tuple(params))
            completed_tasks = {row[0] for row in cursor.fetchall() if row[0]}
            
            # Check if workflow succeeded
            if workflow_name in branching_workflows:
                # Branching workflow: check if common prefix + at least one branch endpoint completed
                branch_config = branching_workflows[workflow_name]
                common_prefix = branch_config['common_prefix']
                branch_endpoints = branch_config['branch_endpoints']
                
                # Check if common prefix completed
                if common_prefix.issubset(completed_tasks):
                    # Check if at least one branch endpoint completed
                    if any(endpoint in completed_tasks for endpoint in branch_endpoints):
                        successful_uids.add(uid)
            else:
                # Normal workflow: all expected tasks must complete
                if expected_tasks.issubset(completed_tasks):
                    successful_uids.add(uid)
        
        conn.close()
        
        total = len(all_uids)
        success = len(successful_uids)
        rate = success / total if total > 0 else 0.0
        
        return rate, total, success
        
    except Exception as e:
        print(f"⚠️ Error getting success rate for {workflow_name}: {e}")
        return 0.0, 0, 0

def get_bottleneck_ratios(metadata, workflows_dir=None, workflow_name=None):
    """Calculate bottleneck ratios for all tasks in a workflow.
    
    A throughput bottleneck occurs when a task receives data from predecessors with higher minscale,
    creating a processing bottleneck (e.g., task6 with minscale=2 receiving from task5 with minscale=6).
    
    Args:
        metadata: Task metadata dict with minscale
        workflows_dir: Directory containing workflow definitions (optional)
        workflow_name: Workflow name to load tasks.json (optional)
    
    Returns:
        Dict mapping task_id -> bottleneck_ratio (1.0 = no bottleneck, >1.0 = bottleneck)
    """
    bottleneck_ratios = {}
    if workflows_dir and workflow_name:
        try:
            tasks_path = os.path.join(workflows_dir, workflow_name, "tasks.json")
            if os.path.exists(tasks_path):
                with open(tasks_path) as f:
                    tasks_data = json.load(f)
                
                # Build predecessor map (reverse of successors)
                predecessors = {}
                for task_id, task_info in tasks_data.get("tasks", {}).items():
                    for successor in task_info.get("successors", []):
                        predecessors.setdefault(successor, []).append(task_id)
                
                # Calculate bottleneck severity for each task
                for task_id, task_info in tasks_data.get("tasks", {}).items():
                    if task_id not in metadata:
                        continue
                    
                    task_minscale = metadata[task_id].get("minscale", 1)
                    if task_minscale == 0:
                        continue
                    
                    # Check if any predecessor has higher minscale
                    max_bottleneck_ratio = 1.0
                    for pred_id in predecessors.get(task_id, []):
                        if pred_id in metadata:
                            pred_minscale = metadata[pred_id].get("minscale", 1)
                            if pred_minscale > task_minscale:
                                # Bottleneck: predecessor produces more than we can process
                                ratio = pred_minscale / task_minscale
                                max_bottleneck_ratio = max(max_bottleneck_ratio, ratio)
                    
                    if max_bottleneck_ratio > 1.0:
                        bottleneck_ratios[task_id] = max_bottleneck_ratio
        except Exception as e:
            print(f"⚠️ Could not load tasks.json for bottleneck analysis: {e}")
    
    return bottleneck_ratios

def get_bottleneck_predecessors(metadata, workflows_dir=None, workflow_name=None):
    """
    Identify tasks that FEED INTO bottlenecks (fan-in predecessors).
    
    In a fan-in/fan-out DAG like wf-5:
      task5(6) → task6(2) is a 3:1 bottleneck
      task5 is the "bottleneck predecessor" - it's producing 6 parallel outputs
      that need to converge into task6's 2 pods.
    
    These predecessor tasks often have high failure rates due to backpressure
    and should get priority scaling.
    
    Returns:
        Dict mapping task_id -> bottleneck_ratio (for tasks that feed into bottlenecks)
    """
    bottleneck_predecessors = {}
    if workflows_dir and workflow_name:
        try:
            tasks_path = os.path.join(workflows_dir, workflow_name, "tasks.json")
            if os.path.exists(tasks_path):
                with open(tasks_path) as f:
                    tasks_data = json.load(f)
                
                # For each task, check if it feeds into a task with lower minscale
                for task_id, task_info in tasks_data.get("tasks", {}).items():
                    if task_id not in metadata:
                        continue
                    
                    task_minscale = task_info.get("minscale", 1)
                    if task_minscale == 0:
                        continue
                    
                    # Check successors for lower minscale (fan-in bottleneck)
                    for succ_id in task_info.get("successors", []):
                        succ_info = tasks_data.get("tasks", {}).get(succ_id, {})
                        succ_minscale = succ_info.get("minscale", 1)
                        
                        if task_minscale > succ_minscale * 1.3:  # >1.3:1 ratio
                            # This task feeds into a bottleneck
                            ratio = task_minscale / max(succ_minscale, 1)
                            bottleneck_predecessors[task_id] = max(
                                bottleneck_predecessors.get(task_id, 1.0), 
                                ratio
                            )
        except Exception as e:
            print(f"⚠️ Could not load tasks.json for bottleneck predecessor analysis: {e}")
    
    return bottleneck_predecessors

def build_predecessor_map(tasks_data):
    """Build a mapping of task -> set of predecessor tasks from workflow definition."""
    predecessors = {task_id: set() for task_id in tasks_data.keys()}
    for task_id, task_info in tasks_data.items():
        for successor in task_info.get("successors", []):
            predecessors.setdefault(successor, set()).add(task_id)
    return predecessors

def compute_task_depths(tasks_data):
    """Compute DAG depth (longest distance from any root) for each task."""
    predecessors = build_predecessor_map(tasks_data)
    depth_cache = {}

    def _depth(task_id):
        if task_id in depth_cache:
            return depth_cache[task_id]
        preds = predecessors.get(task_id, set())
        if not preds:
            depth_cache[task_id] = 0
            return 0
        value = 1 + max((_depth(p) for p in preds), default=0)
        depth_cache[task_id] = value
        return value

    for task_id in tasks_data.keys():
        _depth(task_id)

    max_depth = max(depth_cache.values(), default=0)
    return depth_cache, max_depth, predecessors

# ============================================================================
# CRITICAL PATH ANALYSIS (from TCC-CloudWorkflow2014 paper)
# ============================================================================
# Based on: "Deadline Based Resource Provisioning and Scheduling Algorithm 
# for Scientific Workflows on Clouds" by Rodriguez & Buyya
# Key insight: Tasks on the critical path have zero slack and need aggressive scaling

# Performance variation buffer (from Schad et al. - 24% CPU variability on cloud)
PERFORMANCE_VARIATION_BUFFER = 1.24  # 24% buffer for execution time estimates

def compute_critical_path_metrics(tasks_data, deadline_seconds=None):
    """
    Compute critical path metrics for workflow tasks using forward/backward pass.
    
    From TCC-CloudWorkflow2014 paper:
    - EST (Earliest Start Time): max(EFT of all predecessors)
    - EFT (Earliest Finish Time): EST + execution_time * PERFORMANCE_VARIATION_BUFFER
    - LFT (Latest Finish Time): min(LST of all successors) or deadline for exit nodes
    - LST (Latest Start Time): LFT - execution_time * PERFORMANCE_VARIATION_BUFFER
    - Slack: LFT - EFT (tasks with slack=0 are on critical path)
    
    Returns:
        dict with: 'critical_path_tasks', 'task_slack', 'task_criticality', 'makespan'
    """
    if not tasks_data:
        return {'critical_path_tasks': set(), 'task_slack': {}, 'task_criticality': {}, 'makespan': 0}
    
    predecessors = build_predecessor_map(tasks_data)
    
    # Identify entry and exit nodes
    entry_nodes = [tid for tid, preds in predecessors.items() if not preds]
    exit_nodes = [tid for tid, info in tasks_data.items() if not info.get("successors", [])]
    
    if not entry_nodes:
        entry_nodes = list(tasks_data.keys())[:1]
    if not exit_nodes:
        exit_nodes = list(tasks_data.keys())[-1:]
    
    # Forward pass: compute EST and EFT
    est = {}  # Earliest Start Time
    eft = {}  # Earliest Finish Time
    
    def compute_eft(task_id):
        if task_id in eft:
            return eft[task_id]
        
        exec_time = tasks_data[task_id].get("exec_time", 1) * PERFORMANCE_VARIATION_BUFFER
        preds = predecessors.get(task_id, set())
        
        if not preds:
            est[task_id] = 0
        else:
            est[task_id] = max(compute_eft(pred) for pred in preds)
        
        eft[task_id] = est[task_id] + exec_time
        return eft[task_id]
    
    # Compute EFT for all tasks
    for task_id in tasks_data:
        compute_eft(task_id)
    
    # Makespan = max EFT of exit nodes
    makespan = max(eft.get(exit_node, 0) for exit_node in exit_nodes)
    
    # Use deadline if provided, otherwise use makespan * 1.1 (10% slack)
    effective_deadline = deadline_seconds if deadline_seconds else makespan * 1.1
    
    # Backward pass: compute LFT and LST
    lft = {}  # Latest Finish Time
    lst = {}  # Latest Start Time
    
    # Initialize exit nodes with deadline
    for exit_node in exit_nodes:
        lft[exit_node] = effective_deadline
    
    def compute_lst(task_id):
        if task_id in lst:
            return lst[task_id]
        
        exec_time = tasks_data[task_id].get("exec_time", 1) * PERFORMANCE_VARIATION_BUFFER
        successors = tasks_data[task_id].get("successors", [])
        
        if not successors or task_id in exit_nodes:
            if task_id not in lft:
                lft[task_id] = effective_deadline
        else:
            # LFT = min(LST of all successors)
            lft[task_id] = min(compute_lst(succ) for succ in successors)
        
        lst[task_id] = lft[task_id] - exec_time
        return lst[task_id]
    
    # Compute LST for all tasks (in reverse topological order)
    for task_id in tasks_data:
        compute_lst(task_id)
    
    # Compute slack and identify critical path
    task_slack = {}
    critical_path_tasks = set()
    task_criticality = {}  # 0.0 = on critical path, 1.0 = maximum slack
    
    max_slack = 0
    for task_id in tasks_data:
        slack = lft.get(task_id, 0) - eft.get(task_id, 0)
        task_slack[task_id] = max(0, slack)
        max_slack = max(max_slack, slack)
        
        # Tasks with near-zero slack are on or near critical path
        if slack <= makespan * 0.05:  # Within 5% of critical path
            critical_path_tasks.add(task_id)
    
    # Normalize criticality: 0.0 = critical path, 1.0 = max slack
    for task_id in tasks_data:
        if max_slack > 0:
            task_criticality[task_id] = task_slack[task_id] / max_slack
        else:
            task_criticality[task_id] = 0.0  # All tasks are critical
    
    print(f"📊 Critical Path Analysis: {len(critical_path_tasks)}/{len(tasks_data)} tasks on critical path")
    print(f"   Makespan estimate: {makespan:.1f}s, deadline: {effective_deadline:.1f}s")
    for task_id in sorted(critical_path_tasks):
        exec_time = tasks_data[task_id].get("exec_time", 1)
        print(f"   🔴 {task_id}: exec={exec_time}s, slack={task_slack.get(task_id, 0):.1f}s (CRITICAL)")
    
    return {
        'critical_path_tasks': critical_path_tasks,
        'task_slack': task_slack,
        'task_criticality': task_criticality,
        'makespan': makespan,
        'est': est,
        'eft': eft,
        'lst': lst,
        'lft': lft
    }

def get_critical_path_scaling_boost(task_id, critical_path_metrics, workflow_name):
    """
    Get scaling boost multiplier for tasks based on critical path analysis.
    
    From TCC-CloudWorkflow2014 paper (Section 5.2):
    - Tasks on critical path need aggressive scaling (no slack = must finish on time)
    - Tasks with slack can be scaled more conservatively
    
    Args:
        task_id: Task identifier
        critical_path_metrics: Output from compute_critical_path_metrics()
        workflow_name: Workflow name for workflow-specific tuning
    
    Returns:
        Scaling boost multiplier (1.0 = no boost, >1.0 = boost scaling)
    """
    if not critical_path_metrics:
        return 1.0
    
    criticality = critical_path_metrics.get('task_criticality', {}).get(task_id, 0.5)
    is_on_critical_path = task_id in critical_path_metrics.get('critical_path_tasks', set())
    
    # Base boost for critical path tasks
    if is_on_critical_path:
        # Critical path tasks get significant boost
        if workflow_name in COMPLEX_WORKFLOWS:
            return 2.0  # 2x boost for complex workflow critical path tasks
        else:
            return 1.5  # 1.5x boost for simple workflow critical path tasks
    else:
        # Non-critical tasks: boost inversely proportional to slack
        # criticality=0 -> critical path, criticality=1 -> max slack
        boost = 1.0 + (1.0 - criticality) * 0.5  # Range: 1.0-1.5
        return boost

def get_position_threshold_multiplier(task_id, task_depths, max_depth):
    """Return multiplier (<1.0 for early tasks) to make thresholds more lenient."""
    if not task_depths or task_id not in task_depths:
        return 1.0
    if max_depth <= 0:
        return 0.5
    depth = task_depths[task_id]
    return 0.5 + 0.5 * (depth / max_depth)

def get_failure_threshold_multiplier(failure_rate):
    """Return multiplier (<1.0) based on observed failure/drop-off rate for task."""
    if failure_rate is None or failure_rate <= 0:
        return 1.0
    rate = max(0.0, min(1.0, failure_rate))
    return max(0.3, 1.0 - 0.7 * rate)

def get_task_completion_counts(workflow_name, task_ids, baseline=None):
    """Fetch completion counts per task (any response) for a workflow."""
    counts = {task_id: 0 for task_id in task_ids}
    if not task_ids:
        return counts
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        query = "SELECT workflow_stage, COUNT(*) FROM serverless_workflows WHERE workflow_name = %s"
        params = [workflow_name]
        if baseline:
            query += " AND baseline = %s"
            params.append(baseline)
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        query += " GROUP BY workflow_stage"
        cursor.execute(query, tuple(params))
        for stage, total in cursor.fetchall():
            if stage in counts:
                counts[stage] = int(total)
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error fetching task completion counts for {workflow_name}: {e}")
    return counts

def task_from_podname(pod_name):
    """Extract task name from pod name (e.g., 'task1-abc123' -> 'task1')."""
    if not pod_name:
        return ""
    parts = str(pod_name).split('-')
    return parts[0] if parts else str(pod_name)

def get_recent_cost_per_pod(workflow_name, baseline=None, lookback_seconds=300):
    """Get accurate cost from recent cost_logs using compute_cost_per_wf_baseline logic.
    
    Uses MACHINE PACKING cost calculation:
    - 3 pods share 1 machine
    - Machine ID = (pod_index // 3) + 1 within each task
    - Machine lifetime = max_age of oldest pod on that machine
    - Cost = price_per_hour * (machine_lifetime_sec / 3600)
    
    Args:
        workflow_name: Workflow name to filter costs
        baseline: Optional baseline filter
        lookback_seconds: How far back to look for cost data (default 5 minutes)
    
    Returns:
        Tuple of (avg_cost_per_pod_per_second, avg_price_per_hour, sample_count,
                  total_cost_per_hour, avg_machines, avg_concurrent_pods)
        Returns zeros if no data available
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get recent cost_logs entries
        query = """
            SELECT timestamp, price_per_hour, pod_age_seconds, pod_name, check_interval_seconds
            FROM cost_logs
            WHERE workflow_name = %s
            AND timestamp >= DATE_SUB(NOW(), INTERVAL %s SECOND)
        """
        params = [workflow_name, lookback_seconds]
        if baseline:
            query += " AND baseline = %s"
            params.append(baseline)
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        query += " ORDER BY pod_name, timestamp"
        
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            return 0.0, 0.0, 0, 0.0, 0.0, 0.0
        
        # Convert to DataFrame for efficient processing
        df = pd.DataFrame(rows)
        sample_count = len(df)
        
        # Extract task from pod_name
        df['task'] = df['pod_name'].apply(task_from_podname)
        
        # Convert numeric columns
        df['pod_age_seconds'] = pd.to_numeric(df['pod_age_seconds'], errors='coerce').fillna(0)
        df['price_per_hour'] = pd.to_numeric(df['price_per_hour'], errors='coerce').fillna(0)
        
        # =====================================================================
        # MACHINE ID ASSIGNMENT (from compute_cost_per_wf_baseline)
        # 3 pods share 1 machine: machine_id = (pod_index // 3) + 1
        # =====================================================================
        # Build pod_index mapping for each task, then assign machine_id
        machine_ids = []
        for task in df['task'].unique():
            task_mask = df['task'] == task
            task_pods = sorted(df.loc[task_mask, 'pod_name'].unique())
            pod_index = {p: i for i, p in enumerate(task_pods)}
            for idx in df.loc[task_mask].index:
                pod_name = df.loc[idx, 'pod_name']
                pod_idx = pod_index.get(pod_name, 0)
                machine_ids.append((idx, (pod_idx // PODS_PER_MACHINE) + 1))
        
        # Set machine_id for all rows
        for idx, mid in machine_ids:
            df.loc[idx, 'machine_id'] = mid
        df['machine_id'] = df['machine_id'].astype(int)
        
        # =====================================================================
        # MACHINE-LEVEL COST CALCULATION
        # Machine lifetime = max_age of oldest pod on that machine
        # Cost = price_per_hour * (machine_lifetime_sec / 3600)
        # =====================================================================
        machine_stats = df.groupby(['task', 'machine_id'], observed=False).agg(
            max_age=('pod_age_seconds', 'max'),
            price_per_hour=('price_per_hour', 'first')
        ).reset_index()
        
        # Machine lifetime = oldest pod's age (max_age)
        machine_stats['machine_lifetime_sec'] = machine_stats['max_age'].fillna(0)
        machine_stats['spot_hours'] = machine_stats['machine_lifetime_sec'] / 3600.0
        machine_stats['cost'] = machine_stats['price_per_hour'] * machine_stats['spot_hours']
        
        total_cost = float(machine_stats['cost'].sum())
        num_machines = len(machine_stats)
        avg_price = float(df['price_per_hour'].mean()) if not df.empty else 0.0
        
        # Calculate total runtime for rate calculation
        total_machine_seconds = float(machine_stats['machine_lifetime_sec'].sum())
        
        if total_machine_seconds > 0 and num_machines > 0:
            # Cost per machine per second
            cost_per_machine_per_sec = total_cost / total_machine_seconds
            # Convert to per-pod for compatibility
            avg_cost_per_pod_per_sec = cost_per_machine_per_sec / PODS_PER_MACHINE
            effective_price_per_hour = avg_price / PODS_PER_MACHINE
            
            # Total cost rate ($/hour)
            avg_machine_lifetime_sec = total_machine_seconds / num_machines
            total_cost_per_hour = (total_cost / avg_machine_lifetime_sec) * 3600.0 if avg_machine_lifetime_sec > 0 else 0.0
            
            avg_pods = len(df['pod_name'].unique())
            
            return (avg_cost_per_pod_per_sec, effective_price_per_hour, sample_count,
                    total_cost_per_hour, float(num_machines), float(avg_pods))
        
        # Fallback: estimate from price_per_hour
        if not df.empty:
            avg_price_per_hour = df['price_per_hour'].mean()
            effective_price_per_hour = avg_price_per_hour / PODS_PER_MACHINE
            avg_cost_per_pod_per_sec = effective_price_per_hour / 3600.0
            return avg_cost_per_pod_per_sec, effective_price_per_hour, sample_count, 0.0, 0.0, 0.0
        
        return 0.0, 0.0, 0, 0.0, 0.0, 0.0
        
    except Exception as e:
        print(f"⚠️ Error fetching cost data for {workflow_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0.0, 0.0, 0, 0.0, 0.0, 0.0

def fetch_task_failure_metrics(workflow_name, tasks_data, predecessors, baseline=None):
    """Return per-task failure/drop-off rates derived from completion counts."""
    task_ids = list(tasks_data.keys())
    counts = get_task_completion_counts(workflow_name, task_ids, baseline=baseline)
    failure_rates = {}
    if not task_ids:
        return failure_rates, counts

    root_candidates = [counts.get(task_id, 0) for task_id in task_ids if not predecessors.get(task_id)]
    reference_default = max(root_candidates) if root_candidates else max(counts.values(), default=0)

    for task_id in task_ids:
        completed = counts.get(task_id, 0)
        preds = predecessors.get(task_id, set())
        if preds:
            pred_reference = max((counts.get(pred, 0) for pred in preds), default=reference_default)
            reference = pred_reference if pred_reference > 0 else reference_default
        else:
            reference = reference_default
        if reference <= 0:
            failure_rate = 0.0
        else:
            failure_rate = max(0.0, min(1.0, (reference - completed) / reference))
        failure_rates[task_id] = failure_rate
    return failure_rates, counts

def compute_adaptive_pressure_multiplier(success_rate, target_success, normalized_cost=1.0, task_failure_rate=None):
    """Blend success pressure, cost pressure, and observed failures into a single multiplier.
    
    - Below target: prioritize recovery; cost can soften but not block scaling.
    - Above target: optimize cost; high failure rate keeps multiplier closer to neutral.
    """
    if success_rate is None or target_success is None or target_success <= 0:
        return 1.0

    failure_component = max(0.0, min(1.0, task_failure_rate)) if task_failure_rate is not None else 0.0
    normalized_cost = max(0.0, normalized_cost or 0.0)
    gap = target_success - success_rate

    if gap > 0:
        # Recovery pressure grows with gap; failure makes it stronger
        recovery_pressure = min(1.5, (gap / target_success) * (1.2 + failure_component * 1.5))
        # Cost can soften, but only up to 25%
        cost_softener = 1.0 - min(0.25, max(0.0, normalized_cost - 1.0) * 0.08)
        return max(1.0, (1.0 + recovery_pressure) * cost_softener)
    else:
        # Above target: reduce gently, but keep headroom if failures are present
        over = -gap
        cost_pressure = max(0.0, normalized_cost - 1.0)
        reduction = min(0.5, (over / target_success) * (0.4 + cost_pressure * 0.15 - failure_component * 0.2))
        return max(0.65, 1.0 - reduction)


def get_effective_scaling_factor(task_id, metadata, bottleneck_ratios=None, 
                                  pod_lifetime_risk=None, cost_per_pod_per_sec=None,
                                  workflow_name=None, workflow_success_rate=None,
                                  task_failure_rate=None, critical_path_metrics=None):
    """Calculate effective scaling factor for Kaplan-Meier scaling, considering:
    - HEFT scaling_factor
    - Bottlenecks
    - Pod lifetime risk (short lifetimes = higher risk)
    - Cost-aware adjustments
    - Critical path analysis (from TCC-CloudWorkflow2014 paper)
    
    Args:
        task_id: Task identifier
        metadata: Task metadata dict with scaling_factor and minscale
        bottleneck_ratios: Optional dict mapping task_id -> bottleneck_ratio
        pod_lifetime_risk: Optional risk factor based on pod lifetime (0.0-1.0, higher = shorter lifetime)
        cost_per_pod_per_sec: Optional cost per pod per second for cost-aware adjustments
        workflow_name: Optional workflow name for workflow-specific adjustments
        workflow_success_rate: Latest observed success rate for the workflow (optional)
        task_failure_rate: Task-specific failure/drop-off rate (optional)
        critical_path_metrics: Critical path analysis from compute_critical_path_metrics() (optional)
    
    Returns:
        Effective scaling factor (>= 1.0)
    """
    base_scaling_factor = metadata.get("scaling_factor", 1.0)
    bottleneck_ratio = (bottleneck_ratios or {}).get(task_id, 1.0)
    
    # Start with HEFT scaling factor
    effective_factor = base_scaling_factor
    
    # Apply critical path boost (from TCC-CloudWorkflow2014 paper)
    # Tasks on the critical path have zero slack and must be scaled aggressively
    critical_path_boost = get_critical_path_scaling_boost(task_id, critical_path_metrics, workflow_name)
    if critical_path_boost > 1.0:
        effective_factor *= critical_path_boost
        is_critical = task_id in (critical_path_metrics or {}).get('critical_path_tasks', set())
        if is_critical:
            print(f"   📊 Critical path boost for {task_id}: {critical_path_boost:.2f}x (ON CRITICAL PATH)")
        else:
            slack = (critical_path_metrics or {}).get('task_slack', {}).get(task_id, 0)
            print(f"   📊 Near-critical path boost for {task_id}: {critical_path_boost:.2f}x (slack={slack:.1f}s)")
    
    # Apply bottleneck boost - ONLY when success rate is below target (cost optimization)
    # Complex workflows have deeper DAGs where bottlenecks cause severe cascade failures
    # But we don't need aggressive scaling when already meeting targets
    target_for_bottleneck = get_target_success_rate(workflow_name)
    if bottleneck_ratio > 1.0 and (workflow_success_rate is None or workflow_success_rate < target_for_bottleneck):
        if workflow_name in COMPLEX_WORKFLOWS:
            # Complex workflows: Aggressive bottleneck boost ONLY when below target
            # Scale boost by how far below target we are (0% at target, 100% at 0% success)
            success_gap_factor = 1.0 if workflow_success_rate is None else max(0.3, (target_for_bottleneck - workflow_success_rate) / target_for_bottleneck)
            # e.g., at 50% of target: success_gap_factor=0.5, bottleneck_ratio=3.0 -> boost=1.5
            bottleneck_boost = bottleneck_ratio * success_gap_factor
            effective_factor = max(effective_factor, 1.0 + bottleneck_boost)
            print(f"   🔥 Complex WF bottleneck boost for {task_id}: ratio={bottleneck_ratio:.2f}, gap={success_gap_factor:.2f}, boost={bottleneck_boost:.2f}, factor={effective_factor:.2f}")
        else:
            # Simple workflows: smaller boost, also scaled by success gap
            success_gap_factor = 1.0 if workflow_success_rate is None else max(0.2, (target_for_bottleneck - workflow_success_rate) / target_for_bottleneck)
            bottleneck_boost = bottleneck_ratio * 0.3 * success_gap_factor  # 30% of bottleneck ratio max
            effective_factor = max(effective_factor, 1.0 + bottleneck_boost)
    
    # Apply short-lifetime risk boost (more aggressive scaling for pods with short lifetimes)
    # Guard: only apply lifetime risk boosts to complex workflows to avoid thrash on simple ones
    if workflow_name in COMPLEX_WORKFLOWS and pod_lifetime_risk is not None and pod_lifetime_risk > 0.5:  # High risk (short lifetime)
        # Boost scaling by up to 1.5x for very short lifetimes
        lifetime_boost = 1.0 + (pod_lifetime_risk - 0.5) * 1.0  # 0.0-1.0 boost
        effective_factor *= (1.0 + lifetime_boost * 0.3)  # Up to 30% additional boost
        # Additional boost only for complex workflows (already gated)
        effective_factor *= (1.0 + lifetime_boost * 0.2)  # Additional 20% boost
    elif (workflow_name in SIMPLE_WORKFLOWS and pod_lifetime_risk is not None and
          workflow_success_rate is not None):
        # Enhanced lifetime-risk assistance for simple workflows when they slip below high target
        target = get_target_success_rate(workflow_name)
        if workflow_success_rate < target:
            # Lower threshold when success rate is significantly below target
            risk_threshold = 0.4 if workflow_success_rate < target * 0.90 else 0.5
            if pod_lifetime_risk > risk_threshold:
                # More aggressive boost when success rate is well below target
                if workflow_success_rate < target * 0.80:  # Below 80% of target (78%)
                    excess_risk = min(0.5, pod_lifetime_risk - risk_threshold)
                    effective_factor *= (1.0 + excess_risk * 0.6)  # up to ~30% boost
                elif workflow_success_rate < target * 0.90:  # Below 90% of target (87.75%)
                    excess_risk = min(0.4, pod_lifetime_risk - risk_threshold)
                    effective_factor *= (1.0 + excess_risk * 0.4)  # up to ~16% boost
                else:  # Below target but >= 90%
                    excess_risk = min(0.3, pod_lifetime_risk - risk_threshold)
                    effective_factor *= (1.0 + excess_risk * 0.3)  # up to ~9% boost
    
    # Adaptive pressure multiplier: combines success gap, cost, and failure pattern
    target_success = get_target_success_rate(workflow_name)
    normalized_cost = cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR if cost_per_pod_per_sec and cost_per_pod_per_sec > 0 else 1.0
    pressure_multiplier = compute_adaptive_pressure_multiplier(
        workflow_success_rate, target_success, normalized_cost, task_failure_rate=task_failure_rate
    )
    effective_factor *= pressure_multiplier

    return max(1.0, effective_factor)  # Ensure >= 1.0

def get_critical_tasks(metadata, top_n=3, workflows_dir=None, workflow_name=None, task_failure_rates=None,
                      cost_per_pod_per_sec=None, critical_path_metrics=None):
    """Get the top N most critical tasks considering HEFT rank, throughput bottlenecks, failure rates, 
    cost, and critical path (from TCC-CloudWorkflow2014 paper).
    
    A throughput bottleneck occurs when a task receives data from predecessors with higher minscale,
    creating a processing bottleneck (e.g., task6 with minscale=2 receiving from task5 with minscale=6).
    
    Critical path tasks have zero slack and MUST be scaled aggressively for deadline compliance.
    
    For wf-2, also includes high-failure-rate tasks (task3a, task3b) that are causing drop-offs.
    
    Args:
        metadata: Task metadata dict with minscale and heft_rank
        top_n: Number of critical tasks to return
        workflows_dir: Directory containing workflow definitions (optional)
        workflow_name: Workflow name to load tasks.json (optional)
        task_failure_rates: Optional dict of task_id -> failure_rate for failure-aware selection
        cost_per_pod_per_sec: Optional cost per pod per second for cost-aware HEFT rank adjustment
        critical_path_metrics: Critical path analysis from compute_critical_path_metrics() (optional)
    
    Returns:
        List of task IDs ordered by criticality (highest first)
    """
    # Get bottleneck ratios (for tasks that RECEIVE from bottlenecks)
    bottleneck_ratios = get_bottleneck_ratios(metadata, workflows_dir, workflow_name)
    
    # Get bottleneck predecessors (tasks that FEED INTO bottlenecks) - these are critical!
    bottleneck_predecessors = get_bottleneck_predecessors(metadata, workflows_dir, workflow_name)
    
    # Get critical path tasks (from TCC-CloudWorkflow2014 paper)
    critical_path_tasks = (critical_path_metrics or {}).get('critical_path_tasks', set())
    task_criticality = (critical_path_metrics or {}).get('task_criticality', {})
    
    # Calculate combined criticality score: HEFT rank + bottleneck severity + failure rate + cost adjustment
    criticality_scores = []
    for task_id, info in metadata.items():
        heft_rank = info.get("heft_rank", 0)
        bottleneck_ratio = bottleneck_ratios.get(task_id, 1.0)
        bottleneck_pred_ratio = bottleneck_predecessors.get(task_id, 1.0)  # If this task feeds into a bottleneck
        failure_rate = task_failure_rates.get(task_id, 0.0) if task_failure_rates else 0.0
        
        # Cost-aware HEFT rank adjustment: boost cheaper tasks, reduce expensive ones
        cost_adjusted_heft = heft_rank
        if cost_per_pod_per_sec and cost_per_pod_per_sec > 0:
            # Normalize cost (baseline = $COST_BASELINE_PER_POD_HOUR/pod/hour)
            normalized_cost = cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
            if normalized_cost > 2.0:  # Very expensive (>2x baseline)
                # Reduce HEFT rank by up to 20% for expensive tasks
                cost_penalty = min(0.20, (normalized_cost - 2.0) * 0.10)
                cost_adjusted_heft *= (1.0 - cost_penalty)
            elif normalized_cost < 0.5:  # Cheap (<0.5x baseline)
                # Boost HEFT rank by up to 15% for cheap tasks
                cost_boost = min(0.15, (0.5 - normalized_cost) * 0.30)
                cost_adjusted_heft *= (1.0 + cost_boost)
        
        # Combined score: cost-adjusted HEFT rank (primary) + bottleneck multiplier (secondary) 
        #                 + failure rate boost + critical path boost (from TCC-CloudWorkflow2014 paper)
        # Bottleneck ratio of 3.0 means 3x more data than can be processed
        # We add (bottleneck_ratio - 1) * 10 to boost bottleneck tasks
        bottleneck_boost = (bottleneck_ratio - 1.0) * 10 if bottleneck_ratio > 1.0 else 0
        
        # CRITICAL: Boost for bottleneck predecessors (tasks feeding INTO bottlenecks)
        # These tasks (like task5 in wf-5) often have high failure rates due to backpressure
        # They should get HIGHER priority than the bottleneck receivers
        if bottleneck_pred_ratio > 1.3:
            # Significant boost for tasks that feed into bottlenecks
            # A 3:1 fan-in ratio → boost of 20 points
            bottleneck_pred_boost = (bottleneck_pred_ratio - 1.0) * 15
            bottleneck_boost = max(bottleneck_boost, bottleneck_pred_boost)
        
        # Boost score for high failure rates (especially important for wf-2's task3a/task3b)
        # CRITICAL FIX: Lower threshold to 20% and use quadratic scaling for high failure rates
        # task5 in wf-5 has ~37% drop-off and MUST be prioritized
        if failure_rate > 0.30:  # >30% drop-off: major bottleneck
            failure_boost = failure_rate * 50.0  # 50x weight for severe failures
        elif failure_rate > 0.20:  # 20-30% drop-off: significant issue
            failure_boost = failure_rate * 30.0  # 30x weight
        elif failure_rate > 0.10:  # 10-20% drop-off: notable issue
            failure_boost = failure_rate * 15.0  # 15x weight
        else:
            failure_boost = 0
        
        # Critical path boost: tasks on critical path get significant score boost
        # criticality=0 means on critical path, criticality=1 means max slack
        criticality_score = task_criticality.get(task_id, 0.5)
        if task_id in critical_path_tasks:
            critical_path_boost = 25.0  # Major boost for critical path tasks
        else:
            # Inverse of criticality: near-critical tasks get partial boost
            critical_path_boost = (1.0 - criticality_score) * 15.0
        
        combined_score = cost_adjusted_heft + bottleneck_boost + failure_boost + critical_path_boost
        
        is_on_critical_path = task_id in critical_path_tasks
        criticality_scores.append((task_id, combined_score, heft_rank, bottleneck_ratio, bottleneck_pred_ratio, failure_rate, is_on_critical_path, critical_path_boost))
    
    # Sort by combined score (highest first)
    criticality_scores.sort(key=lambda x: x[1], reverse=True)
    
    # Return top N (tuple: task_id, score, heft_rank, bottleneck, bottleneck_pred, failure_rate, is_critical, cp_boost)
    result = [task_id for task_id, _, _, _, _, _, _, _ in criticality_scores[:top_n]]
    
    # For wf-2, ensure task3a and task3b are included if they have high failure rates
    # These are critical parallel branches that both must complete
    if workflow_name == 'wf-2' and task_failure_rates:
        high_failure_tasks = []
        for task_id in ['task3a', 'task3b']:
            if task_id in metadata and task_failure_rates.get(task_id, 0) > 0.40:
                high_failure_tasks.append(task_id)
        
        # Include high-failure tasks even if they're not in top N by HEFT
        for task in high_failure_tasks:
            if task not in result:
                result.append(task)
                failure_pct = task_failure_rates.get(task, 0) * 100
                print(f"   🚨 Added {task} to critical tasks due to high failure rate ({failure_pct:.1f}%)")
    
    # CRITICAL FIX: For ALL complex workflows, ensure high-failure tasks are included
    # This is essential for wf-5 where task5 has ~37% drop-off but low HEFT rank
    if workflow_name in COMPLEX_WORKFLOWS and task_failure_rates:
        # Find all tasks with >25% failure rate that aren't already in result
        high_failure_tasks = [
            (task_id, fr) for task_id, fr in task_failure_rates.items()
            if fr > 0.25 and task_id not in result and task_id in metadata
        ]
        # Sort by failure rate (highest first) and add top 2
        high_failure_tasks.sort(key=lambda x: x[1], reverse=True)
        for task_id, fr in high_failure_tasks[:2]:
            result.append(task_id)
            print(f"   🚨 Added {task_id} to critical tasks due to high failure rate ({fr*100:.1f}%)")
    
    # PROACTIVE: For ALL complex workflows, ensure bottleneck predecessor tasks are included
    # These tasks feed into bottlenecks and need proactive scaling before failure rates spike
    if workflow_name in COMPLEX_WORKFLOWS and bottleneck_predecessors:
        for task_id, ratio in sorted(bottleneck_predecessors.items(), key=lambda x: -x[1]):
            if task_id not in result and task_id in metadata and ratio > 2.0:  # Only severe bottleneck predecessors (>2:1)
                result.append(task_id)
                print(f"   🔥 Added {task_id} to critical tasks: feeds into {ratio:.1f}:1 bottleneck")
    
    # Print detailed info for debugging
    if bottleneck_ratios or bottleneck_predecessors or critical_path_tasks or (task_failure_rates and any(fr > 0.20 for fr in task_failure_rates.values())):
        print(f"🔍 Critical task selection (with TCC-CloudWorkflow2014 critical path analysis):")
        for task_id, score, heft_rank, bottleneck, bottleneck_pred, failure_rate, is_critical, cp_boost in criticality_scores[:min(top_n + 2, len(criticality_scores))]:
            parts = [f"HEFT={heft_rank:.1f}"]
            if bottleneck > 1.0:
                parts.append(f"receives={bottleneck:.1f}x")
            if bottleneck_pred > 1.3:
                parts.append(f"🔥feeds_into={bottleneck_pred:.1f}x")  # Critical: this task feeds into a bottleneck
            if failure_rate > 0.20:  # Show failure rates above 20%
                parts.append(f"failure={failure_rate*100:.1f}%")
            if is_critical:
                parts.append("🔴CRITICAL_PATH")
            elif cp_boost > 5:
                parts.append(f"near-critical(+{cp_boost:.0f})")
            parts.append(f"score={score:.1f}")
            print(f"   {task_id}: {', '.join(parts)}")
    
    return result

# Complex workflows that need more aggressive scaling (lower cost-benefit threshold)
# Workflow classifications based on DAG depth and complexity
# Simple workflows (wf-1, wf-2, wf-3, wf-4, wf-7): Shallow DAGs, fewer dependencies
# Target success rates (workflow classifications defined at top of file)
# Simple workflows: target 95-100% (use 97.5% as midpoint)
# Complex workflows: target 65-70% (use 67.5% as midpoint)
SIMPLE_WORKFLOW_TARGET_SUCCESS_RATE = 0.975   # 97.5%
COMPLEX_WORKFLOW_TARGET_SUCCESS_RATE = 0.675    # 67.5%

def get_target_success_rate(workflow_name: str) -> float:
    """Get target success rate for a workflow based on its complexity.
    
    Args:
        workflow_name: Workflow identifier (e.g., 'wf-1', 'wf-5')
    
    Returns:
        Target success rate (0.0-1.0)
    """
    if workflow_name in COMPLEX_WORKFLOWS:
        return COMPLEX_WORKFLOW_TARGET_SUCCESS_RATE
    else:
        return SIMPLE_WORKFLOW_TARGET_SUCCESS_RATE

def evaluate_pareto_guard(success_rate, target_success, real_cost_per_pod_per_sec):
    """
    Pareto-based cost control evaluation.
    Returns (trigger, reason, normalized_cost, success_margin).
    """
    if success_rate is None or target_success is None:
        return False, "missing_success_rate", None, None
    if real_cost_per_pod_per_sec is None or real_cost_per_pod_per_sec <= 0:
        return False, "missing_cost", None, success_rate - target_success

    # Normalized cost relative to realistic baseline ($COST_BASELINE_PER_POD_HOUR/pod-hour)
    normalized_cost = (real_cost_per_pod_per_sec * 3600) / COST_BASELINE_PER_POD_HOUR
    success_margin = success_rate - target_success
    if success_margin >= PARETO_SUCCESS_MARGIN and normalized_cost >= PARETO_COST_NORM_THRESHOLD:
        return True, "triggered", normalized_cost, success_margin
    if success_margin < PARETO_SUCCESS_MARGIN:
        return False, "success_below_margin", normalized_cost, success_margin
    return False, "cost_below_threshold", normalized_cost, success_margin

def record_pareto_savings(task_id, pods_avoided, cost_per_pod_per_sec, scope_label):
    """Record and log estimated cost savings when Pareto guard skips scaling."""
    if pods_avoided <= 0:
        return
    pareto_savings_tracker['skipped_scale_events'] += 1
    if cost_per_pod_per_sec and cost_per_pod_per_sec > 0:
        saved_per_hour = pods_avoided * cost_per_pod_per_sec * 3600
        pareto_savings_tracker['saved_dollars_per_hour'] += saved_per_hour
        print(f"   💸 Pareto savings ({scope_label}): skipped +{pods_avoided} pods for {task_id} "
              f"≈ ${saved_per_hour:.2f}/hour (cumulative ${pareto_savings_tracker['saved_dollars_per_hour']:.2f}/hour)")
    else:
        print(f"   💸 Pareto savings ({scope_label}): skipped +{pods_avoided} pods for {task_id} "
              f"(cost unavailable)")

def calculate_kaplan_meier_cost_benefit(current_scale, new_scale, base_scale, survival_prob,
                                        exec_time=0.25, unit_cost=1.0, workflow_name=None,
                                        real_cost_per_pod_per_sec=None):
    """Calculate cost-benefit ratio for Kaplan-Meier preemptive scaling.
    
    Benefit is based on survival probability improvement (higher scale = lower eviction risk).
    Cost is based on additional pods needed.
    
    Args:
        current_scale: Current minscale
        new_scale: Proposed new minscale
        base_scale: Base minscale from metadata
        survival_prob: Current survival probability (0.0-1.0) - lower means higher risk
        exec_time: Task execution time in seconds (default 0.25)
        unit_cost: Cost per pod per second (default 1.0, used as fallback)
        workflow_name: Optional workflow name for complex workflow-specific adjustments
        real_cost_per_pod_per_sec: Real cost per pod per second from cost_logs (optional)
    
    Returns:
        Cost-benefit ratio (benefit / cost). Higher is better.
    """
    # Use real cost if available, otherwise fall back to unit_cost
    effective_unit_cost = real_cost_per_pod_per_sec if real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0 else unit_cost
    
    # Cost increase: additional pods * execution time * effective unit cost
    additional_pods = new_scale - current_scale
    cost_increase = additional_pods * exec_time * effective_unit_cost
    
    # Benefit: improved survival probability
    # Lower survival_prob means higher risk, so scaling is more beneficial
    # Scale improvement factor
    scale_improvement = (new_scale / max(current_scale, 1)) - 1.0
    
    # Benefit is based on risk reduction (1 - survival_prob)
    # Higher scale improves survival probability, reducing eviction risk
    # For bottleneck tasks, benefit is even higher (they're critical for throughput)
    risk_level = 1.0 - survival_prob  # Higher risk = more benefit from scaling
    
    # Base benefit: scale improvement * risk level
    # More pods = better survival probability, especially for bottleneck tasks
    base_benefit = scale_improvement * risk_level * 0.1
    
    # For complex workflows BELOW target, increase benefit (they're more critical)
    # But when AT/ABOVE target, use normal cost-benefit (no artificial boost)
    target_for_cb = get_target_success_rate(workflow_name)
    # Note: survival_prob is NOT success_rate, but lower survival_prob = higher risk
    # We check if risk is high (survival_prob < 0.5) to justify complex workflow boost
    if workflow_name in COMPLEX_WORKFLOWS and survival_prob < 0.5:
        base_benefit *= 1.3  # 30% more benefit for complex workflows AT RISK (was 50%)
        cost_increase = cost_increase * 0.9  # 10% cost reduction (was 20%)
    
    # For workflows with target success rates, adjust cost-benefit to achieve targets
    # Simple workflows: target 85-90% (more lenient to maintain high success rates)
    # Complex workflows: target 45-55% (already handled above, but ensure we're pushing toward target)
    target_success_rate = get_target_success_rate(workflow_name)
    
    if workflow_name not in COMPLEX_WORKFLOWS:
        # Simple workflows need more lenient cost-benefit to maintain 85-90% target
        # Higher target = more lenient (to prevent blocking necessary scaling)
        # Formula: benefit_multiplier = 1.0 + (target_success_rate * 1.0)
        #          cost_reduction = 1.0 - (target_success_rate * 0.5)
        # For 87.5% target: 1.875x benefit, 56.25% cost reduction
        benefit_multiplier = 1.0 + (target_success_rate * 1.0)  # 1.0-2.0 range
        cost_reduction = 1.0 - (target_success_rate * 0.5)  # 0.5-1.0 range (50%-0% reduction)
        base_benefit *= benefit_multiplier
        cost_increase = cost_increase * cost_reduction
    
    # For very high risk (survival_prob < 0.3), be more aggressive
    if survival_prob < 0.3:
        base_benefit *= 2.0  # Double benefit for high-risk situations
        cost_increase = cost_increase * 0.6  # 40% cost reduction
    
    benefit = min(base_benefit, 0.3)  # Cap benefit at reasonable maximum
    
    # Avoid division by zero
    if cost_increase <= 0:
        return float('inf')
    
    return benefit / cost_increase

def calculate_cost_benefit_ratio(current_scale, new_scale, base_scale, success_rate, 
                                 exec_time=0.25, unit_cost=1.0, workflow_name=None,
                                 real_cost_per_pod_per_sec=None):
    """Calculate cost-benefit ratio for scaling decision.
    
    When success rates are very low (<20%), we use emergency scaling logic
    that prioritizes recovery over cost efficiency.
    
    Args:
        current_scale: Current minscale
        new_scale: Proposed new minscale
        base_scale: Base minscale from metadata
        success_rate: Current workflow success rate (0.0-1.0)
        exec_time: Task execution time in seconds (default 0.25)
        unit_cost: Cost per pod per second (default 1.0, used as fallback)
        workflow_name: Optional workflow name for complex workflow-specific adjustments
        real_cost_per_pod_per_sec: Real cost per pod per second from cost_logs (optional)
    
    Returns:
        Cost-benefit ratio (benefit / cost). Higher is better.
    """
    # Use real cost if available, otherwise fall back to unit_cost
    effective_unit_cost = real_cost_per_pod_per_sec if real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0 else unit_cost
    
    # Cost increase: additional pods * execution time * effective unit cost
    additional_pods = new_scale - current_scale
    cost_increase = additional_pods * exec_time * effective_unit_cost
    
    # REMOVED emergency scaling mode - cost controls take priority
    # Retry service handles recovery; scaler should make cost-conscious decisions
    
    # Standard cost-benefit calculation (no emergency bypass)
    scale_improvement = (new_scale / max(current_scale, 1)) - 1.0
    
    # Benefit estimation based on workflow type and success rate
    target = get_target_success_rate(workflow_name)
    success_rate_gap = target - success_rate
    
    # Adjust benefit multipliers based on workflow type
    if workflow_name in COMPLEX_WORKFLOWS:
        improvement_multiplier = 0.15
        max_improvement = 0.20
    else:  # Simple workflows
        improvement_multiplier = 0.12
        max_improvement = 0.18
    
    # Estimated success rate improvement (diminishing returns)
    # When success rate is below target, benefit is higher (we need to improve)
    # When success rate is above target, benefit is lower (we're optimizing, not recovering)
    if success_rate < target:
        # Below target: higher benefit to encourage scaling
        gap_multiplier = 1.0 + min(0.5, success_rate_gap * 2.0)  # Up to 1.5x when far below target
        success_rate_improvement = min(scale_improvement * (1 - success_rate) * improvement_multiplier * gap_multiplier, max_improvement)
        benefit = success_rate_improvement
    else:
        # Above target: lower benefit, but still allow scaling for survival probability
        # Use a base benefit that doesn't depend on success rate gap
        base_benefit = scale_improvement * 0.03  # Small base benefit for survival improvements
        success_rate_improvement = min(scale_improvement * (1 - success_rate) * improvement_multiplier * 0.5, max_improvement * 0.5)
        benefit = max(base_benefit, success_rate_improvement)  # Take the higher of the two
    
    # Cost adjustment based on success rate and workflow type
    # When below target, reduce cost penalty to prioritize success
    # When above target, apply cost penalty to optimize spending
    if success_rate < target:
        # Below target: reduce cost penalty to prioritize recovery
        if workflow_name in COMPLEX_WORKFLOWS:
            cost_increase = cost_increase * 0.7  # 30% cost reduction for complex workflows
        else:
            cost_increase = cost_increase * 0.75  # 25% cost reduction for simple workflows
    else:
        # Above target: apply cost penalty to optimize spending
        if workflow_name in COMPLEX_WORKFLOWS:
            cost_increase = cost_increase * 0.9  # 10% cost penalty for complex workflows
        else:
            cost_increase = cost_increase * 0.95  # 5% cost penalty for simple workflows
    
    # Avoid division by zero
    if cost_increase <= 0:
        return float('inf')
    
    return benefit / cost_increase

def scale_critical_tasks(critical_tasks, all_function_specs, metadata, scale_factor=2.0, 
                         max_scale=None, last_scaled=None, scale_cooldown=60,
                         success_rate=0.5, min_cost_benefit_ratio=0.01, workflow_name=None,
                         task_depths=None, max_depth=None, task_failure_rates=None,
                         bottleneck_ratios=None, real_cost_per_pod_per_sec=None,
                         critical_path_metrics=None):
    """Scale up critical tasks by the specified factor, with cost optimization features.
    
    Args:
        critical_tasks: List of task IDs to scale
        all_function_specs: Current function specs
        metadata: Workflow metadata
        scale_factor: Multiplicative factor (default 2.0)
        max_scale: Maximum absolute scale limit (None = no limit)
        last_scaled: Dict mapping task_id -> last scaling timestamp
        scale_cooldown: Minimum seconds between scaling actions (default 180s)
        success_rate: Current workflow success rate (0.0-1.0) for cost-benefit analysis
        min_cost_benefit_ratio: Minimum cost-benefit ratio to proceed (default 0.01)
        workflow_name: Workflow name for complex workflow-specific adjustments
        task_depths: Optional dict task_id -> DAG depth for position-aware scaling
        max_depth: Maximum DAG depth (for normalizing position weights)
        task_failure_rates: Optional dict task_id -> drop-off rate for failure-aware scaling
        bottleneck_ratios: Optional dict task_id -> bottleneck_ratio for bottleneck-aware scaling
        real_cost_per_pod_per_sec: Real cost per pod per second from cost_logs (optional)
    """
    # =========================================================================
    # BUDGET ENVELOPE CHECK: Hard block when budget exhausted
    # =========================================================================
    budget_exhausted, should_block = is_budget_exhausted(
        current_success_rate=success_rate,
        workflow_name=workflow_name,
    )
    budget_throttle_factor = 1.0
    if budget_exhausted:
        if should_block:
            print(f"🛑 BUDGET ENVELOPE: Scaling disabled. Relying on retry service for recovery.")
            return last_scaled if last_scaled else {}
        budget_throttle_factor = min(budget_throttle_factor, BUDGET_EMERGENCY_THROTTLE)
        print(f"⚠️ BUDGET BYPASS: Limited scaling enabled (throttle={budget_throttle_factor*100:.0f}%)")
    
    # =========================================================================
    # BUDGET PACING: Throttle scaling if spending ahead of schedule
    # =========================================================================
    pacing_throttle, pace_ratio = get_budget_pacing_throttle()
    if pacing_throttle < 1.0:
        budget_throttle_factor *= pacing_throttle
        print(f"⏱️ BUDGET PACING: Scaling throttled to {pacing_throttle*100:.0f}% (spending {pace_ratio:.1f}x ahead of schedule)")
    
    # Ensure we always scale up, not down
    effective_scale_factor = max(scale_factor, 1.0) * budget_throttle_factor
    current_time = time.time()
    
    if last_scaled is None:
        last_scaled = {}
    
    # Define target_success early for use throughout the function
    target_success = get_target_success_rate(workflow_name)
    workflow_norm_cost = (real_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                          if real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0 else 1.0)
    cost_win_throttle = get_cost_win_throttle(success_rate, target_success, workflow_norm_cost, workflow_name=workflow_name)
    cost_win_threshold_boost = 1.0
    if cost_win_throttle < 1.0:
        recent_gain = get_recent_success_gain()
        cost_win_threshold_boost = 1.0 + (1.0 - cost_win_throttle) * 1.5
        print(f"COST-WIN: success={success_rate*100:.1f}% target={target_success*100:.1f}% "
              f"gain={recent_gain*100:.2f}% norm_cost={workflow_norm_cost:.2f} "
              f"throttle={cost_win_throttle*100:.0f}%")
    
    current_total_pods = None
    if all_function_specs:
        current_total_pods = sum(spec.get('minscale', 1) for spec in all_function_specs.values())
    
    for task_id in critical_tasks:
        func_spec = all_function_specs.get(task_id)
        if not func_spec:
            continue
        
        # STRICT POD BUDGET: When pod budget is exceeded, ALWAYS skip scaling
        # No more critical-path bypass - this was causing cost blowup
        if scaling_efficiency_tracker['scaling_paused'] and "Total pods" in (scaling_efficiency_tracker.get('pause_reason') or ""):
            print(f"   🛑 POD BUDGET EXCEEDED: Skipping {task_id} scale (total pods {current_total_pods}/{MAX_TOTAL_PODS})")
            continue

        # CPSP CHECK: Soft throttle for tasks with poor historical cost efficiency
        # Now passes success_rate for emergency bypass
        should_scale_cpsp, cpsp_reason = should_scale_based_on_cpsp(task_id, current_success_rate=success_rate)
        cpsp_throttle = 1.0
        if not should_scale_cpsp:
            # Don't skip entirely - just throttle scaling by 50%
            cpsp_throttle = 0.5
            print(f"   💸 CPSP SOFT: Throttling {task_id} to 50% - {cpsp_reason}")
            
        # Dynamic cooldown: reduce cooldown when success rate is critically low
        # Keep targeted adjustments (wf-5, wf-8, wf-9); others use generic complex rules
        effective_cooldown = scale_cooldown
        # OPTION A: Conservative Cost Reduction - minimum cooldown raised to 45s
        if workflow_name == 'wf-5':
            # wf-5: Long linear DAG with task5 as major bottleneck (37% drop-off)
            target = get_target_success_rate(workflow_name)
            if success_rate < 0.40:  # Critical: < 40% (way below 67.5% target)
                effective_cooldown = max(45, scale_cooldown // 4)  # 25% of normal (min 45s)
            elif success_rate < 0.50:  # Low: 40-50%
                effective_cooldown = max(45, scale_cooldown // 3)  # 33% of normal (min 45s)
            elif success_rate < target:  # Below target but closer
                effective_cooldown = max(60, scale_cooldown // 2)  # 50% of normal (min 60s)
            else:
                effective_cooldown = max(90, scale_cooldown * 2 // 3)  # 67% of normal (min 90s)
        elif workflow_name == 'wf-8':
            # wf-8: Target 78.4%+ to beat static_over_2x 73.4% by 5%
            if success_rate < 0.60:  # Below hourglass performance
                effective_cooldown = max(60, scale_cooldown // 2)  # 50% of normal (min 60s)
            elif success_rate < 0.74:  # Below static_over_2x performance
                effective_cooldown = max(90, scale_cooldown * 2 // 3)  # 67% of normal (min 90s)
            else:
                effective_cooldown = max(90, scale_cooldown * 2 // 3)  # 67% of normal (min 90s)
        elif workflow_name == 'wf-9':
            # wf-9: Most complex workflow
            # Target is 67.5%, so adjust cooldown based on how far below target we are
            target = get_target_success_rate(workflow_name)
            if success_rate < 0.40:  # Critical: < 40% (way below 67.5% target)
                effective_cooldown = max(45, scale_cooldown // 4)  # 25% of normal (min 45s)
            elif success_rate < target * 0.60:  # Below 60% of target (40.5%)
                effective_cooldown = max(60, scale_cooldown // 3)  # 33% of normal (min 60s)
            elif success_rate < target * 0.80:  # Below 80% of target (54%)
                effective_cooldown = max(60, scale_cooldown // 2)  # 50% of normal (min 60s)
            else:
                effective_cooldown = max(90, scale_cooldown * 2 // 3)  # 67% of normal (min 90s)
        elif workflow_name in COMPLEX_WORKFLOWS:
            # Complex workflows: moderate cooldown reduction
            if success_rate < 0.30:  # Critical: < 30% (well below 67.5% target)
                effective_cooldown = max(45, scale_cooldown // 4)  # 25% of normal (min 45s)
            elif success_rate < 0.45:  # Low: 30-45% (below target)
                effective_cooldown = max(60, scale_cooldown // 3)  # 33% of normal (min 60s)
            elif success_rate < target_success:  # Below target but closer
                effective_cooldown = max(60, scale_cooldown // 2)  # 50% of normal (min 60s)
        else:
            # Simple workflows: never reduce cooldown to avoid thrash
            effective_cooldown = scale_cooldown
        
        # Check cooldown period (using effective cooldown)
        if task_id in last_scaled:
            time_since_last_scale = current_time - last_scaled[task_id]
            if time_since_last_scale < effective_cooldown:
                remaining = int(effective_cooldown - time_since_last_scale)
                if effective_cooldown < scale_cooldown:
                    print(f"⏸️  {task_id} in cooldown ({remaining}s remaining, reduced from {scale_cooldown}s due to low success rate {success_rate*100:.1f}%)")
                else:
                    print(f"⏸️  {task_id} in cooldown ({remaining}s remaining)")
            continue
            
        current_minscale = func_spec['minscale']
        base_minscale = metadata[task_id].get("minscale", 1)
        exec_time = metadata[task_id].get("exec_time", 0.25)
        
        # FAILURE-FOCUSED SCALING: Boost tasks based on their ACTUAL drop-off rates, not just global success rate
        # This targets resources at the tasks that are actually failing instead of blindly scaling everything
        min_scale_boost = 1.0
        target = get_target_success_rate(workflow_name)
        
        # Get this task's failure rate (drop-off rate)
        task_drop_off = task_failure_rates.get(task_id, 0.0) if task_failure_rates else 0.0
        
        # FAILURE-FOCUSED: Tasks with high drop-off get priority scaling
        # Reduced boosts to prevent cost explosion (was 1.8/1.5/1.3/1.15)
        if task_drop_off > 0.40:  # >40% drop-off: This task is a major bottleneck (was 30%)
            min_scale_boost = 1.4  # 40% boost for high-failure tasks (was 80%)
            print(f"   🚨 FAILURE-FOCUSED: {task_id} has {task_drop_off*100:.1f}% drop-off → boosting by 1.4x")
        elif task_drop_off > 0.30:  # 30-40% drop-off: Significant bottleneck (was 20%)
            min_scale_boost = 1.25  # 25% boost (was 50%)
            print(f"   ⚠️ FAILURE-FOCUSED: {task_id} has {task_drop_off*100:.1f}% drop-off → boosting by 1.25x")
        elif task_drop_off > 0.20:  # 20-30% drop-off: Moderate bottleneck (was 10%)
            min_scale_boost = 1.15  # 15% boost (was 30%)
            print(f"   📊 FAILURE-FOCUSED: {task_id} has {task_drop_off*100:.1f}% drop-off → boosting by 1.15x")
        elif task_drop_off > 0.10:  # 10-20% drop-off: Minor bottleneck (was 5%)
            min_scale_boost = 1.1  # 10% boost (was 15%)
        # Tasks with <5% drop-off: No boost needed - they're working fine
        
        # GLOBAL SUCCESS RATE BOOST: Apply additional boost only when workflow is critically low
        # But ONLY for tasks that have SOME failure rate (not 0% drop-off tasks)
        if success_rate < target * 0.40 and task_drop_off > 0.15:  # Below 40% of target AND task has significant failures
            additional_boost = 1.1  # 10% additional boost for critical situations (was 20%)
            min_scale_boost *= additional_boost
            print(f"   🔥 CRITICAL workflow ({success_rate*100:.1f}% < {target*0.40*100:.1f}%): additional {additional_boost}x boost for {task_id}")
        
        # Workflow-specific adjustments (smaller than before, since failure-focused does heavy lifting)
        if workflow_name in COMPLEX_WORKFLOWS and success_rate < target * 0.80 and task_drop_off < 0.05:
            # Complex workflow below target but this task has low drop-off
            # Give modest boost since other tasks are the bottleneck
            min_scale_boost = max(min_scale_boost, 1.1)
        elif workflow_name in SIMPLE_WORKFLOWS and success_rate < target * 0.90 and task_drop_off < 0.05:
            # Simple workflow below target but this task has low drop-off
            min_scale_boost = max(min_scale_boost, 1.1)
        
        # Emergency boosts REMOVED - cost controls take priority
        # The retry service handles recovery; scaler should not bypass cost controls
        # Only apply minimal boost if success is extremely low AND cost-win allows it
        is_complex_wf = workflow_name in COMPLEX_WORKFLOWS
        if not scaling_efficiency_tracker['scaling_paused'] and cost_win_throttle >= 0.5:
            # Only boost if cost controls allow (throttle >= 50%)
            if success_rate < 0.10:  # Critical: < 10% success rate
                emergency_boost = 1.1 if is_complex_wf else 1.15  # Very conservative
                min_scale_boost = max(min_scale_boost, emergency_boost)
                print(f"   🚨 CRITICAL (cost-allowed): {task_id} boost {min_scale_boost:.1f}x (success {success_rate*100:.1f}%)")
        else:
            # Scaling paused due to efficiency issues - use minimal boost
            min_scale_boost = min(min_scale_boost, 1.2)
            print(f"   ⏸️ Scaling efficiency pause: limiting {task_id} boost to {min_scale_boost:.1f}x ({scaling_efficiency_tracker['pause_reason']})")
        
        # =====================================================================
        # RAY-INSPIRED OPTIMIZATIONS
        # =====================================================================
        
        # RAY-INSPIRED 1: Lineage-aware downstream pressure
        # If downstream tasks are struggling, boost upstream to prevent pipeline starvation
        downstream_pressure = get_downstream_pressure(task_id, metadata, task_failure_rates, bottleneck_ratios)
        if downstream_pressure > 1.0:
            min_scale_boost *= downstream_pressure
            print(f"   🔗 RAY-LINEAGE: {task_id} downstream pressure → boost ×{downstream_pressure:.2f} (total: {min_scale_boost:.2f}x)")
        
        # RAY-INSPIRED 1b: Upstream backpressure propagation
        # If a downstream task (successor) has high failure rate, boost THIS task to keep pipeline flowing
        # This is critical for fan-in/fan-out DAGs like wf-5 where task5 (6 subtasks) feeds into task6 (2 subtasks)
        successors = metadata.get(task_id, {}).get('successors', [])
        for successor_id in successors:
            successor_backpressure = get_upstream_backpressure(successor_id, metadata, task_failure_rates, bottleneck_ratios)
            if successor_backpressure > 1.0:
                min_scale_boost *= successor_backpressure
                print(f"   🔙 RAY-BACKPRESSURE: {task_id} → {successor_id} has high failure, boosting {task_id} by ×{successor_backpressure:.2f} (total: {min_scale_boost:.2f}x)")
        
        # RAY-INSPIRED 2: Checkpoint-aware scaling priority
        # Tasks with checkpoints recover faster, so prioritize scaling them
        checkpoint_priority = get_checkpoint_scaling_priority(task_id, metadata)
        if checkpoint_priority > 1.0:
            min_scale_boost *= checkpoint_priority
        
        # Burst scaling removed - retry service handles rapid recovery
        
        # =====================================================================
        # ROI-BASED THROTTLING
        # =====================================================================
        # Apply ROI-based throttling to reduce scaling when ROI is negative
        roi_throttle = get_roi_throttle_factor()
        if roi_throttle < 1.0:
            original_boost = min_scale_boost
            min_scale_boost = 1.0 + (min_scale_boost - 1.0) * roi_throttle  # Scale down the boost
            print(f"   📉 ROI-THROTTLE: {task_id} boost {original_boost:.2f}x → {min_scale_boost:.2f}x (ROI throttle={roi_throttle*100:.0f}%)")

        # COST-WIN THROTTLING: BLOCK scaling when throttle is very low
        # This is the key to reducing costs - actually skip scaling, not just reduce boost
        if cost_win_throttle < 1.0:
            # If throttle is very low (< 30%), SKIP scaling entirely - NO EMERGENCY BYPASS
            if cost_win_throttle < 0.30:
                print(f"   🛑 COST-WIN BLOCK: {task_id} scaling SKIPPED (throttle={cost_win_throttle*100:.0f}% < 30%)")
                continue  # Skip this task entirely - cost controls always apply
            original_boost = min_scale_boost
            min_scale_boost = 1.0 + (min_scale_boost - 1.0) * cost_win_throttle
            if abs(original_boost - min_scale_boost) >= 0.01:
                print(f"   COST-WIN: {task_id} boost {original_boost:.2f}x → {min_scale_boost:.2f}x")
        
        # Apply min_scale boost to base before calculating new scale
        boosted_base_minscale = math.ceil(base_minscale * min_scale_boost)
        # Ensure we don't scale below the boosted minimum
        effective_base = max(current_minscale, boosted_base_minscale)
        
        # Blend success, cost, and failure signals into the scale factor (live cache driven)
        normalized_cost = real_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR if real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0 else 1.0
        failure_rate = task_failure_rates.get(task_id, 0.0) if task_failure_rates else 0.0
        pressure_multiplier = compute_adaptive_pressure_multiplier(success_rate, target, normalized_cost, task_failure_rate=failure_rate)
        
        # Apply ROI throttle to pressure multiplier as well
        if roi_throttle < 1.0 and pressure_multiplier > 1.0:
            pressure_multiplier = 1.0 + (pressure_multiplier - 1.0) * roi_throttle

        # Apply COST-WIN throttle to pressure multiplier as well
        if cost_win_throttle < 1.0 and pressure_multiplier > 1.0:
            pressure_multiplier = 1.0 + (pressure_multiplier - 1.0) * cost_win_throttle
        
        effective_scale_factor *= pressure_multiplier

        # Always scale up from CURRENT scale, not base, to avoid scaling down
        # This ensures burst scaling continues to grow from current level
        # Add guard for all workflows: cap per-tick growth to prevent runaway scaling
        capped_scale_factor = effective_scale_factor
        
        # If scaling efficiency is paused, use very conservative scaling
        if scaling_efficiency_tracker['scaling_paused']:
            capped_scale_factor = min(effective_scale_factor, 1.2)  # Max 20% per tick when paused
            print(f"   ⏸️ Scaling efficiency pause: limiting per-tick scale factor for {task_id} to 1.2x")
        elif workflow_name == 'wf-9':
            # wf-9: Conservative per-tick scaling (REDUCED to prevent cost explosion)
            if success_rate < 0.40:  # Critical: < 40%
                capped_scale_factor = min(effective_scale_factor, 1.4)  # Up to 1.4x per tick (was 1.6x)
            elif success_rate < target * 0.60:  # Below 60% of target
                capped_scale_factor = min(effective_scale_factor, 1.3)  # Up to 1.3x per tick (was 1.5x)
            elif success_rate < target * 0.80:  # Below 80% of target
                capped_scale_factor = min(effective_scale_factor, 1.25)  # Up to 1.25x per tick (was 1.4x)
            else:
                capped_scale_factor = min(effective_scale_factor, 1.2)  # Up to 1.2x per tick (was 1.3x)
        elif workflow_name in COMPLEX_WORKFLOWS:
            # Complex workflows: CONSERVATIVE per-tick scaling to prevent cost explosion
            # Rely on retry service for recovery, not explosive scaling
            if success_rate < target_success * 0.70:  # Well below target
                capped_scale_factor = min(effective_scale_factor, 1.4)  # Up to +40% per tick (was 80%)
            elif success_rate < target_success:
                capped_scale_factor = min(effective_scale_factor, 1.3)  # Up to +30% per tick (was 60%)
            else:
                capped_scale_factor = min(effective_scale_factor, 1.2)  # +20% when at target (was 40%)
        else:
            capped_scale_factor = min(effective_scale_factor, 1.25)  # cap to +25% per tick for simple workflows
        
        # Apply CPSP throttle if task has poor cost efficiency
        capped_scale_factor = capped_scale_factor * cpsp_throttle
        
        # HARD CAP: Calculate early for machine-aligned scaling
        hard_cap = get_hard_scale_cap(task_id, base_minscale, workflow_name)
        
        # MACHINE-ALIGNED SCALING: Scale in machine increments to avoid partial machine waste
        if MACHINE_ALIGNED_SCALING:
            # Use machine-aligned scaling with actual metadata for accurate machine counting
            old_method_scale = math.ceil(max(current_minscale, effective_base) * capped_scale_factor)
            new_scale = calculate_machine_aligned_scale(
                current_pods=current_minscale,
                scale_factor=capped_scale_factor,
                min_pods=base_minscale,
                max_pods=hard_cap,
                task_id=task_id,
                metadata=metadata
            )
            if new_scale != old_method_scale:
                # Use actual machine count from metadata for accurate logging
                curr_machines, new_machines, machines_added = estimate_machines_after_scaling(
                    task_id, current_minscale, new_scale, metadata
                )
                print(f"   🖥️ MACHINE-ALIGNED: {task_id} {current_minscale}→{new_scale} pods "
                      f"({curr_machines}→{new_machines} machines [+{machines_added}], factor={capped_scale_factor:.2f})")
        else:
            new_scale = math.ceil(max(current_minscale, effective_base) * capped_scale_factor)
        
        # RAY-INSPIRED 4: Speculative replication
        # Add extra replicas for tasks likely to fail, similar to Ray's speculative execution
        # NOTE: When machine-aligned, speculative replicas are added to reach next machine boundary
        bottleneck_ratio = bottleneck_ratios.get(task_id, 1.0) if bottleneck_ratios else 1.0
        speculative_replicas = calculate_speculative_replicas(
            task_id, new_scale, task_drop_off, bottleneck_ratio, workflow_name, success_rate
        )
        if speculative_replicas > 0:
            if MACHINE_ALIGNED_SCALING:
                # Align speculative replicas to machine boundary
                new_scale_with_spec = align_to_machine_boundary(new_scale + speculative_replicas, round_up=True)
                if new_scale_with_spec > new_scale and new_scale_with_spec <= hard_cap:
                    print(f"   🔮 SPECULATIVE (aligned): {task_id} adding {new_scale_with_spec - new_scale} pods to reach {pods_to_machines(new_scale_with_spec)} machines")
                    new_scale = new_scale_with_spec
            else:
                new_scale += speculative_replicas
        
        # HARD CAP check (already calculated above)
        if new_scale > hard_cap:
            print(f"   🛑 HARD CAP: {task_id} scale {new_scale} → {hard_cap} (max {get_max_scale_multiple(workflow_name)}x base={base_minscale})")
            new_scale = hard_cap
        
        # Apply maximum scale cap if specified (cost optimization)
        # For complex workflows with low success rates, dynamically increase max_scale
        # This helps achieve target success rates when workflows are struggling
        # When success rate is good and cost is high, reduce max_scale to save costs
        if max_scale is not None:
            effective_max_scale = max_scale
            target_success = get_target_success_rate(workflow_name)
            
            # Cost-aware max_scale reduction when success rate is above target
            if success_rate >= target_success and real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0:
                normalized_cost = real_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                if normalized_cost > 2.0:  # Very expensive pods (>2x baseline)
                    effective_max_scale = int(max_scale * 0.7)  # Reduce max_scale by 30%
                    print(f"   💰 Cost-aware max_scale reduction: {task_id} max_scale {max_scale} → {effective_max_scale} (success={success_rate*100:.1f}%≥{target_success*100:.1f}%, cost=${real_cost_per_pod_per_sec*3600:.2f}/pod/hour)")
                elif normalized_cost > 1.5:  # Moderately expensive pods (>1.5x baseline)
                    effective_max_scale = int(max_scale * 0.85)  # Reduce max_scale by 15%
                    print(f"   💰 Cost-aware max_scale reduction: {task_id} max_scale {max_scale} → {effective_max_scale} (success={success_rate*100:.1f}%≥{target_success*100:.1f}%, cost=${real_cost_per_pod_per_sec*3600:.2f}/pod/hour)")
                elif normalized_cost > 1.0:  # Standard expensive pods (>1x baseline)
                    effective_max_scale = int(max_scale * 0.95)  # Reduce max_scale by 5%
            
            # Dynamic max_scale adjustment - CONSERVATIVE approach to prevent runaway costs
            # Only allow modest increases, never exceed hard caps
            # If scaling hasn't been helping (tracked by efficiency tracker), don't increase max_scale
            if not scaling_efficiency_tracker['scaling_paused']:
                success_gap = target_success - success_rate
                
                if success_rate < 0.20:  # Critical: < 20% success rate
                    # Allow modest increase: up to 1.5x normal limit
                    effective_max_scale = min(int(max_scale * 1.5), hard_cap)
                    if new_scale > max_scale:
                        print(f"   ⚠️ Critical scaling: allowing {new_scale} (capped at {effective_max_scale}, success rate {success_rate*100:.1f}% < 20%)")
                elif success_rate < 0.40:  # Low: 20-40% success rate
                    # Allow small increase: up to 1.3x normal limit
                    effective_max_scale = min(int(max_scale * 1.3), hard_cap)
                    if new_scale > max_scale:
                        print(f"   ⚠️ Low success scaling: allowing {new_scale} (capped at {effective_max_scale}, success rate {success_rate*100:.1f}% < 40%)")
                elif success_gap > 0.15:  # Success rate gap > 15% below target
                    # Allow minimal increase: up to 1.2x normal limit
                    effective_max_scale = min(int(max_scale * 1.2), hard_cap)
                    if new_scale > max_scale:
                        print(f"   ℹ️ Below target scaling: allowing {new_scale} (capped at {effective_max_scale}, success rate {success_rate*100:.1f}% vs target {target_success*100:.1f}%)")
            else:
                # Scaling efficiency paused - use normal max_scale, no increases
                print(f"   ⏸️ Max_scale increase blocked due to scaling efficiency pause")
            
            new_scale = min(new_scale, effective_max_scale)

        # Pareto-based cost control: if we're slightly above target but cost is high,
        # skip scaling and let retry service lead. Log estimated savings.
        pareto_trigger, pareto_reason, normalized_cost, success_margin = evaluate_pareto_guard(
            success_rate, target_success, real_cost_per_pod_per_sec
        )
        if pareto_trigger:
            pods_avoided = max(0, new_scale - current_minscale)
            print(f"   🧮 Pareto guard: skip scaling {task_id} "
                  f"(success={success_rate*100:.1f}%>={target_success*100:.1f}%+{PARETO_SUCCESS_MARGIN*100:.1f}%, "
                  f"norm_cost={normalized_cost:.2f}≥{PARETO_COST_NORM_THRESHOLD:.2f})")
            record_pareto_savings(task_id, pods_avoided, real_cost_per_pod_per_sec, "critical")
            if pods_avoided <= 0:
                print(f"   ℹ️ Pareto guard: no additional pods to avoid for {task_id}")
            continue
        elif normalized_cost is not None and normalized_cost >= PARETO_COST_NORM_THRESHOLD:
            print(f"   🧮 Pareto guard not triggered for {task_id} ({pareto_reason}, "
                  f"success_margin={success_margin:.3f}, norm_cost={normalized_cost:.2f})")
        
        # Cost-benefit analysis: only scale if cost-effective
        # Dynamic threshold based on success rate and workflow complexity
        target_success = get_target_success_rate(workflow_name)
        
        # Cost-benefit thresholds based on workflow type and success rate
        # When success rate is above target and cost is high, increase threshold to reduce scaling
        target_success = get_target_success_rate(workflow_name)
        cost_penalty_multiplier = 1.0
        if success_rate >= target_success and real_cost_per_pod_per_sec and real_cost_per_pod_per_sec > 0:
            normalized_cost = real_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
            if normalized_cost > 2.0:  # Very expensive pods (>2x baseline)
                cost_penalty_multiplier = 3.0  # 3x higher threshold (less scaling)
            elif normalized_cost > 1.5:  # Moderately expensive pods (>1.5x baseline)
                cost_penalty_multiplier = 2.0  # 2x higher threshold (less scaling)
            elif normalized_cost > 1.0:  # Standard expensive pods (>1x baseline)
                cost_penalty_multiplier = 1.5  # 1.5x higher threshold (less scaling)
        
        # Cost-benefit thresholds - Lower thresholds ONLY when below target
        # CRITICAL: When at/above target, be STRICT about cost to prevent over-scaling
        if success_rate < 0.20:  # Emergency: very low success rate
            # Emergency threshold: very low to allow aggressive scaling
            effective_threshold = max(min_cost_benefit_ratio * 0.1, MIN_COST_BENEFIT_FLOOR)  # 10% of normal
        elif workflow_name in COMPLEX_WORKFLOWS:
            # Complex workflows (wf-5,6,8,9): target 65-70% (67.5% midpoint)
            if success_rate < target_success * 0.50:  # Below 50% of target (33.75%) - critical
                effective_threshold = max(min_cost_benefit_ratio * 0.08, MIN_COST_BENEFIT_FLOOR)  # 8% of normal
            elif success_rate < target_success * 0.70:  # Below 70% of target (47.25%)
                effective_threshold = max(min_cost_benefit_ratio * 0.15, MIN_COST_BENEFIT_FLOOR)  # 15% of normal
            elif success_rate < target_success * 0.90:  # Below 90% of target (60.75%)
                effective_threshold = max(min_cost_benefit_ratio * 0.25, MIN_COST_BENEFIT_FLOOR)  # 25% of normal
            elif success_rate < target_success:  # Below target but close
                effective_threshold = max(min_cost_benefit_ratio * 0.40, MIN_COST_BENEFIT_FLOOR)  # 40% of normal
            else:
                # AT/ABOVE TARGET: Be strict about cost - require strong justification
                effective_threshold = min_cost_benefit_ratio * 2.0  # 2x HIGHER threshold (less scaling)
        else:  # Simple workflows
            # Simple workflows: target 95-100% (97.5% midpoint)
            if success_rate < target_success * 0.70:  # Below 70% of target (68.25%)
                effective_threshold = max(min_cost_benefit_ratio * 0.20, MIN_COST_BENEFIT_FLOOR)  # 20% of normal
            elif success_rate < target_success * 0.85:  # Below 85% of target (82.9%)
                effective_threshold = max(min_cost_benefit_ratio * 0.35, MIN_COST_BENEFIT_FLOOR)  # 35% of normal
            elif success_rate < target_success:  # Below target but close
                effective_threshold = max(min_cost_benefit_ratio * 0.50, MIN_COST_BENEFIT_FLOOR)  # 50% of normal
            else:
                # AT/ABOVE TARGET: Be strict about cost
                effective_threshold = min_cost_benefit_ratio * 1.5  # 1.5x HIGHER threshold (less scaling)
        
        # Apply cost penalty multiplier when success rate is above target and cost is high
        effective_threshold *= cost_penalty_multiplier
        if cost_penalty_multiplier > 1.0:
            print(f"   💰 Cost penalty applied: threshold ×{cost_penalty_multiplier:.1f} (success={success_rate*100:.1f}%≥{target_success*100:.1f}%, cost=${real_cost_per_pod_per_sec*3600:.4f}/pod/hour)")

        # Apply cost-win threshold tightening when marginal gains are detected
        if cost_win_threshold_boost > 1.0:
            effective_threshold *= cost_win_threshold_boost
        
        if task_depths is not None:
            position_multiplier = get_position_threshold_multiplier(task_id, task_depths, max_depth)
            if position_multiplier != 1.0:
                effective_threshold *= position_multiplier
                print(f"   📍 Position weighting for {task_id}: depth multiplier ×{position_multiplier:.2f}")
        
        if task_failure_rates:
            failure_rate = task_failure_rates.get(task_id)
            if failure_rate and failure_rate > 0:
                failure_multiplier = get_failure_threshold_multiplier(failure_rate)
                effective_threshold *= failure_multiplier
                print(f"   ⚠️ Failure pressure on {task_id}: drop-off={failure_rate*100:.1f}%, threshold ×{failure_multiplier:.2f}")

        if bottleneck_ratios:
            bottleneck_ratio = bottleneck_ratios.get(task_id, 1.0)
            # ONLY apply bottleneck threshold reduction when BELOW target
            # At/above target: bottleneck scaling is wasteful
            if bottleneck_ratio > 1.0 and success_rate < target_success:
                # More aggressive loosening for bottleneck tasks WHEN BELOW TARGET
                # For 2x bottleneck: 0.5x threshold (50% reduction)
                # For 3x bottleneck: 0.33x threshold (67% reduction)
                # For 4x bottleneck: 0.25x threshold (75% reduction)
                # Cap at 0.2x (80% reduction) for extreme bottlenecks
                loosen_multiplier = max(0.2, 1.0 / bottleneck_ratio)
                effective_threshold *= loosen_multiplier
                print(f"   🚦 Bottleneck relief for {task_id}: ratio={bottleneck_ratio:.1f}×, threshold ×{loosen_multiplier:.2f}")
        
        cost_benefit = calculate_cost_benefit_ratio(
            current_minscale, new_scale, base_minscale, success_rate, exec_time, 
            workflow_name=workflow_name,
            real_cost_per_pod_per_sec=real_cost_per_pod_per_sec
        )
        
        if new_scale > current_minscale:
            target_success = get_target_success_rate(workflow_name)
            
            # =========================================================================
            # COST-BENEFIT THRESHOLD CALCULATION
            # =========================================================================
            # Even when success is low, require SOME positive cost-benefit
            # This prevents unbounded scaling that exhausts budget early
            
            effective_threshold_for_check = effective_threshold
            
            # CHECK BUDGET PACING FIRST - if blocked, skip this scaling entirely
            pacing_throttle, pace_ratio = get_budget_pacing_throttle()
            if pacing_throttle <= 0.0:
                print(f"   🛑 BUDGET GATE: Skipping {task_id} scaling due to budget pacing (pace={pace_ratio:.1f}x)")
                continue  # Skip this task
            
            if success_rate < target_success:
                # Below target: use relaxed threshold but NOT zero
                if success_rate < target_success * 0.3:
                    # Very low (<30% of target): use 5% of threshold 
                    effective_threshold_for_check = max(effective_threshold * 0.05, MIN_COST_BENEFIT_FLOOR * 10)
                elif success_rate < target_success * 0.5:
                    # Low (<50% of target): use 8% of threshold
                    effective_threshold_for_check = max(effective_threshold * 0.08, MIN_COST_BENEFIT_FLOOR * 5)
                elif success_rate < target_success * 0.7:
                    # Medium (<70% of target): use 12% of threshold
                    effective_threshold_for_check = max(effective_threshold * 0.12, MIN_COST_BENEFIT_FLOOR * 2)
                else:
                    # Close to target: use 20% of threshold
                    effective_threshold_for_check = max(effective_threshold * 0.20, MIN_COST_BENEFIT_FLOOR)
            
            if cost_benefit >= effective_threshold_for_check:
                scale_function_spec(task_id, new_scale)
                last_scaled[task_id] = current_time
                workflow_marker = "🔥" if workflow_name in COMPLEX_WORKFLOWS else "🚨"
                if effective_threshold_for_check < effective_threshold:
                    print(f"{workflow_marker} Critical task {task_id} scaled up: {current_minscale} → {new_scale} "
                          f"(max={max_scale or 'unlimited'}, cost-benefit={cost_benefit:.4f}, relaxed threshold={effective_threshold_for_check:.4f}, success={success_rate*100:.1f}%)")
                else:
                    print(f"{workflow_marker} Critical task {task_id} scaled up: {current_minscale} → {new_scale} "
                          f"(max={max_scale or 'unlimited'}, cost-benefit={cost_benefit:.4f}, threshold={effective_threshold:.4f})")
            else:
                print(f"💰 Skipping {task_id} scaling: cost-benefit ratio {cost_benefit:.4f} < threshold {effective_threshold_for_check:.4f}")
                
                # Mild scale down if cost-benefit is too low and success rate is good
                # Only scale down if we're above target and cost-benefit is very low
                low_cost_benefit_threshold = effective_threshold * 0.3  # 30% of threshold = very low
                if (success_rate >= target_success and 
                    cost_benefit < low_cost_benefit_threshold and 
                    current_minscale > base_minscale):
                    # Mild scale down: reduce by 5-10% depending on how low the cost-benefit is
                    scale_down_factor = 0.90 if cost_benefit < low_cost_benefit_threshold * 0.5 else 0.95
                    raw_new_scale_down = max(base_minscale, int(current_minscale * scale_down_factor))
                    
                    # MACHINE-ALIGNED SCALE-DOWN
                    if MACHINE_ALIGNED_SCALING:
                        new_scale_down = align_to_machine_boundary(raw_new_scale_down, round_up=False)
                        new_scale_down = max(base_minscale, new_scale_down)
                    else:
                        new_scale_down = raw_new_scale_down
                    
                    if new_scale_down < current_minscale:
                        # Scale-downs don't trigger global cooldown
                        scale_function_spec(task_id, new_scale_down, record_global_cooldown=False)
                        last_scaled[task_id] = current_time
                        print(f"   🖥️ Mild scale down (machine-aligned) for {task_id}: {current_minscale} → {new_scale_down} "
                              f"(cost-benefit={cost_benefit:.4f} too low, success={success_rate*100:.1f}%≥{target_success*100:.1f}%)")
    
    return last_scaled

# ============================================================================
# RL ADAPTIVE SCALER - CONTEXTUAL BANDIT BASED SCALING
# ============================================================================

# Track RL state across ticks
_rl_last_action_time = None
_rl_last_success_rate = None
_rl_last_cost = None
_rl_pending_reward = None  # (action, success_before, cost_before, time)

def rl_adaptive_scaling_tick(
    workflow_name: str,
    baseline: str,
    all_function_specs: dict,
    metadata: dict,
    success_rate: float,
    total_runs: int,
    critical_tasks: list,
    bottleneck_ratios: dict,
    task_failure_rates: dict,
    budget_tracker: dict,
    cached_cost_per_pod_per_sec: float,
    max_scale: int = None,
    scale_cooldown: int = 180,
    last_scaled: dict = None,
    critical_path_metrics: dict = None,
    task_depths: dict = None,
    max_depth: int = 0,
):
    """
    RL-based adaptive scaling decision using contextual bandit.
    
    This function replaces the hardcoded scaling logic with a learned policy
    that selects from: idle / conservative / moderate / aggressive scaling.
    
    The RL agent:
    1. Extracts context features from current system state
    2. Selects an action based on Thompson Sampling
    3. Executes scaling based on the action's scale factor
    4. Observes reward after a delay and updates the model
    
    Args:
        workflow_name: Name of the workflow
        baseline: Baseline name
        all_function_specs: Current function specifications
        metadata: Task metadata
        success_rate: Current workflow success rate
        total_runs: Total workflow runs
        critical_tasks: List of critical task IDs
        bottleneck_ratios: Task bottleneck ratios
        task_failure_rates: Task failure rates
        budget_tracker: Budget envelope tracker dict
        cached_cost_per_pod_per_sec: Current cost rate
        max_scale: Maximum scale limit
        scale_cooldown: Cooldown between scaling actions
        last_scaled: Dict tracking last scale time per task
        critical_path_metrics: Critical path info
        task_depths: Task depth in DAG
        max_depth: Maximum DAG depth
        
    Returns:
        Updated last_scaled dict
    """
    global _rl_last_action_time, _rl_last_success_rate, _rl_last_cost, _rl_pending_reward
    
    if not RL_AVAILABLE:
        print("⚠️ RL mode requested but modules not available")
        return last_scaled or {}
    
    if last_scaled is None:
        last_scaled = {}
    
    current_time = time.time()
    bandit = get_bandit()
    state_tracker = get_state_tracker()
    
    # Calculate current total pods
    current_total_pods = sum(spec.get('minscale', 1) for spec in all_function_specs.values())
    max_total_pods = sum(metadata.get(tid, {}).get('minscale', 1) * 3 for tid in metadata)
    
    # Get budget info
    budget_spent = budget_tracker.get('spent', 0.0)
    budget_total = budget_tracker.get('budget', 1.0)
    budget_remaining_fraction = max(0, (budget_total - budget_spent) / budget_total) if budget_total > 0 else 0
    
    # Calculate elapsed time
    start_time = budget_tracker.get('start_time', current_time)
    elapsed_seconds = current_time - start_time
    runtime_hours = budget_tracker.get('estimated_runtime_hours', 0.167)
    expected_runtime_seconds = runtime_hours * 3600
    
    # Get retry exhaustion info
    is_retry_exhausted, retry_exhausted_count = check_retry_service_exhausted(workflow_name, baseline)
    
    # =========================================================================
    # STEP 1: Process pending reward from previous action
    # =========================================================================
    if _rl_pending_reward is not None:
        action, success_before, cost_before, action_time = _rl_pending_reward
        
        # Check if enough time has passed to measure reward
        if current_time - action_time >= TIMING_CONFIG['observation_window']:
            # Calculate cost incurred
            current_cost_rate = (cached_cost_per_pod_per_sec * 3600 * current_total_pods) if cached_cost_per_pod_per_sec else 0
            cost_incurred = (current_cost_rate - cost_before) * (current_time - action_time) / 3600
            
            # Calculate and record reward
            reward = calculate_reward(
                success_before=success_before,
                success_after=success_rate,
                cost_incurred=max(0, cost_incurred),
                budget_exhausted=budget_tracker.get('budget_exhausted', False)
            )
            
            # Get the context that was used for the action (approximate with current)
            context = get_context_vector(
                workflow_name=workflow_name,
                baseline=baseline,
                success_rate=success_rate,
                retry_exhausted_count=retry_exhausted_count,
                retry_exhausted_threshold=MIN_RETRY_EXHAUSTED_UIDS,
                budget_spent=budget_spent,
                budget_total=budget_total,
                elapsed_seconds=elapsed_seconds,
                expected_runtime_seconds=expected_runtime_seconds,
                total_pods=current_total_pods,
                max_pods=max_total_pods,
            )
            
            bandit.update(context, action, reward)
            print(f"🎰 RL REWARD: action={ACTIONS[action]['name']}, reward={reward:.4f} "
                  f"(success {success_before:.2%} -> {success_rate:.2%})")
            
            _rl_pending_reward = None
    
    # =========================================================================
    # STEP 2: Check if we should make a new decision
    # =========================================================================
    # Respect decision interval
    if _rl_last_action_time is not None:
        time_since_last = current_time - _rl_last_action_time
        if time_since_last < TIMING_CONFIG['decision_interval']:
            return last_scaled
    
    # =========================================================================
    # STEP 3: Extract context and select action
    # =========================================================================
    context = get_context_vector(
        workflow_name=workflow_name,
        baseline=baseline,
        success_rate=success_rate,
        retry_exhausted_count=retry_exhausted_count,
        retry_exhausted_threshold=MIN_RETRY_EXHAUSTED_UIDS,
        budget_spent=budget_spent,
        budget_total=budget_total,
        elapsed_seconds=elapsed_seconds,
        expected_runtime_seconds=expected_runtime_seconds,
        total_pods=current_total_pods,
        max_pods=max_total_pods,
    )
    
    # Check cold start phase (workflow-specific: simple workflows have longer cold start)
    early_scale_guard = (not is_retry_exhausted) and (
        RETRY_EXHAUST_DISABLE_ALL or workflow_name in RETRY_EXHAUST_DISABLE_WORKFLOWS
    )
    if not is_cold_start_complete(success_rate):
        # During cold start, use warmup actions (workflow-specific)
        warmup_actions = get_allowed_warmup_actions(workflow_name)
        if early_scale_guard:
            warmup_actions = [a for a in warmup_actions if a in (0, 1)] or [0]
        action = bandit.select_warmup_action(context, warmup_actions)
        print(f"❄️ RL COLD START ({workflow_name}): Selected warmup action {ACTIONS[action]['name']}")
    else:
        # Get budget-constrained allowed actions
        allowed_actions = rl_get_allowed_actions(budget_remaining_fraction)
        if early_scale_guard:
            allowed_actions = [a for a in allowed_actions if a in (0, 1)] or [0]
            print(f"🧯 Early-scale guard: restricting actions to {[ACTIONS[a]['name'] for a in allowed_actions]}")
        action = bandit.select_action(context, allowed_actions)
        print(f"🎰 RL ACTIVE: Selected action {ACTIONS[action]['name']} "
              f"(budget remaining: {budget_remaining_fraction:.1%}, allowed: {[ACTIONS[a]['name'] for a in allowed_actions]})")

    # Hard cost guardrail: if we're at target success and pods are expensive, force idle.
    target_success_rate = get_target_success_rate(workflow_name)
    normalized_cost = (
        (cached_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR)
        if cached_cost_per_pod_per_sec and cached_cost_per_pod_per_sec > 0
        else 1.0
    )
    if action != 0 and success_rate >= target_success_rate and normalized_cost > RL_HARD_IDLE_NORM_COST_THRESHOLD:
        print(f"🛑 RL HARD-IDLE OVERRIDE: forcing idle for {workflow_name} "
              f"(success={success_rate*100:.1f}%>=target {target_success_rate*100:.1f}%, norm_cost={normalized_cost:.2f})")
        action = 0
    
    # =========================================================================
    # STEP 4: Execute the selected action
    # =========================================================================
    action_info = ACTIONS[action]
    scale_factor = action_info['scale_factor']
    
    if action == 0:
        # Idle: do nothing, let cp+retry handle it
        print(f"🎯 RL ACTION: IDLE - letting cp+retry handle recovery")
    else:
        # Scale critical tasks by the selected factor
        print(f"🎯 RL ACTION: {action_info['name'].upper()} - scaling critical tasks by {scale_factor}x")
        
        target_success_rate = get_target_success_rate(workflow_name)
        
        min_runs_for_skip = SKIP_SCALE_UP_MIN_RUNS.get(workflow_name, 0)
        allow_skip = total_runs is not None and total_runs >= min_runs_for_skip

        # Only skip scale-up if success is high AND enough real runs have been observed
        if success_rate < target_success_rate * 0.95 or not allow_skip:
            if not allow_skip and min_runs_for_skip > 0:
                print(f"   ⏳ Deferring scale-up skip: runs={total_runs}, min_runs={min_runs_for_skip}")
            last_scaled = scale_critical_tasks(
                critical_tasks=critical_tasks,
                all_function_specs=all_function_specs,
                metadata=metadata,
                scale_factor=scale_factor,
                max_scale=max_scale,
                last_scaled=last_scaled,
                scale_cooldown=scale_cooldown,
                success_rate=success_rate,
                min_cost_benefit_ratio=0.005,  # Lower threshold for RL mode
                workflow_name=workflow_name,
                task_depths=task_depths,
                max_depth=max_depth,
                task_failure_rates=task_failure_rates,
                bottleneck_ratios=bottleneck_ratios,
                real_cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                critical_path_metrics=critical_path_metrics,
            )
        else:
            print(f"   ✅ Success {success_rate:.1%} >= target {target_success_rate*0.95:.1%}, skipping scale-up")
    
    # =========================================================================
    # STEP 5: Record for future reward calculation
    # =========================================================================
    cost_rate = (cached_cost_per_pod_per_sec * 3600 * current_total_pods) if cached_cost_per_pod_per_sec else 0
    _rl_last_action_time = current_time
    _rl_last_success_rate = success_rate
    _rl_last_cost = cost_rate
    _rl_pending_reward = (action, success_rate, cost_rate, current_time)
    
    return last_scaled


def rl_save_state_on_exit():
    """Save RL bandit state when experiment ends."""
    if RL_AVAILABLE:
        save_bandit_state()
        print("💾 RL bandit state saved")


def scale_function_spec(func_name, new_minscale, record_global_cooldown=True):
    """
    Scale a function's minscale/maxscale.
    
    Args:
        func_name: Name of the function to scale
        new_minscale: New minimum scale value
        record_global_cooldown: If True, trigger global scaler cooldown after scaling
    """
    try:
        new_maxscale = max(new_minscale + 1, new_minscale * 2)
        subprocess.run(
            ["fission", "fn", "update", "--name", func_name,
             "--minscale", str(new_minscale), "--maxscale", str(new_maxscale)],
            check=True, capture_output=True, text=True
        )
        print(f"📈 Updated {func_name}: minscale={new_minscale}, maxscale={new_maxscale}")
        
        # Record global scaling event to trigger cooldown
        if record_global_cooldown:
            record_global_scaling_event()
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Fission update failed for {func_name}: {e.stderr}")
    except Exception as e:
        print(f"❌ Error scaling {func_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Kaplan-Meier based preemptive scaler.")
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--workflows-dir", required=True)
    parser.add_argument("--survival-curve", required=True)
    parser.add_argument("--scale-up-threshold", type=float, default=0.5)
    parser.add_argument("--scale-down-threshold", type=float, default=0.8)
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--success-rate-threshold", type=float, default=0.4, 
                       help="Success rate threshold below which to scale up critical tasks")
    parser.add_argument("--success-rate-interval", type=int, default=60,
                       help="Interval in seconds for success rate monitoring")
    parser.add_argument("--critical-tasks-count", type=int, default=3,
                       help="Number of top HEFT-ranked tasks to consider critical")
    parser.add_argument("--critical-scale-factor", type=float, default=2.0,
                       help="Scaling factor for critical tasks when success rate is low")
    parser.add_argument("--max-scale", type=int, default=None,
                       help="Maximum absolute scale limit for cost control (default: unlimited)")
    parser.add_argument("--scale-cooldown", type=int, default=180,
                       help="Minimum seconds between scaling actions for same task (default: 120)")
    parser.add_argument("--kaplan-meier-cost-benefit-threshold", type=float, default=0.005,
                       help="Minimum cost-benefit ratio for Kaplan-Meier scaling (default: 0.005)")
    parser.add_argument("--baseline", help="Baseline filter for success rate monitoring")
    parser.add_argument("--az", default="us-west-2a",
                       help="Availability zone for spot cost lookup (default: us-west-2a)")
    parser.add_argument("--instance", default="v100",
                       help="Instance type for spot cost lookup (default: v100)")
    parser.add_argument("--rl-mode", action="store_true",
                       help="Enable RL adaptive scaler (contextual bandit for action selection)")
    parser.add_argument("--rl-cold-start-minutes", type=int, default=5,
                       help="Minutes to observe before RL agent activates (default: 5)")
    parser.add_argument("--rl-activation-threshold", type=float, default=0.6,
                       help="Success rate threshold below which RL agent activates (default: 0.6)")
    args = parser.parse_args()

    with open(args.survival_curve) as f:
        survival_curve = json.load(f)

    config.load_kube_config()
    core_api = client.CoreV1Api()

    metadata_path = os.path.join(args.workflows_dir, args.workflow, "metadata.json")
    if not os.path.exists(metadata_path):
        print(f"❌ Metadata file not found: {metadata_path}")
        return
    with open(metadata_path) as f:
        metadata = json.load(f)
    
    # Reset scaling efficiency tracker at start of each experiment (including budget envelope + CPSP)
    # Load spot prices from CSV to calculate on-demand costs for budget
    reset_scaling_efficiency_tracker(workflow_name=args.workflow, az=args.az, instance=args.instance)
    workflow_max_pods = get_max_total_pods(args.workflow)
    workflow_max_scale = get_max_scale_multiple(args.workflow)
    print(f"🔄 Scaling efficiency tracker reset (max {workflow_max_scale}x scale, {workflow_max_pods} max pods for {args.workflow})")
    
    # RL Mode setup
    if args.rl_mode:
        if RL_AVAILABLE:
            from rl_config import COLD_START_CONFIG, get_cold_start_config, SIMPLE_WORKFLOWS as RL_SIMPLE_WFS
            wf_cold_start = get_cold_start_config(args.workflow)
            print(f"🎰 RL ADAPTIVE SCALER ENABLED")
            print(f"   Workflow: {args.workflow} ({'simple' if args.workflow in RL_SIMPLE_WFS else 'complex'})")
            print(f"   Cold start: {wf_cold_start['min_observation_minutes']} min, "
                  f"threshold: {wf_cold_start['activation_threshold']:.0%}, "
                  f"warmup actions: {wf_cold_start['warmup_actions']}")
            print(f"   Actions: idle (1.0x), conservative (1.1x), moderate (1.3x), aggressive (1.5x)")
        else:
            print(f"⚠️ RL mode requested but modules not available. Falling back to traditional scaler.")

    tasks_path = os.path.join(args.workflows_dir, args.workflow, "tasks.json")
    workflow_tasks = {}
    task_depths = {}
    max_task_depth = 0
    task_predecessors = {}
    if not os.path.exists(tasks_path):
        print(f"⚠️ tasks.json not found for {args.workflow} at {tasks_path}; position/failure-aware tuning disabled")
    else:
        with open(tasks_path) as f:
            tasks_payload = json.load(f)
        workflow_tasks = tasks_payload.get("tasks", {})
        task_depths, max_task_depth, task_predecessors = compute_task_depths(workflow_tasks)
        print(f"📐 Computed task depths (max depth={max_task_depth}) for position-aware thresholds")

    # Compute critical path metrics (from TCC-CloudWorkflow2014 paper)
    # This identifies tasks on the critical path that need aggressive scaling
    critical_path_metrics = {}
    if workflow_tasks:
        # Estimate deadline based on workflow type (complex workflows have longer deadlines)
        if args.workflow in COMPLEX_WORKFLOWS:
            estimated_deadline = 600  # 10 minutes for complex workflows
        else:
            estimated_deadline = 300  # 5 minutes for simple workflows
        critical_path_metrics = compute_critical_path_metrics(workflow_tasks, estimated_deadline)

    machine_tasks = {}
    for tid, info in metadata.items():
        for machine_id in info.get("machine_ids", {}):
            machine_tasks.setdefault(machine_id, set()).add(tid)

    # Calculate bottleneck ratios once (for both critical task selection and Kaplan-Meier scaling)
    bottleneck_ratios = get_bottleneck_ratios(metadata, args.workflows_dir, args.workflow)
    if bottleneck_ratios:
        print(f"🔍 Bottleneck ratios detected: {len(bottleneck_ratios)} tasks with throughput bottlenecks")
        for task_id, ratio in sorted(bottleneck_ratios.items(), key=lambda x: x[1], reverse=True):
            print(f"   {task_id}: {ratio:.1f}x bottleneck")

    # Initialize task_failure_rates (will be updated during success rate checks)
    task_failure_rates = {}

    # Get critical tasks (considering HEFT ranks, throughput bottlenecks, and critical path)
    # Initially without failure rates (will be updated when we have failure data)
    # Cost will be added after first fetch
    critical_tasks = get_critical_tasks(metadata, args.critical_tasks_count, 
                                       workflows_dir=args.workflows_dir, 
                                       workflow_name=args.workflow,
                                       task_failure_rates=task_failure_rates,
                                       cost_per_pod_per_sec=None,
                                       critical_path_metrics=critical_path_metrics)
    print(f"🎯 Critical tasks (top {args.critical_tasks_count} by HEFT + bottlenecks + critical path): {critical_tasks}")
    
    # Track last success rate check time
    last_success_rate_check = 0
    last_success_rate = None
    
    # Track last scaling time per task (for cooldown enforcement)
    last_critical_scaled = {}
    last_cost_win_scaled = {}

    # Cached failure metrics for threshold tuning
    task_failure_rates = {}
    task_completion_counts = {}
    
    # Cache for real-time cost data
    cached_cost_per_pod_per_sec = None
    cached_cost_price_per_hour = None
    last_cost_fetch_time = time.time()  # Start from now, not 0
    cost_fetch_interval = 60  # Fetch cost data every 60 seconds
    
    if args.max_scale:
        print(f"💰 Cost control: Maximum scale limit set to {args.max_scale}")
    print(f"⏱️  Scaling cooldown: {args.scale_cooldown}s between scaling actions")

    while True:
        current_time = time.time()
        print(f"\n🚀 Preemptive scaler tick for '{args.workflow}' ...")
        
        # Display global scaler status
        scaler_status = get_global_scaler_status()
        if not scaler_status['enabled']:
            print(f"⏸️ SCALER WAITING: Retry service has exhausted {scaler_status['exhausted_uids']}/{MIN_RETRY_EXHAUSTED_UIDS} UIDs")
        elif scaler_status['in_cooldown']:
            remaining = int(GLOBAL_SCALER_COOLDOWN_SECONDS - (time.time() - global_scaler_cooldown_tracker['last_scaling_time']))
            print(f"⏸️ GLOBAL COOLDOWN: {remaining}s remaining (total events: {scaler_status['total_events']})")
        else:
            print(f"✅ SCALER ACTIVE: {scaler_status['total_events']} scaling events so far")
        
        # Reset speculative tracking to prevent duplicate speculation
        reset_speculative_tracking()

        all_function_specs = get_all_function_specs()
        if not all_function_specs:
            time.sleep(args.interval)
            continue
        
        # Fetch real-time cost data periodically
        if current_time - last_cost_fetch_time >= cost_fetch_interval:
            (cost_per_sec, price_per_hour, sample_count,
             total_cost_per_hour, avg_machines, avg_pods) = get_recent_cost_per_pod(
                args.workflow, baseline=args.baseline, lookback_seconds=300
            )
            if sample_count > 0:
                cached_cost_per_pod_per_sec = cost_per_sec
                cached_cost_price_per_hour = price_per_hour
                # cost_per_sec is per real second; multiply by 3600 to get per hour
                cost_per_hour = cost_per_sec * 3600
                if total_cost_per_hour > 0:
                    print(f"💰 Real-time cost: ${total_cost_per_hour:.4f}/hour total "
                          f"(avg {avg_machines:.1f} machines, {avg_pods:.0f} pods, {sample_count} samples)")
                else:
                    print(f"💰 Real-time cost: ${cost_per_hour:.4f}/pod/hour ({sample_count} samples)")
            else:
                # Use default cost estimate when no data available
                cost_per_hour = COST_BASELINE_PER_POD_HOUR
                total_cost_per_hour = 0.0
                print(f"💰 Using default cost estimate: ${cost_per_hour:.4f}/pod/hour (no cost_logs data)")
            
            # Update Budget Envelope tracker (whether real or estimated cost)
            time_since_last_cost = current_time - last_cost_fetch_time
            # Use actual total cost rate if available, otherwise estimate from pods
            if total_cost_per_hour > 0:
                total_cost_rate = total_cost_per_hour
            else:
                current_total_pods_for_budget = sum(spec.get('minscale', 1) for spec in all_function_specs.values()) if all_function_specs else 30
                total_cost_rate = cost_per_hour * current_total_pods_for_budget
            remaining_budget, budget_exhausted = update_budget_spent(total_cost_rate, time_since_last_cost)
            
            if budget_exhausted:
                _, should_block_budget = is_budget_exhausted(
                    current_success_rate=last_success_rate,
                    workflow_name=args.workflow,
                )
                if should_block_budget:
                    print(f"🛑 BUDGET ENVELOPE: Scaling disabled. Relying on retry service for recovery.")
                    # RESET PODS TO BASE: When budget exhausted, stop bleeding cost
                    # Also reset if success is at target - no need for extra capacity
                    target_sr = get_target_success_rate(args.workflow)
                    success_at_target = last_success_rate is not None and last_success_rate >= target_sr
                    should_reset = (not budget_envelope_tracker.get('pods_reset_on_exhaustion', False)) or success_at_target
                    if should_reset:
                        reset_count = reset_pods_to_base(all_function_specs, metadata, args.workflow, reason="budget_exhausted")
                        if reset_count > 0:
                            budget_envelope_tracker['pods_reset_on_exhaustion'] = True
                            if success_at_target:
                                print(f"🔄 Pods reset to base - success at target ({last_success_rate*100:.1f}% >= {target_sr*100:.1f}%), no extra capacity needed.")
                            else:
                                print(f"🔄 Pods reset to base to stop cost bleeding. Retry service now leads recovery.")
                else:
                    print("⚠️ BUDGET EMERGENCY BYPASS ACTIVE: allowing limited scaling below target")
            else:
                budget_status = get_budget_status()
                print(f"💵 Budget: ${budget_status['spent']:.2f}/${budget_status['budget']:.2f} (${budget_status['remaining']:.2f} remaining)")
            
            last_cost_fetch_time = current_time
        
        # Check success rate periodically
        if current_time - last_success_rate_check >= args.success_rate_interval:
            print(f"\n📊 Checking success rate for workflow '{args.workflow}'...")
            success_rate, total_runs, successful_runs = get_workflow_success_rate(args.workflow, args.workflows_dir, args.baseline)
            last_success_rate = success_rate
            print(f"📈 Success Rate: {success_rate*100:.2f}% ({successful_runs}/{total_runs})")

            # Adaptive budget headroom for wf-5/wf-9 when success lags
            apply_adaptive_budget_envelope(args.workflow, success_rate, total_runs)
            
            # Update scaling efficiency tracker to detect runaway scaling
            if total_runs > 5:  # Wait for enough samples
                update_scaling_efficiency(success_rate, 0, all_function_specs, workflow_name=args.workflow)
            
            # Update ROI tracker with current state
            current_total_pods = sum(spec.get('minscale', 1) for spec in all_function_specs.values()) if all_function_specs else 0
            cost_per_hour = (cached_cost_per_pod_per_sec * 3600) if cached_cost_per_pod_per_sec else 0.10
            if total_runs > 5:
                roi_value, _, roi_throttle = update_roi_tracker(
                    current_total_pods, success_rate, cost_per_hour * current_total_pods, args.workflow
                )
                roi_summary = get_roi_summary()
                print(f"📊 ROI: {roi_value:.2f} (throttle={roi_throttle*100:.0f}%, pods_added={roi_summary['total_pods_added']}, "
                      f"success_gained={roi_summary['total_success_gained_pct']:.1f}%, cost=${roi_summary['total_cost_spent']:.2f})")

            if workflow_tasks:
                task_failure_rates, task_completion_counts = fetch_task_failure_metrics(
                    args.workflow, workflow_tasks, task_predecessors, baseline=args.baseline
                )
                if task_failure_rates:
                    hottest = sorted(task_failure_rates.items(), key=lambda x: x[1], reverse=True)[:3]
                    for task_id, fr in hottest:
                        completion = task_completion_counts.get(task_id, 0)
                        print(f"   🔎 Task '{task_id}': completions={completion}, drop-off={fr*100:.1f}%")
            
                # Recalculate critical tasks with updated failure rates, cost, and critical path (important for wf-2)
                critical_tasks = get_critical_tasks(metadata, args.critical_tasks_count,
                                                   workflows_dir=args.workflows_dir, workflow_name=args.workflow,
                                                   task_failure_rates=task_failure_rates,
                                                   cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                                                   critical_path_metrics=critical_path_metrics)
            
            # Use workflow-specific target instead of generic threshold
            target_success_rate = get_target_success_rate(args.workflow)
            # Use 90% of target as threshold to trigger scaling proactively
            effective_threshold = target_success_rate * 0.90
            
            # =========================================================================
            # SCALING DECISION: RL MODE vs TRADITIONAL MODE
            # =========================================================================
            if args.rl_mode and RL_AVAILABLE:
                # RL Adaptive Scaler: Use contextual bandit for action selection
                print(f"🎰 RL MODE: Using contextual bandit for scaling decision")
                last_critical_scaled = rl_adaptive_scaling_tick(
                    workflow_name=args.workflow,
                    baseline=args.baseline,
                    all_function_specs=all_function_specs,
                    metadata=metadata,
                    success_rate=success_rate,
                    total_runs=total_runs,
                    critical_tasks=critical_tasks,
                    bottleneck_ratios=bottleneck_ratios,
                    task_failure_rates=task_failure_rates,
                    budget_tracker=budget_envelope_tracker,
                    cached_cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                    max_scale=args.max_scale,
                    scale_cooldown=args.scale_cooldown,
                    last_scaled=last_critical_scaled,
                    critical_path_metrics=critical_path_metrics,
                    task_depths=task_depths,
                    max_depth=max_task_depth,
                )
            else:
                # Traditional scaling logic
                if success_rate < effective_threshold and total_runs > 0:
                    print(f"⚠️ Success rate {success_rate*100:.2f}% below threshold {effective_threshold*100:.2f}% (target: {target_success_rate*100:.2f}%)")
                    print(f"🚨 Scaling up critical tasks: {critical_tasks}")
                    last_critical_scaled = scale_critical_tasks(
                        critical_tasks, all_function_specs, metadata, 
                        args.critical_scale_factor, 
                        max_scale=args.max_scale,
                        last_scaled=last_critical_scaled,
                        scale_cooldown=args.scale_cooldown,
                        success_rate=success_rate,
                        min_cost_benefit_ratio=0.01,  # Only scale if cost-benefit >= 0.01
                        workflow_name=args.workflow,  # Pass workflow name for complex workflow adjustments
                        task_depths=task_depths,
                        max_depth=max_task_depth,
                        task_failure_rates=task_failure_rates,
                        bottleneck_ratios=bottleneck_ratios,
                        real_cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                        critical_path_metrics=critical_path_metrics
                    )
                else:
                    print(f"✅ Success rate {success_rate*100:.2f}% is acceptable")
                    
                    # AGGRESSIVE COST RECLAIM: When success is at/above target, reset pods to base
                    # We don't need extra capacity when we're already meeting the target
                    if success_rate >= target_success_rate and total_runs > 10:
                        # Check if we have elevated pods (any task above base minscale)
                        elevated_pods = sum(
                            1 for task_id, func_spec in all_function_specs.items()
                            if task_id in metadata and func_spec.get('minscale', 1) > metadata[task_id].get('minscale', 1)
                        )
                        if elevated_pods > 0:
                            print(f"💰 COST RECLAIM: Success at target ({success_rate*100:.1f}% >= {target_success_rate*100:.1f}%), {elevated_pods} tasks above base")
                            reset_count = reset_pods_to_base(all_function_specs, metadata, args.workflow, reason="success_at_target")
                            if reset_count > 0:
                                print(f"🔄 Reset {reset_count} tasks to base - retry service can handle any dips")

            # COST-WIN: reclaim cost by scaling down low-risk, non-critical tasks when gains are marginal
            cost_win_cooldown = max(90, args.scale_cooldown)
            cost_win_scaled = apply_cost_win_scale_down(
                all_function_specs, metadata, args.workflow, success_rate,
                task_failure_rates=task_failure_rates,
                bottleneck_ratios=bottleneck_ratios,
                critical_tasks=critical_tasks,
                real_cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                last_scaled=last_cost_win_scaled,
                scale_cooldown=cost_win_cooldown
            )
            if cost_win_scaled > 0:
                print(f"💸 COST-WIN: scaled down {cost_win_scaled} low-risk tasks to reduce cost")

            # Extra tail-cost clipping for wf-8/wf-9 after target is already met.
            clamp_count = clamp_post_target_pods(
                all_function_specs=all_function_specs,
                metadata=metadata,
                workflow_name=args.workflow,
                success_rate=success_rate,
                target_success_rate=target_success_rate,
                total_runs=total_runs,
                real_cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                reason="tail_cost_clip",
            )
            if clamp_count > 0:
                print(f"💰 Tail-cost clip applied to {clamp_count} tasks")
            
            last_success_rate_check = current_time
            # Refresh specs in case scaling updates changed minscale/maxscale
            all_function_specs = get_all_function_specs()
            if not all_function_specs:
                time.sleep(args.interval)
                continue

        # Track tasks scaled in this KM tick to prevent duplicate scaling
        # (multiple machines may trigger scaling for the same task)
        tasks_scaled_this_km_tick = set()

        for machine_id in machine_tasks:
            try:
                pods = get_machine_pods(core_api, machine_tasks, machine_id)
                if not pods:
                    continue
            except Exception as e:
                # Additional safety net in case get_machine_pods raises an unexpected exception
                print(f"❌ Unexpected error processing {machine_id}: {str(e)}. Continuing with next machine")
                continue

            # Calculate pod ages and remove outliers for cost stabilization
            pod_ages = [get_pod_age(p) for p in pods]
            if len(pod_ages) > 2:
                # Remove outliers: filter out ages > 3 standard deviations from mean
                import statistics
                mean_age = statistics.mean(pod_ages)
                if len(pod_ages) > 1:
                    stdev_age = statistics.stdev(pod_ages)
                    # Filter outliers (ages > mean + 3*stdev or < mean - 3*stdev)
                    filtered_ages = [age for age in pod_ages 
                                   if abs(age - mean_age) <= 3 * stdev_age]
                    if filtered_ages:  # Only use filtered if we have data
                        pod_ages = filtered_ages
                        if len(pod_ages) < len([get_pod_age(p) for p in pods]):
                            print(f"   📊 Removed {len([get_pod_age(p) for p in pods]) - len(pod_ages)} outlier pod ages for cost stabilization")
            
            avg_age = sum(pod_ages) / max(1, len(pod_ages))
            expected_completion = int(avg_age + 15)
            survival_prob = lookup_survival(survival_curve, expected_completion)
            
            # Calculate pod lifetime risk (for short-lifetime scaling aggressiveness)
            # Lower survival_prob = shorter expected lifetime = higher risk
            pod_lifetime_risk = 1.0 - survival_prob  # 0.0 = long lifetime, 1.0 = very short lifetime
            
            print(f"🧠 {machine_id} | AvgAge={avg_age:.1f}s | S(>{expected_completion}s)={survival_prob:.2f} | LifetimeRisk={pod_lifetime_risk:.2f}")

            # PROACTIVE KM: Track survival history and detect trends
            update_survival_history(machine_id, survival_prob)
            is_declining, decline_magnitude = detect_survival_trend(machine_id)
            target_success_for_km = get_target_success_rate(args.workflow)
            km_tier, km_scale_factor = get_proactive_km_tier(
                survival_prob, is_declining, decline_magnitude,
                success_rate=last_success_rate,
                target_success=target_success_for_km
            )
            
            if is_declining and decline_magnitude > 0.05:
                print(f"   📉 TREND: Survival declining by {decline_magnitude*100:.1f}% → tier={km_tier}")

            # Burst scaling removed - retry service handles rapid recovery

            # PROACTIVE KM: Scale based on tier (proactive/warning/critical), not just single threshold
            if km_tier != 'safe':
                # BUDGET ENVELOPE CHECK: Soft throttle if budget exhausted (not hard block)
                km_success_rate = last_success_rate if last_success_rate is not None else 0.5
                budget_exhausted_km, should_block_km = is_budget_exhausted(
                    current_success_rate=km_success_rate,
                    workflow_name=args.workflow,
                )
                km_budget_throttle = 1.0
                if budget_exhausted_km:
                    if should_block_km:
                        print(f"🛑 BUDGET GATE (KM): Skip scaling for {machine_id} - budget exhausted")
                        continue
                    km_budget_throttle = min(km_budget_throttle, BUDGET_EMERGENCY_THROTTLE)
                    print(f"⚠️ BUDGET BYPASS (KM): Limited scaling on {machine_id} (throttle={km_budget_throttle*100:.0f}%)")
                
                # BUDGET PACING (KM): Throttle if spending ahead of schedule
                km_pacing_throttle, km_pace_ratio = get_budget_pacing_throttle()
                if km_pacing_throttle < 1.0:
                    km_budget_throttle *= km_pacing_throttle
                    # For proactive tier, skip entirely if throttle is strong
                    if km_tier == 'proactive' and km_pacing_throttle < 0.5:
                        print(f"⏱️ BUDGET PACING (KM): Skipping proactive scaling (throttle={km_pacing_throttle*100:.0f}%)")
                        continue
                
                tier_emoji = {'proactive': '🔮', 'warning': '⚠️', 'critical': '🚨'}[km_tier]
                tier_msg = {'proactive': 'Proactive scale', 'warning': 'Warning scale', 'critical': 'Critical scale'}[km_tier]
                print(f"{tier_emoji} {tier_msg} on {machine_id} (tier={km_tier}, factor={km_scale_factor:.2f}x)")
                for task_id in machine_tasks[machine_id]:
                    # Skip tasks already scaled in this KM tick (prevents duplicate scaling)
                    if task_id in tasks_scaled_this_km_tick:
                        continue
                    
                    func_spec = all_function_specs.get(task_id)
                    if not func_spec: continue
                    
                    # CPSP CHECK: Soft throttle for tasks with poor historical cost efficiency
                    should_scale_cpsp, cpsp_reason = should_scale_based_on_cpsp(task_id, current_success_rate=km_success_rate)
                    km_cpsp_throttle = 1.0
                    if not should_scale_cpsp:
                        km_cpsp_throttle = 0.5
                        print(f"   💸 CPSP SOFT (KM): Throttling {task_id} to 50% - {cpsp_reason}")
                    
                    # STRICT POD BUDGET (KM): When pod budget is exceeded, ALWAYS skip scaling
                    # No more critical-path bypass - this was causing cost blowup
                    if scaling_efficiency_tracker['scaling_paused'] and "Total pods" in (scaling_efficiency_tracker.get('pause_reason') or ""):
                        current_total_pods = sum(spec.get('minscale', 1) for spec in all_function_specs.values()) if all_function_specs else None
                        print(f"   🛑 POD BUDGET EXCEEDED (KM): Skipping {task_id} scale (total pods {current_total_pods}/{MAX_TOTAL_PODS})")
                        continue
                    current_minscale = func_spec['minscale']
                    base_minscale = metadata[task_id].get("minscale", 1)
                    exec_time = metadata[task_id].get("exec_time", 0.25)
                    
                    # Use effective scaling factor (HEFT + bottleneck + short-lifetime + cost-aware + critical path)
                    failure_rate = task_failure_rates.get(task_id) if 'task_failure_rates' in locals() and task_failure_rates else None
                    # PROACTIVE KM: Check cooldown for proactive tier (shorter cooldown for critical)
                    proactive_cooldown = 45 if km_tier == 'proactive' else (30 if km_tier == 'warning' else 20)
                    if km_tier == 'proactive' and not should_proactive_scale(task_id, proactive_cooldown):
                        continue  # Skip if recently scaled proactively
                    
                    effective_scaling_factor = get_effective_scaling_factor(
                        task_id, metadata[task_id], bottleneck_ratios,
                        pod_lifetime_risk=pod_lifetime_risk,
                        cost_per_pod_per_sec=cached_cost_per_pod_per_sec,
                        workflow_name=args.workflow,
                        workflow_success_rate=last_success_rate,
                        task_failure_rate=failure_rate,
                        critical_path_metrics=critical_path_metrics
                    )
                    
                    # PROACTIVE KM: Apply tier-based scale factor
                    if km_scale_factor > 1.0:
                        effective_scaling_factor = max(effective_scaling_factor, km_scale_factor)
                        if km_tier == 'proactive':
                            print(f"   🔮 PROACTIVE-KM: {task_id} preemptive scale ×{km_scale_factor:.2f}")
                    
                    # Apply budget and CPSP throttles
                    effective_scaling_factor = effective_scaling_factor * km_budget_throttle * km_cpsp_throttle
                    
                    if effective_scaling_factor > 1.0:
                        # Burst scaling removed - retry service handles rapid recovery
                        
                        # HARD CAP: Calculate early for machine-aligned scaling
                        hard_cap_km = get_hard_scale_cap(task_id, base_minscale, args.workflow)
                        
                        # MACHINE-ALIGNED SCALING for KM-based scaling
                        if MACHINE_ALIGNED_SCALING:
                            new_scale = calculate_machine_aligned_scale(
                                current_pods=current_minscale,
                                scale_factor=effective_scaling_factor,
                                min_pods=base_minscale,
                                max_pods=hard_cap_km,
                                task_id=task_id,
                                metadata=metadata
                            )
                            # Use actual machine count from metadata for accurate logging
                            curr_machines_km, new_machines_km, machines_added_km = estimate_machines_after_scaling(
                                task_id, current_minscale, new_scale, metadata
                            )
                            if new_scale != current_minscale:
                                print(f"   🖥️ MACHINE-ALIGNED (KM): {task_id} {current_minscale}→{new_scale} pods "
                                      f"({curr_machines_km}→{new_machines_km} machines [+{machines_added_km}])")
                        else:
                            new_scale = max(current_minscale, math.ceil(base_minscale * effective_scaling_factor))
                        
                        # RAY-INSPIRED: Add speculative replicas for high-risk tasks
                        # Use source='km' to prevent duplicate speculation (only critical tasks get speculation)
                        task_drop_off_km = task_failure_rates.get(task_id, 0) if task_failure_rates else 0
                        bottleneck_ratio_km = bottleneck_ratios.get(task_id, 1.0) if bottleneck_ratios else 1.0
                        speculative_km = calculate_speculative_replicas(
                            task_id, new_scale, task_drop_off_km, bottleneck_ratio_km,
                            args.workflow, last_success_rate if last_success_rate else 0.5,
                            source='km'  # Disable speculation in KM loop to prevent 200+ adds
                        )
                        if speculative_km > 0:
                            if MACHINE_ALIGNED_SCALING:
                                # Align to machine boundary
                                new_scale_spec = align_to_machine_boundary(new_scale + speculative_km, round_up=True)
                                if new_scale_spec > new_scale and new_scale_spec <= hard_cap_km:
                                    new_scale = new_scale_spec
                            else:
                                new_scale += speculative_km
                        
                        # Apply maximum scale cap if specified (cost optimization)
                        # Cost-aware max_scale reduction when success rate is above target
                        effective_max_scale_km = args.max_scale
                        if args.max_scale is not None and last_success_rate is not None:
                            target_success_km = get_target_success_rate(args.workflow)
                            if last_success_rate >= target_success_km and cached_cost_per_pod_per_sec and cached_cost_per_pod_per_sec > 0:
                                normalized_cost_km = cached_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                                if normalized_cost_km > 2.0:  # Very expensive pods (>2x baseline)
                                    effective_max_scale_km = int(args.max_scale * 0.7)  # Reduce by 30%
                                elif normalized_cost_km > 1.5:  # Moderately expensive pods (>1.5x baseline)
                                    effective_max_scale_km = int(args.max_scale * 0.85)  # Reduce by 15%
                                elif normalized_cost_km > 1.0:  # Standard expensive pods
                                    effective_max_scale_km = int(args.max_scale * 0.95)  # Reduce by 5%
                        
                        if effective_max_scale_km is not None:
                            new_scale = min(new_scale, effective_max_scale_km)
                        
                        # HARD CAP check (hard_cap_km calculated earlier for machine-aligned scaling)
                        if new_scale > hard_cap_km:
                            print(f"   🛑 HARD CAP (KM): {task_id} scale {new_scale} → {hard_cap_km} (max {get_max_scale_multiple(args.workflow)}x base={base_minscale})")
                            new_scale = hard_cap_km
                        
                        # Cost-aware scaling: reduce scaling when success rate is good and cost is high
                        target_success = get_target_success_rate(args.workflow)
                        scale_reduction_factor = 1.0
                        if last_success_rate is not None and last_success_rate >= target_success:
                            if cached_cost_per_pod_per_sec and cached_cost_per_pod_per_sec > 0:
                                normalized_cost = cached_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                                if normalized_cost > 2.0:  # Very expensive pods (>2x baseline)
                                    scale_reduction_factor = 0.7  # Reduce scaling by 30%
                                elif normalized_cost > 1.5:  # Moderately expensive pods (>1.5x baseline)
                                    scale_reduction_factor = 0.85  # Reduce scaling by 15%
                                elif normalized_cost > 1.0:  # Standard expensive pods
                                    scale_reduction_factor = 0.95  # Reduce scaling by 5%
                        
                        if scale_reduction_factor < 1.0:
                            # Apply reduction: scale less aggressively when success rate is good
                            scale_reduction = new_scale - current_minscale
                            new_scale = current_minscale + int(scale_reduction * scale_reduction_factor)
                            if new_scale < current_minscale:
                                new_scale = current_minscale
                            print(f"   💰 Cost-aware scaling reduction: {task_id} scale reduced by {(1-scale_reduction_factor)*100:.0f}% (success={last_success_rate*100:.1f}%≥{target_success*100:.1f}%)")
                        
                        # Pareto-based cost control: if success is slightly above target and cost is high,
                        # avoid preemptive scaling and let retry service lead. Log estimated savings.
                        target_success_km = get_target_success_rate(args.workflow)
                        pareto_trigger, pareto_reason, normalized_cost, success_margin = evaluate_pareto_guard(
                            last_success_rate, target_success_km, cached_cost_per_pod_per_sec
                        )
                        if pareto_trigger:
                            pods_avoided = max(0, new_scale - current_minscale)
                            print(f"   🧮 Pareto guard (KM): skip scaling {task_id} "
                                  f"(success={last_success_rate*100:.1f}%>={target_success_km*100:.1f}%+{PARETO_SUCCESS_MARGIN*100:.1f}%, "
                                  f"norm_cost={normalized_cost:.2f}≥{PARETO_COST_NORM_THRESHOLD:.2f})")
                            record_pareto_savings(task_id, pods_avoided, cached_cost_per_pod_per_sec, "km")
                            if pods_avoided <= 0:
                                print(f"   ℹ️ Pareto guard (KM): no additional pods to avoid for {task_id}")
                            continue
                        elif normalized_cost is not None and normalized_cost >= PARETO_COST_NORM_THRESHOLD:
                            print(f"   🧮 Pareto guard (KM) not triggered for {task_id} ({pareto_reason}, "
                                  f"success_margin={success_margin:.3f}, norm_cost={normalized_cost:.2f})")
                        
                        if new_scale > current_minscale:
                            cost_benefit = calculate_kaplan_meier_cost_benefit(
                                current_minscale, new_scale, base_minscale, survival_prob,
                                exec_time=exec_time, workflow_name=args.workflow,
                                real_cost_per_pod_per_sec=cached_cost_per_pod_per_sec
                            )
                            
                            # Check if cost-benefit is acceptable
                            kaplan_meier_threshold = args.kaplan_meier_cost_benefit_threshold
                            target_success_km = get_target_success_rate(args.workflow)
                            normalized_cost_km = (cached_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                                                  if cached_cost_per_pod_per_sec and cached_cost_per_pod_per_sec > 0 else 1.0)
                            cost_win_throttle_km = get_cost_win_throttle(last_success_rate, target_success_km, normalized_cost_km, workflow_name=args.workflow)
                            
                            # COST-WIN BLOCK: Skip KM scaling entirely when throttle is very low
                            # COST-WIN BLOCK (KM): Skip when throttle is low - NO EMERGENCY BYPASS
                            if cost_win_throttle_km < 0.30:
                                print(f"   🛑 COST-WIN BLOCK (KM): {task_id} scaling SKIPPED (throttle={cost_win_throttle_km*100:.0f}% < 30%)")
                                continue  # Skip this task in KM loop - cost controls always apply
                            
                            if cost_win_throttle_km < 1.0:
                                threshold_boost_km = 1.0 + (1.0 - cost_win_throttle_km) * 1.5
                                kaplan_meier_threshold *= threshold_boost_km
                                print(f"   COST-WIN (KM): threshold ×{threshold_boost_km:.2f} (norm_cost={normalized_cost_km:.2f})")
                            
                            # When success rate is below target, prioritize recovery over cost
                            # Only apply strict cost-benefit checks when success rate is good
                            should_scale_up = False
                            should_scale_down = False
                            
                            if last_success_rate is None or last_success_rate < target_success_km:
                                # Below target: prioritize success rate with low threshold
                                # Use 15% of normal threshold when below target (aggressive)
                                relaxed_threshold = max(kaplan_meier_threshold * 0.15, MIN_COST_BENEFIT_FLOOR)
                                
                                # Efficiency flag doesn't block scaling, just logs
                                if scaling_efficiency_tracker['scaling_paused']:
                                    relaxed_threshold = max(kaplan_meier_threshold * 0.25, MIN_COST_BENEFIT_FLOOR)
                                    print(f"   ℹ️ Scaling efficiency note: using threshold {relaxed_threshold:.4f}")
                                
                                if cost_benefit >= relaxed_threshold:
                                    should_scale_up = True
                                else:
                                    print(f"   💰 Skipping {task_id} scaling: cost-benefit {cost_benefit:.4f} < threshold {relaxed_threshold:.4f} (success {last_success_rate*100:.1f}% < target {target_success_km*100:.1f}%)")
                            elif last_success_rate >= target_success_km:
                                # At or above target: apply STRICT cost-benefit checks
                                # Use 2x higher threshold to prevent wasteful scaling
                                strict_threshold = kaplan_meier_threshold * 2.0  
                                low_cost_benefit_threshold_km = kaplan_meier_threshold * 0.5  # 50% of normal = low
                                
                                if cost_benefit >= strict_threshold:
                                    # Cost-benefit is strong, allow scale up
                                    should_scale_up = True
                                elif cost_benefit < low_cost_benefit_threshold_km and current_minscale > base_minscale:
                                    # Cost-benefit is very low and success rate is good - mild scale down
                                    should_scale_down = True
                                else:
                                    # Cost-benefit not strong enough when already at target - don't scale
                                    print(f"   💰 Skipping {task_id} scaling: cost-benefit {cost_benefit:.4f} < strict threshold {strict_threshold:.4f} (success {last_success_rate*100:.1f}% ≥ target)")
                            
                            if should_scale_up:
                                base_factor = metadata[task_id].get("scaling_factor", 1.0)
                                bottleneck_ratio = bottleneck_ratios.get(task_id, 1.0)
                                if bottleneck_ratio > 1.0:
                                    print(f"   📈 {task_id}: {current_minscale} → {new_scale} "
                                          f"(HEFT={base_factor:.2f}, bottleneck={bottleneck_ratio:.1f}x, "
                                          f"effective={effective_scaling_factor:.2f}, "
                                          f"cost-benefit={cost_benefit:.4f})")
                                else:
                                    print(f"   📈 {task_id}: {current_minscale} → {new_scale} "
                                          f"(HEFT={effective_scaling_factor:.2f}, "
                                          f"cost-benefit={cost_benefit:.4f})")
                                scale_function_spec(task_id, new_scale)
                                # Track that we scaled this task (prevents duplicate scaling)
                                tasks_scaled_this_km_tick.add(task_id)
                                # Update metadata so subsequent decisions use correct pod counts
                                metadata[task_id]['minscale'] = new_scale
                                # Mark proactive scaling for cooldown tracking
                                if km_tier == 'proactive':
                                    mark_proactive_scaled(task_id)
                            elif should_scale_down:
                                low_cost_benefit_threshold_km = kaplan_meier_threshold * 0.3
                                scale_down_factor = 0.90 if cost_benefit < low_cost_benefit_threshold_km * 0.5 else 0.95
                                raw_new_scale_down = max(base_minscale, int(current_minscale * scale_down_factor))
                                
                                # MACHINE-ALIGNED SCALE-DOWN
                                if MACHINE_ALIGNED_SCALING:
                                    new_scale_down = align_to_machine_boundary(raw_new_scale_down, round_up=False)
                                    new_scale_down = max(base_minscale, new_scale_down)
                                else:
                                    new_scale_down = raw_new_scale_down
                                
                                if new_scale_down < current_minscale:
                                    # Scale-downs don't trigger global cooldown
                                    scale_function_spec(task_id, new_scale_down, record_global_cooldown=False)
                                    # Track scaled task and update metadata
                                    tasks_scaled_this_km_tick.add(task_id)
                                    metadata[task_id]['minscale'] = new_scale_down
                                    print(f"   🖥️ Mild scale down (machine-aligned) for {task_id}: {current_minscale} → {new_scale_down} "
                                          f"(cost-benefit={cost_benefit:.4f} too low, success={last_success_rate*100:.1f}%≥{target_success_km*100:.1f}%)")

            elif survival_prob > args.scale_down_threshold:
                print(f"✅ Low risk on {machine_id}. Consider scale down.")
                for task_id in machine_tasks[machine_id]:
                    # Skip tasks already scaled in this KM tick
                    if task_id in tasks_scaled_this_km_tick:
                        continue
                    
                    func_spec = all_function_specs.get(task_id)
                    if not func_spec: continue
                    current_minscale = func_spec['minscale']
                    base_minscale = metadata[task_id].get("minscale", 1)
                    
                    # Cost-aware scale down: be more aggressive when success rate is good and cost is high
                    target_success = get_target_success_rate(args.workflow)
                    should_scale_down = False
                    scale_down_factor = 1.0
                    
                    if last_success_rate is not None and last_success_rate >= target_success:
                        # Success rate is at or above target - can scale down more aggressively
                        if cached_cost_per_pod_per_sec and cached_cost_per_pod_per_sec > 0:
                            normalized_cost = cached_cost_per_pod_per_sec * 3600 / COST_BASELINE_PER_POD_HOUR
                            if normalized_cost > 2.0:  # Very expensive pods (>2x baseline)
                                # Scale down very aggressively for very expensive pods
                                scale_down_factor = 0.75  # Scale down to 75% of current (25% reduction)
                                should_scale_down = True
                                print(f"   💰 Aggressive cost-aware scale down for {task_id}: cost=${cached_cost_per_pod_per_sec*3600:.2f}/pod/hour, success={last_success_rate*100:.1f}%≥{target_success*100:.1f}%")
                            elif normalized_cost > 1.5:  # Expensive pods (>1.5x baseline)
                                # Scale down more aggressively for expensive pods when success rate is good
                                scale_down_factor = 0.85  # Scale down to 85% of current (15% reduction)
                                should_scale_down = True
                                print(f"   💰 Cost-aware scale down for {task_id}: cost=${cached_cost_per_pod_per_sec*3600:.2f}/pod/hour, success={last_success_rate*100:.1f}%≥{target_success*100:.1f}%")
                            elif normalized_cost > 1.0:  # Moderately expensive pods (>1x baseline)
                                scale_down_factor = 0.90  # Scale down to 90% of current (10% reduction)
                                should_scale_down = True
                        else:
                            # No cost data, but success rate is good - still scale down conservatively
                            scale_down_factor = 0.95  # Scale down to 95% of current (5% reduction)
                            should_scale_down = True
                    
                    if should_scale_down and current_minscale > base_minscale:
                        # Scale down gradually: reduce by scale_down_factor, but don't go below base
                        raw_new_scale = max(base_minscale, int(current_minscale * scale_down_factor))
                        
                        # MACHINE-ALIGNED SCALE-DOWN: Round down to machine boundary
                        if MACHINE_ALIGNED_SCALING:
                            new_scale = align_to_machine_boundary(raw_new_scale, round_up=False)
                            new_scale = max(base_minscale, new_scale)  # Never go below base
                        else:
                            new_scale = raw_new_scale
                        
                        if new_scale < current_minscale:
                            # Scale-downs don't trigger global cooldown
                            scale_function_spec(task_id, new_scale, record_global_cooldown=False)
                            tasks_scaled_this_km_tick.add(task_id)
                            metadata[task_id]['minscale'] = new_scale
                            print(f"   🖥️ Cost-aware scale down (machine-aligned): {task_id} {current_minscale} → {new_scale} (factor={scale_down_factor:.2f})")
                    elif current_minscale > base_minscale:
                        # Standard scale down: only when survival_prob is high - reset to base
                        # Scale-downs don't trigger global cooldown
                        scale_function_spec(task_id, base_minscale, record_global_cooldown=False)
                        tasks_scaled_this_km_tick.add(task_id)
                        metadata[task_id]['minscale'] = base_minscale

        print("------")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()