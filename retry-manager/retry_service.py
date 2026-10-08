#!/usr/bin/env python3
import time
import json
import os
import argparse
import random
import heapq
from typing import Tuple, Any, Dict, Set, List
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
from mysql.connector import pooling
import requests

# -------------------------
# Configuration (env defaults)
# -------------------------
DB_CONFIG = {
    'user': os.getenv('MYSQL_USER', 'remote'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'database': os.getenv('MYSQL_DB', 'wms'),
}
WORKFLOW_DIR = os.getenv('WORKFLOW_DIR', 'workflows')
FISSION_ROUTER_PREFIX = os.getenv(
    'FISSION_ROUTER_PREFIX',
    f"{os.getenv('ROUTER_BASE_URL', 'http://localhost:32363')}/fission-function/"
)
EXPERIMENT_TAG = os.getenv('EXPERIMENT_TAG', 'main_experiment')

# -------------------------
# Workflow behavior — tune via environment variables (see .env.example)
# -------------------------
WF_MAX_RETRIES         = int(os.getenv('WF_MAX_RETRIES', '20'))
WF_TARGET_SUCCESS_RATE = float(os.getenv('WF_TARGET_SUCCESS_RATE', '0.85'))
WF_MIN_BACKOFF_SEC     = int(os.getenv('WF_MIN_BACKOFF_SEC', '10'))
WF_MAX_BACKOFF_SEC     = int(os.getenv('WF_MAX_BACKOFF_SEC', '240'))
WF_BACKOFF_MULTIPLIER  = float(os.getenv('WF_BACKOFF_MULTIPLIER', '1.3'))

POLL_INTERVAL          = int(os.getenv('WF_POLL_INTERVAL', '5'))
MAX_RETRIES_PER_UID    = 3
COOLDOWN_SEC           = 60

ENABLE_ADAPTIVE_RETRY  = True
BACKOFF_JITTER         = 0.2

PARETO_SUCCESS_MARGIN        = 0.03
PARETO_COST_NORM_THRESHOLD   = 1.2
PARETO_MAX_RETRY_BOOST       = 0.25
PARETO_COOLDOWN_FACTOR       = 0.8

RETRY_LEAD_ENABLED           = os.getenv("RETRY_LEAD_ENABLED", "true").lower() not in {"0", "false", "no"}
RETRY_LEAD_ROI_THRESHOLD     = 0.5
RETRY_LEAD_NEGATIVE_TICKS    = 2
RETRY_LEAD_COST_NORM_THRESHOLD = 1.2
RETRY_LEAD_NEAR_TARGET_FRAC  = 0.10
RETRY_LEAD_COOLDOWN_FACTOR   = 0.7
RETRY_LEAD_PRIORITY_BOOST    = 150
RETRY_LEAD_MAX_UIDS_FACTOR   = 1.25

COST_BASELINE_PER_POD_HOUR   = 0.10
PERFORMANCE_VARIATION_BUFFER = 1.24

# Success rate tracking cache (to avoid repeated DB queries)
_success_rate_cache = {}
_success_rate_cache_time = {}
SUCCESS_RATE_CACHE_TTL = 300  # Cache success rates for 5 minutes

# Checkpointer-aware retry (checkscale): only prioritize retries from checkpointer-selected tasks
# When success rate is above threshold, retry only from selected checkpoints; when below, also retry from non-selected
CHECKPOINTER_SELECTED_PRIORITY_BOOST = 2000   # Priority boost when last checkpoint is at a checkpointer-selected task
# TUNED: Increased from 0.05 to 0.10 - allow non-selected retries when within 10% of target
# This helps complex workflows recover faster when they're struggling
SUCCESS_RATE_THRESHOLD_FOR_NON_SELECTED = 0.10  # Below target by this much → also retry from non-selected checkpoints

# ============================================================================
# STATE MACHINE-INSPIRED WORKFLOW TRACKING (from State Machines Simplified whitepaper)
# ============================================================================
# Each workflow instance (UID) has a state that determines valid retry actions.
# States: RUNNING -> STALLED -> RECOVERING -> (COMPLETED | FAILED)
# This prevents wasted retries on hopeless workflows and prioritizes promising ones.

from enum import Enum
from dataclasses import dataclass, field

class WorkflowState(Enum):
    """Workflow instance states inspired by Temporal/Step Functions state machines."""
    RUNNING = "running"           # Workflow is actively executing
    STALLED = "stalled"           # No progress detected (heartbeat timeout)
    RECOVERING = "recovering"     # Retry in progress, recovering from failure
    CHECKPOINT_RECOVERY = "checkpoint_recovery"  # Recovering from a checkpoint
    WAITING_RETRY = "waiting_retry"  # Waiting for retry cooldown
    COMPLETED = "completed"       # All tasks finished successfully
    FAILED = "failed"             # Max retries exceeded or unrecoverable error
    ABANDONED = "abandoned"       # Marked as unrecoverable (saga compensation triggered)

@dataclass
class WorkflowInstance:
    """Track state machine for a single workflow instance (UID)."""
    uid: str
    workflow_name: str
    state: WorkflowState = WorkflowState.RUNNING
    last_progress_time: float = field(default_factory=time.time)
    last_completed_task: str = None
    completed_tasks: set = field(default_factory=set)
    retry_count: int = 0
    checkpoint_task: str = None  # Last checkpoint we can recover from
    stall_count: int = 0         # Number of times workflow stalled
    recovery_attempts: int = 0   # Number of recovery attempts from checkpoints
    created_at: float = field(default_factory=time.time)
    
    def record_progress(self, task_id: str):
        """Record that a task completed - resets stall detection."""
        self.last_progress_time = time.time()
        self.last_completed_task = task_id
        self.completed_tasks.add(task_id)
        if self.state in (WorkflowState.STALLED, WorkflowState.RECOVERING):
            self.state = WorkflowState.RUNNING
    
    def check_stalled(self, stall_threshold_sec: float = 120.0) -> bool:
        """Check if workflow has stalled (no progress for threshold seconds)."""
        if self.state in (WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.ABANDONED):
            return False
        elapsed = time.time() - self.last_progress_time
        if elapsed > stall_threshold_sec:
            if self.state != WorkflowState.STALLED:
                self.state = WorkflowState.STALLED
                self.stall_count += 1
            return True
        return False
    
    def can_retry(self, max_retries: int) -> bool:
        """State transition guard: check if retry is allowed from current state."""
        if self.state in (WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.ABANDONED):
            return False
        if self.retry_count >= max_retries:
            self.state = WorkflowState.FAILED
            return False
        return True
    
    def start_recovery(self, from_checkpoint: bool = False):
        """Transition to recovery state."""
        if from_checkpoint:
            self.state = WorkflowState.CHECKPOINT_RECOVERY
            self.recovery_attempts += 1
        else:
            self.state = WorkflowState.RECOVERING
        self.retry_count += 1
    
    def mark_completed(self):
        """Mark workflow as successfully completed."""
        self.state = WorkflowState.COMPLETED
    
    def mark_abandoned(self, reason: str = None):
        """Mark workflow as abandoned (saga compensation may be needed)."""
        self.state = WorkflowState.ABANDONED
        
    def get_priority_score(self) -> int:
        """Calculate retry priority based on state machine position.
        Higher score = higher priority for retry resources.
        
        Inspired by Temporal's workflow priority and Step Functions' state tracking.
        """
        base_priority = 0
        
        # Workflows closer to completion get higher priority (don't waste work)
        completion_ratio = len(self.completed_tasks) / max(1, len(self.completed_tasks) + 3)
        base_priority += int(completion_ratio * 100)
        
        # Checkpoint recovery gets priority (cheaper than full restart)
        if self.checkpoint_task:
            base_priority += 50
        
        # Penalize workflows that have stalled multiple times
        base_priority -= self.stall_count * 20
        
        # Penalize high retry counts (diminishing returns)
        base_priority -= self.retry_count * 10
        
        # Boost RECOVERING state (actively being worked on)
        if self.state == WorkflowState.RECOVERING:
            base_priority += 30
        elif self.state == WorkflowState.CHECKPOINT_RECOVERY:
            base_priority += 40  # Checkpoint recovery is most valuable
        
        return base_priority

# Global workflow state tracking
_workflow_instances: Dict[Tuple[str, str, str], WorkflowInstance] = {}  # (workflow, baseline, uid) -> instance

# Heartbeat/stall detection configuration
# RELAXED: Previous values were too aggressive, causing premature workflow abandonment
STALL_DETECTION_THRESHOLD_SEC = 180.0  # Consider stalled after 180s without progress (was 90s)
MAX_STALL_COUNT = 10  # Abandon workflow after 10 stalls (was 3) - give more chances
MAX_CHECKPOINT_RECOVERY_ATTEMPTS_DEFAULT = 15  # Default max attempts to recover from checkpoint (was 5)
MAX_CHECKPOINT_RECOVERY_ATTEMPTS_COMPLEX = 20  # Complex workflows get more recovery attempts
MAX_CHECKPOINT_RECOVERY_ATTEMPTS_LONGEST = 30  # Longest workflows (wf-4, wf-8) get most attempts

# Stale UID filter (avoid retrying UIDs from previous runs)
STALE_UID_THRESHOLD_SEC = 7200  # 2 hours since last attempt -> treat as stale

def get_or_create_workflow_instance(workflow: str, baseline: str, uid: str) -> WorkflowInstance:
    """Get existing workflow instance or create new one."""
    key = (workflow, baseline, uid)
    if key not in _workflow_instances:
        _workflow_instances[key] = WorkflowInstance(
            uid=uid,
            workflow_name=workflow
        )
    return _workflow_instances[key]

def update_workflow_progress(workflow: str, baseline: str, uid: str, task_id: str, 
                            checkpoint_task: str = None):
    """Update workflow instance when task completes."""
    instance = get_or_create_workflow_instance(workflow, baseline, uid)
    instance.record_progress(task_id)
    if checkpoint_task:
        instance.checkpoint_task = checkpoint_task

def check_stalled_workflows(workflow: str, baseline: str) -> List[WorkflowInstance]:
    """Find all stalled workflow instances for a workflow/baseline."""
    stalled = []
    for key, instance in _workflow_instances.items():
        if key[0] == workflow and key[1] == baseline:
            if instance.check_stalled(STALL_DETECTION_THRESHOLD_SEC):
                stalled.append(instance)
    return stalled

def get_max_checkpoint_recovery_attempts(workflow_name: str) -> int:
    """Get workflow-specific max checkpoint recovery attempts."""
    if True:
        return MAX_CHECKPOINT_RECOVERY_ATTEMPTS_LONGEST
    if True:
        return MAX_CHECKPOINT_RECOVERY_ATTEMPTS_COMPLEX
    return MAX_CHECKPOINT_RECOVERY_ATTEMPTS_DEFAULT

def should_abandon_workflow(instance: WorkflowInstance) -> Tuple[bool, str]:
    """Determine if workflow should be abandoned based on state machine rules.
    
    Returns (should_abandon, reason) tuple.
    """
    # Too many stalls - workflow is likely stuck on a persistent issue
    if instance.stall_count >= MAX_STALL_COUNT:
        return True, f"Exceeded max stall count ({MAX_STALL_COUNT})"
    
    # Too many checkpoint recovery attempts - checkpoint may be corrupted
    max_attempts = get_max_checkpoint_recovery_attempts(instance.workflow_name)
    if instance.recovery_attempts >= max_attempts:
        return True, f"Exceeded max checkpoint recovery attempts ({max_attempts})"
    
    # Workflow has been running too long without completion (30 minutes)
    # RELAXED: 10 minutes was too aggressive for complex workflows
    if time.time() - instance.created_at > 1800 and len(instance.completed_tasks) == 0:
        return True, "No progress after 30 minutes"
    
    return False, None

def get_retry_priority_queue(workflow: str, baseline: str, 
                             candidates: List[str]) -> List[Tuple[int, str]]:
    """Sort retry candidates by state machine priority.
    
    Returns list of (priority_score, uid) sorted by priority (highest first).
    """
    scored = []
    for uid in candidates:
        instance = get_or_create_workflow_instance(workflow, baseline, uid)
        
        # Check if should abandon
        should_abandon, reason = should_abandon_workflow(instance)
        if should_abandon:
            instance.mark_abandoned(reason)
            print(f"   🛑 STATE-MACHINE: Abandoning {uid}: {reason}")
            continue
        
        priority = instance.get_priority_score()
        scored.append((priority, uid))
    
    # Sort by priority (highest first)
    scored.sort(key=lambda x: -x[0])
    return scored

def cleanup_completed_instances(workflow: str, baseline: str, completed_uids: Set[str]):
    """Mark completed workflow instances and clean up old entries."""
    for uid in completed_uids:
        key = (workflow, baseline, uid)
        if key in _workflow_instances:
            _workflow_instances[key].mark_completed()
    
    # Clean up old completed/failed instances (keep last 1000)
    if len(_workflow_instances) > 2000:
        # Remove completed/failed instances older than 5 minutes
        cutoff = time.time() - 300
        to_remove = [
            k for k, v in _workflow_instances.items()
            if v.state in (WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.ABANDONED)
            and v.last_progress_time < cutoff
        ]
        for k in to_remove[:len(_workflow_instances) - 1000]:
            del _workflow_instances[k]

# ============================================================================
# RETRY EFFICIENCY TRACKING - Monitors retry effectiveness
# ============================================================================
retry_efficiency_tracker = {
    'last_success_rate': None,
    'last_total_retries': 0,
    'ticks_without_improvement': 0,
    'max_ticks_without_improvement': 10,  # Only pause after 10 ticks with no improvement (relaxed)
    'retry_paused': False,
    'pause_reason': None
}

# RETRY SERVICE IS THE HERO - aggressive batch sizes
# Scaler is throttled, so retry service must handle most recovery
# Configurable via env: RETRY_MAX_RETRIES_PER_CYCLE, RETRY_THREAD_POOL_SIZE (see below)
MAX_RETRIES_PER_CYCLE = int(os.getenv('RETRY_MAX_RETRIES_PER_CYCLE', '250'))   # Batch per cycle (was 150)
MAX_TOTAL_RETRIES_BUDGET = 5000  # Very high budget - retry service is primary recovery

def update_retry_efficiency(current_success_rate, total_retries_this_cycle):
    """
    Track retry efficiency for monitoring purposes.
    Returns True always - we don't pause retries, just log efficiency.
    """
    global retry_efficiency_tracker
    tracker = retry_efficiency_tracker
    
    # Track total retries (for monitoring only)
    tracker['last_total_retries'] += total_retries_this_cycle
    
    # Check if success rate improved since last check
    if tracker['last_success_rate'] is not None:
        success_improved = current_success_rate > tracker['last_success_rate'] + 0.01  # 1% improvement threshold
        retries_increased = total_retries_this_cycle > 10  # Did significant retrying
        
        if retries_increased and not success_improved:
            tracker['ticks_without_improvement'] += 1
            if tracker['ticks_without_improvement'] % 5 == 0:  # Log every 5 ticks
                print(f"ℹ️ Retry efficiency note: {total_retries_this_cycle} retries, success rate {tracker['last_success_rate']*100:.1f}%→{current_success_rate*100:.1f}%. Ticks: {tracker['ticks_without_improvement']}")
        else:
            if success_improved:
                print(f"✅ Retry efficiency: Success improved {tracker['last_success_rate']*100:.1f}%→{current_success_rate*100:.1f}%")
            tracker['ticks_without_improvement'] = 0
        
        # Only flag for monitoring, don't pause
        if tracker['ticks_without_improvement'] >= tracker['max_ticks_without_improvement']:
            tracker['retry_paused'] = True  # Flag only, doesn't block retries
            tracker['pause_reason'] = f"Low efficiency: {tracker['ticks_without_improvement']} ticks without improvement"
    
    # Update tracked values
    tracker['last_success_rate'] = current_success_rate
    
    return True  # Always continue retrying - success rate is priority

def reset_retry_efficiency_tracker():
    """Reset the retry efficiency tracker (call at start of each workflow experiment)."""
    global retry_efficiency_tracker
    retry_efficiency_tracker = {
        'last_success_rate': None,
        'last_total_retries': 0,
        'ticks_without_improvement': 0,
        'max_ticks_without_improvement': 5,
        'retry_paused': False,
        'pause_reason': None
    }

def compute_adaptive_pressure_multiplier(success_rate, target_success, normalized_cost=1.0, failure_rate_hint=None):
    """Blend success gap, cost, and observed failures into a single multiplier.
    
    - Below target: prioritize recovery; cost can soften but never fully block retries.
    - Above target: optimize cost; keep headroom if failures persist.
    """
    if success_rate is None or target_success is None or target_success <= 0:
        return 1.0
    
    failure_component = max(0.0, min(1.0, failure_rate_hint)) if failure_rate_hint is not None else 0.0
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

def get_target_success_rate(workflow_name: str = None) -> float:
    return WF_TARGET_SUCCESS_RATE

def get_checkpointer_selected_tasks(workflow: str, baseline: str) -> Set[str]:
    """
    Get task_ids that the checkpointer selected for checkpointing (workflow_checkpoint_plan).
    Used for checkscale: retry service prioritizes retries from these checkpoints only when
    success rate is above threshold; when below, also retries from non-selected checkpoints.
    Returns empty set if no plan exists (backward compat: no filtering).
    """
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        query = """SELECT task_id FROM workflow_checkpoint_plan
               WHERE workflow_name = %s AND baseline = %s AND is_checkpoint = 1"""
        params = [workflow, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        selected = {row[0] for row in cursor.fetchall() if row[0]}
        cursor.close()
        conn.close()
        return selected
    except Exception:
        return set()

def should_pareto_bias_retries(success_rate: float, target_success: float, normalized_cost: float) -> bool:
    """Return True when retries should lead because scaling is cost-inefficient."""
    if success_rate is None or target_success is None:
        return False
    normalized_cost = normalized_cost if normalized_cost is not None else 1.0
    success_margin = success_rate - target_success
    return success_margin >= PARETO_SUCCESS_MARGIN and normalized_cost >= PARETO_COST_NORM_THRESHOLD

# ============================================================================
# ROI-AWARE RETRY LOGIC
# ============================================================================
# When the scaler has negative ROI (scaling not helping), the retry service
# should become more aggressive to compensate

# Track ROI state for retry decisions
_roi_tracking = {
    'last_success_rate': None,
    'last_pods': None,
    'last_cost': None,
    'negative_roi_ticks': 0,
    'estimated_scaler_roi': 0.0,
    'retry_boost_active': False,
}

def estimate_scaler_roi(workflow_name: str, baseline: str, current_success: float) -> float:
    """
    Estimate the scaler's ROI based on recent cost vs success improvement.
    This helps the retry service decide when to be more aggressive.
    
    Returns estimated ROI (positive = scaler is helping, negative = wasting resources)
    """
    global _roi_tracking
    
    # Get current cost from cost_logs
    current_cost = get_recent_cost_per_attempt(workflow_name, baseline, lookback_seconds=300)
    
    # Get current pod count from workflow_status table or cost_logs
    current_pods = 0
    try:
        with mysql_connection() as conn:
            cursor = conn.cursor()
            # Estimate pods from recent cost entries
            params = [workflow_name, baseline]
            notes_clause = ""
            if EXPERIMENT_TAG:
                notes_clause = " AND notes = %s"
                params.append(EXPERIMENT_TAG)
            cursor.execute(
                f"""
                SELECT AVG(pod_count) FROM (
                    SELECT COUNT(DISTINCT machine_id) * 3 as pod_count 
                    FROM cost_logs 
                    WHERE workflow_name = %s AND baseline = %s
                      AND check_timestamp > DATE_SUB(NOW(), INTERVAL 5 MINUTE){notes_clause}
                    GROUP BY machine_id
                ) t
                """,
                tuple(params)
            )
            result = cursor.fetchone()
            if result and result[0]:
                current_pods = int(result[0])
    except Exception:
        pass  # Estimation is best-effort
    
    # Compare with previous state
    if _roi_tracking['last_success_rate'] is not None:
        success_change = current_success - _roi_tracking['last_success_rate']
        cost_change = (current_cost or 0) - (_roi_tracking['last_cost'] or 0)
        pods_change = current_pods - (_roi_tracking['last_pods'] or 0)
        
        # ROI = success_gain (%) / cost_spent ($)
        if cost_change > 0.001:  # Avoid division by near-zero
            roi = (success_change * 100) / cost_change
        elif success_change > 0:
            roi = 100.0  # Improved without cost increase = great ROI
        else:
            roi = 0.0 if success_change == 0 else -10.0  # No change or regression
        
        # Track negative ROI
        if roi < 0 and pods_change > 0:
            _roi_tracking['negative_roi_ticks'] += 1
        elif roi > 0:
            _roi_tracking['negative_roi_ticks'] = max(0, _roi_tracking['negative_roi_ticks'] - 1)
        
        _roi_tracking['estimated_scaler_roi'] = roi
    else:
        roi = 0.0  # First call
    
    # Update tracking state
    _roi_tracking['last_success_rate'] = current_success
    _roi_tracking['last_cost'] = current_cost
    _roi_tracking['last_pods'] = current_pods
    
    return roi

def should_retry_boost_for_negative_roi(workflow_name: str, baseline: str, current_success: float,
                                        estimated_roi: float = None) -> Tuple[bool, float]:
    """
    Determine if retry service should boost its aggressiveness due to negative scaler ROI.
    
    Returns:
        (should_boost, boost_factor)
        - should_boost: True if retry service should be more aggressive
        - boost_factor: 1.0-1.5, multiplier for retry limits and speed
    """
    global _roi_tracking
    
    if estimated_roi is None:
        roi = estimate_scaler_roi(workflow_name, baseline, current_success)
    else:
        roi = estimated_roi
        _roi_tracking['estimated_scaler_roi'] = roi
    target_success = get_target_success_rate(workflow_name)
    success_gap = target_success - current_success
    
    should_boost = False
    boost_factor = 1.0
    
    # Case 1: Negative ROI for multiple ticks - scaler is wasting resources
    if _roi_tracking['negative_roi_ticks'] >= 2:
        should_boost = True
        boost_factor = 1.3  # 30% more aggressive retries
        print(f"   📉 ROI-AWARE: Scaler has negative ROI for {_roi_tracking['negative_roi_ticks']} ticks, retry boost active")
    
    # Case 2: Low ROI and below target - retries should help
    elif roi < 0.5 and success_gap > 0.10:  # ROI < 0.5% per $ and >10% below target
        should_boost = True
        boost_factor = 1.2  # 20% more aggressive
        print(f"   ⚠️ ROI-AWARE: Low scaler ROI ({roi:.2f}), retry boost active")
    
    # Case 3: Far below target - retries are critical regardless of ROI
    elif success_gap > 0.30:  # >30% below target
        should_boost = True
        boost_factor = 1.4  # 40% more aggressive for critical situations
        print(f"   🚨 ROI-AWARE: Critical success gap ({success_gap*100:.1f}%), emergency retry boost active")
    
    _roi_tracking['retry_boost_active'] = should_boost
    
    return should_boost, boost_factor

def get_retry_lead_policy(workflow_name: str, current_success: float,
                          normalized_cost: float, estimated_roi: float) -> Tuple[bool, str, Dict[str, float]]:
    """Determine if retry service should lead when scaling ROI is weak."""
    if not RETRY_LEAD_ENABLED:
        return False, "", {}
    target_success = get_target_success_rate(workflow_name)
    if current_success is None or target_success is None or target_success <= 0:
        return False, "", {}

    normalized_cost = normalized_cost if normalized_cost is not None else 1.0
    gap = target_success - current_success
    near_target = gap <= target_success * RETRY_LEAD_NEAR_TARGET_FRAC
    negative_ticks = _roi_tracking.get('negative_roi_ticks', 0) >= RETRY_LEAD_NEGATIVE_TICKS
    low_roi = estimated_roi is not None and estimated_roi < RETRY_LEAD_ROI_THRESHOLD
    high_cost = normalized_cost >= RETRY_LEAD_COST_NORM_THRESHOLD

    if negative_ticks or (low_roi and high_cost) or (near_target and high_cost):
        reason = "negative ROI" if negative_ticks else ("low ROI + high cost" if low_roi and high_cost else "near target + high cost")
        policy = {
            "cooldown_factor": RETRY_LEAD_COOLDOWN_FACTOR,
            "priority_boost": RETRY_LEAD_PRIORITY_BOOST,
            "max_uids_factor": RETRY_LEAD_MAX_UIDS_FACTOR,
        }
        return True, reason, policy
    return False, "", {}

def get_workflow_success_rate(workflow_name: str, baseline: str, all_expected_tasks: Set[str] = None) -> float:
    """Get current success rate for a workflow (with caching).
    
    Args:
        workflow_name: Workflow identifier
        baseline: Baseline name
        all_expected_tasks: Set of all expected task IDs (for completion check)
    
    Returns:
        Current success rate (0.0-1.0)
    """
    cache_key = (workflow_name, baseline)
    current_time = time.time()
    
    # Check cache
    if cache_key in _success_rate_cache:
        cache_time = _success_rate_cache_time.get(cache_key, 0)
        if current_time - cache_time < SUCCESS_RATE_CACHE_TTL:
            return _success_rate_cache[cache_key]
    
    expected_tasks = set(all_expected_tasks) if all_expected_tasks else set()
    if not expected_tasks:
        tasks_path = os.path.join(WORKFLOW_DIR, workflow_name, "tasks.json")
        try:
            with open(tasks_path) as f:
                tasks_data = json.load(f)
            expected_tasks = set((tasks_data or {}).get("tasks", {}).keys())
        except Exception:
            expected_tasks = set()
    
    expected_task_count = len(expected_tasks)
    
    # Calculate success rate
    with mysql_connection() as conn:
        cursor = conn.cursor()
        
        # Get all UIDs for this workflow
        query = "SELECT DISTINCT uuid_passed FROM serverless_workflows USE INDEX (idx_swv_wb) WHERE workflow_name=%s AND baseline=%s"
        params = [workflow_name, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        all_uids = {uid for (uid,) in cursor.fetchall() if uid}
        
        if not all_uids or expected_task_count == 0:
            success_rate = 0.0
        else:
            completion_counts = query_task_completion_counts(cursor, workflow_name, baseline, expected_tasks)
            succeeded = {
                uid for uid in all_uids
                if completion_counts.get(uid, 0) >= expected_task_count
            }
            success_rate = len(succeeded) / len(all_uids) if all_uids else 0.0
    
    # Update cache
    _success_rate_cache[cache_key] = success_rate
    _success_rate_cache_time[cache_key] = current_time
    
    return success_rate

def get_recent_cost_per_attempt(workflow_name: str, baseline: str, lookback_seconds: int = 600) -> float:
    """Fetch average recent cost per attempt from cost_logs (if available).
    
    Returns:
        cost_per_attempt (in $/pod/sec) or None if unavailable.
    """
    try:
        since_ts = datetime.utcnow() - timedelta(seconds=lookback_seconds)
        with mysql_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT price_per_hour
                FROM cost_logs 
                WHERE workflow_name=%s AND baseline=%s AND timestamp >= %s
            """
            params = [workflow_name, baseline, since_ts]
            if EXPERIMENT_TAG:
                query += " AND notes = %s"
                params.append(EXPERIMENT_TAG)
            query += " ORDER BY timestamp DESC LIMIT 200"
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            # Convert hourly price to per-second cost
            costs = []
            for (price_per_hour,) in rows:
                if price_per_hour is None:
                    continue
                try:
                    p = float(price_per_hour)
                except Exception:
                    continue
                if p < 0:
                    continue
                costs.append(p / 3600.0)
            if costs:
                return sum(costs) / len(costs)
    except Exception as e:
        print(f"⚠️ Cost fetch failed for {workflow_name}: {e}")
    return None

session = requests.Session()

# MySQL connection pool for better performance
_pool = None

# -------------------------
# DB Helpers (optimized with connection pooling)
# -------------------------
def init_pool():
    """Initialize MySQL connection pool. Retries on 'Too many connections' with backoff."""
    global _pool
    if _pool is not None:
        return _pool
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            pool_config = {
                **DB_CONFIG,
                'pool_name': 'retry_service_pool',
                'pool_size': 5,  # Number of connections in pool
                'pool_reset_session': True,
                'autocommit': False,
            }
            _pool = pooling.MySQLConnectionPool(**pool_config)
            print(f"✅ Initialized MySQL connection pool (size={pool_config['pool_size']})")
            return _pool
        except Exception as e:
            err_str = str(e).lower()
            if 'too many connections' in err_str and attempt < max_attempts:
                backoff = 2 * attempt
                print(f"⚠️ MySQL pool init failed (attempt {attempt}/{max_attempts}), retrying in {backoff}s: {e}")
                time.sleep(backoff)
            else:
                print(f"❌ MySQL connection pool init failed: {e}")
                raise
    return _pool

def mysql_connection():
    """Get a connection from the pool (or create new if pool not initialized)."""
    try:
        if _pool is None:
            init_pool()
        return _pool.get_connection()
    except Exception as e:
        # Fallback to direct connection if pool fails
        print(f"⚠️ Pool connection failed, using direct connection: {e}")
        return mysql.connector.connect(**DB_CONFIG)

def _last_attempt_at_to_utc(naive_dt):
    """Convert last_attempt_at from DB (stored in EDT) to UTC for comparison with Python (Etc/UTC)."""
    if naive_dt is None:
        return None
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None
    if ZoneInfo is not None:
        eastern = ZoneInfo("America/New_York")
        return naive_dt.replace(tzinfo=eastern).astimezone(timezone.utc)
    # Python < 3.9: assume EDT = UTC-4
    return naive_dt + timedelta(hours=4)

def setup_tables(clear=False):
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint_retries (
                uid VARCHAR(256),
                workflow_name VARCHAR(256),
                baseline VARCHAR(128),
                retry_count INT DEFAULT 0,
                last_task_id VARCHAR(256) NULL,
                last_status INT NULL,
                last_attempt_at DATETIME NULL,
                notes VARCHAR(255) DEFAULT 'main_experiment',
                PRIMARY KEY(uid, workflow_name, baseline)
            )
        """)
        try:
            cursor.execute(
                """SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME='checkpoint_retries' AND COLUMN_NAME='notes'""",
                (DB_CONFIG.get("database"),),
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute("ALTER TABLE checkpoint_retries ADD COLUMN notes VARCHAR(255) DEFAULT 'main_experiment'")
        except Exception:
            pass
        if clear:
            cursor.execute("TRUNCATE TABLE checkpoint_retries")

        # Helpful indexes for performance (ignore errors if MySQL doesn't support IF NOT EXISTS)
        # Composite indexes for common query patterns
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_swv_wb ON serverless_workflows(workflow_name, baseline)",
            "CREATE INDEX IF NOT EXISTS idx_swv_uid ON serverless_workflows(uuid_passed)",
            "CREATE INDEX IF NOT EXISTS idx_swv_uid_wb ON serverless_workflows(uuid_passed, workflow_name, baseline)",
            "CREATE INDEX IF NOT EXISTS idx_swv_wb_stage_code ON serverless_workflows(workflow_name, baseline, workflow_stage, response_code)",
            "CREATE INDEX IF NOT EXISTS idx_tc_uwb ON task_checkpoints(uid, workflow_name, baseline)",
            "CREATE INDEX IF NOT EXISTS idx_tc_uwb_ts ON task_checkpoints(uid, workflow_name, baseline, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_cr_wb_retry ON checkpoint_retries(workflow_name, baseline, retry_count)",
            "CREATE INDEX IF NOT EXISTS idx_cr_uid_wb ON checkpoint_retries(uid, workflow_name, baseline)",
            "CREATE INDEX IF NOT EXISTS idx_cr_wb_time ON checkpoint_retries(workflow_name, baseline, last_attempt_at)",
        ]:
            try: cursor.execute(stmt)
            except Exception: pass
        conn.commit()

def query_task_completion_counts(cursor, workflow_name: str, baseline: str,
                                 task_ids: Set[str]) -> Dict[str, int]:
    """Return mapping of UID -> number of successful tasks limited to task_ids."""
    task_ids = [t for t in (task_ids or []) if t]
    if not task_ids:
        return {}
    
    placeholders = ','.join(['%s'] * len(task_ids))
    params = [workflow_name, baseline]
    if EXPERIMENT_TAG:
        params.append(EXPERIMENT_TAG)
    params += task_ids
    notes_clause = " AND notes = %s" if EXPERIMENT_TAG else ""
    cursor.execute(
        f"""
        SELECT uuid_passed, COUNT(DISTINCT workflow_stage) AS completed_count
          FROM serverless_workflows USE INDEX (idx_swv_wb_stage_code)
         WHERE workflow_name=%s AND baseline=%s{notes_clause}
           AND workflow_stage IN ({placeholders})
           AND response_code=200
         GROUP BY uuid_passed
        """,
        params
    )
    return {uid: count for uid, count in cursor.fetchall()}

# -------------------------
# DAG utilities
# -------------------------
def load_workflow_tasks(workflow_name) -> Dict[str, Dict[str, Any]]:
    tasks_path = os.path.join(WORKFLOW_DIR, workflow_name, "tasks.json")
    try:
        with open(tasks_path) as f:
            data = json.load(f)
            if "tasks" not in data:
                print(f"⚠️ tasks.json for {workflow_name} missing 'tasks' key. Keys found: {list(data.keys())}")
                return {}
            tasks = data["tasks"]
            if not tasks:
                print(f"⚠️ tasks.json for {workflow_name} has empty tasks dictionary")
            else:
                print(f"📋 Loaded {len(tasks)} tasks from {workflow_name}: {sorted(tasks.keys())}")
            return tasks
    except FileNotFoundError:
        print(f"❌ tasks.json not found for {workflow_name} at {tasks_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in tasks.json for {workflow_name}: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading tasks.json for {workflow_name}: {e}")
        return {}

def build_predecessor_map(tasks_data: Dict[str, Dict[str, Any]]) -> Dict[str, Set[str]]:
    preds: Dict[str, Set[str]] = {tid: set() for tid in tasks_data}
    for tid, meta in tasks_data.items():
        for succ in meta.get("successors", []):
            preds.setdefault(succ, set()).add(tid)
    return preds

def find_source_tasks(tasks_data: Dict[str, Dict[str, Any]]) -> List[str]:
    preds = build_predecessor_map(tasks_data)
    return sorted([tid for tid, ps in preds.items() if len(ps) == 0])

def find_final_tasks(tasks_data: Dict[str, Dict[str, Any]]) -> List[str]:
    """Find tasks with no successors (final tasks in the workflow)."""
    if not tasks_data:
        return []
    final_tasks = [tid for tid, meta in tasks_data.items() if not meta.get("successors")]
    return sorted(final_tasks)

def determine_next_tasks(tasks_data: Dict[str, Dict[str, Any]], completed: Set[str]) -> List[str]:
    completed = completed or set()
    preds = build_predecessor_map(tasks_data)
    if not completed:
        return find_source_tasks(tasks_data)
    candidates = []
    for tid in sorted(tasks_data.keys()):
        if tid in completed: continue
        if preds.get(tid, set()).issubset(completed):
            candidates.append(tid)
    return candidates

def verify_dag_once(tasks_data: Dict[str, Dict[str, Any]], workflow: str):
    src = find_source_tasks(tasks_data); fin = find_final_tasks(tasks_data)
    if not src: print(f"⚠️ [{workflow}] No sources detected.")
    if not fin: print(f"⚠️ [{workflow}] No finals detected.")
    print(f"   [{workflow}] Sources={src} | Finals={fin}")

def topo_order(tasks_data):
    indeg = defaultdict(int); succ = defaultdict(list)
    for u, meta in tasks_data.items():
        for v in meta.get("successors", []):
            succ[u].append(v); indeg[v] += 1
        indeg.setdefault(u, indeg.get(u, 0))
    q = deque(sorted([t for t in tasks_data if indeg[t] == 0]))
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in succ[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    return order

def all_ancestors(tasks_data):
    preds = build_predecessor_map(tasks_data)
    memo: Dict[str, Set[str]] = {}
    def get_all_prev(t: str) -> Set[str]:
        if t in memo: return memo[t]
        base = set(preds.get(t, set()))
        for p in list(base):
            base |= get_all_prev(p)
        memo[t] = base
        return base
    return {t: get_all_prev(t) for t in tasks_data}

def backoff_chain(tasks_data, start_task, completed):
    A = all_ancestors(tasks_data)
    order = topo_order(tasks_data)
    pos = {t:i for i,t in enumerate(order)}
    preds = build_predecessor_map(tasks_data)

    cands = [start_task] + sorted(A[start_task], key=lambda t: pos[start_task]-pos[t])
    for t in cands:
        if preds.get(t, set()).issubset(completed):
            yield t

def get_checkpoint_semantic_group(tasks_data: Dict[str, Dict[str, Any]], checkpoint_task: str) -> str:
    """Classify checkpoint position in workflow: 'early', 'middle', or 'late'.
    
    Args:
        tasks_data: Workflow task definitions
        checkpoint_task: Task ID where checkpoint was taken
    
    Returns:
        'early', 'middle', or 'late'
    """
    if not tasks_data or checkpoint_task not in tasks_data:
        return 'middle'
    
    order = topo_order(tasks_data)
    if not order:
        return 'middle'
    
    try:
        position = order.index(checkpoint_task)
        total_tasks = len(order)
        ratio = position / total_tasks if total_tasks > 0 else 0.5
        
        if ratio < 0.33:
            return 'early'
        elif ratio < 0.67:
            return 'middle'
        else:
            return 'late'
    except ValueError:
        return 'middle'

def get_semantic_retry_strategy(checkpoint_group: str, workflow: str) -> Dict[str, Any]:
    """Get retry strategy parameters based on checkpoint semantic grouping.
    
    Args:
        checkpoint_group: 'early', 'middle', or 'late'
        workflow: Workflow name
    
    Returns:
        Dict with retry strategy parameters (cooldown_multiplier, priority_boost, etc.)
    """
    base_strategy = {
        'cooldown_multiplier': 1.0,
        'priority_boost': 0,
        'max_retry_boost': 0,
        'parallel_retry': False
    }
    
    # For longest workflows and workflows needing aggressive retries, apply semantic grouping strategies
    # wf-2, wf-3, wf-5, wf-7: Need aggressive retry strategies to beat baselines by 5%+
    workflows_needing_semantic = set()  # all workflows use semantic grouping
        
    # For longest workflows and aggressive retry workflows, apply semantic grouping strategies
    if checkpoint_group == 'early':
        # Early checkpoints: more aggressive retries (faster recovery from early failures)
        base_strategy['cooldown_multiplier'] = 0.7  # 30% faster retries
        base_strategy['priority_boost'] = 100
        base_strategy['max_retry_boost'] = 2  # Allow 2 extra retries
    elif checkpoint_group == 'middle':
        # Middle checkpoints: balanced approach
        base_strategy['cooldown_multiplier'] = 0.9  # 10% faster retries
        base_strategy['priority_boost'] = 50
        base_strategy['max_retry_boost'] = 1
    else:  # late
        # Late checkpoints: most aggressive (almost done, don't give up)
        base_strategy['cooldown_multiplier'] = 0.5  # 50% faster retries
        base_strategy['priority_boost'] = 200
        base_strategy['max_retry_boost'] = 3  # Allow 3 extra retries
        base_strategy['parallel_retry'] = True  # Try multiple tasks in parallel
    
    return base_strategy

# -------------------------
# Query helpers
# -------------------------
def get_max_retries_for_workflow(workflow: str, baseline: str = None, 
                                 current_success_rate: float = None,
                                 all_expected_tasks: Set[str] = None,
                                 completed_count: int = None,
                                 normalized_cost: float = None,
                                 failure_rate_hint: float = None) -> int:
    """Get the maximum retry limit for a specific workflow.
    
    Adjusts retry limit dynamically based on distance from target success rate.
    For longest workflows, also boosts retry limit for UIDs close to completion:
    - If below target: increase retry limit (up to +50% boost)
    - If at or above target: use base retry limit
    
    Args:
        workflow: Workflow identifier
        baseline: Baseline name (optional, for success rate calculation)
        current_success_rate: Current success rate (optional, will calculate if not provided)
        all_expected_tasks: Set of all expected task IDs (optional, for completion check)
    
    Returns:
        Maximum retry limit (adjusted based on target)
    """
    base_retries = WF_MAX_RETRIES
    target_success = get_target_success_rate(workflow)
    
    # If baseline and tasks provided, calculate current success rate
    if baseline and current_success_rate is None:
        current_success_rate = get_workflow_success_rate(workflow, baseline, all_expected_tasks)
    
    # Complex workflows: aggressive retry boosting when success rate is very low
    # This helps when scaler is hitting budget caps and can't add more pods
    # Apply this BEFORE completion boost calculation so completion boost applies to boosted base
    if True and current_success_rate is not None:
        if current_success_rate < 0.40:  # Critical: < 40% (way below 67.5% target)
            # Allow up to 2x base retries in critical situations
            distance_from_target = target_success - current_success_rate
            distance_boost = min(1.0, distance_from_target * 3.0)  # Up to 100% boost
            base_retries = int(base_retries * (1.0 + distance_boost))
            print(f"   🚨 {workflow} CRITICAL retry boost: {base_retries} max retries (success rate {current_success_rate*100:.1f}% < 40%)")
        elif current_success_rate < target_success * 0.60:  # Below 60% of target
            distance_from_target = target_success - current_success_rate
            distance_boost = min(0.75, distance_from_target * 2.5)  # Up to 75% boost
            base_retries = int(base_retries * (1.0 + distance_boost))
            print(f"   ⚠️ {workflow} aggressive retry boost: {base_retries} max retries (success rate {current_success_rate*100:.1f}% < {target_success*0.60*100:.1f}%)")
    
    # For longest workflows: boost retry limit for UIDs close to completion
    # If a UID is 80%+ complete, allow extra retries to finish it
    completion_boost = 0
    if True and completed_count is not None and all_expected_tasks:
        expected_task_count = len(all_expected_tasks)
        if expected_task_count > 0:
            completion_ratio = completed_count / expected_task_count
            if completion_ratio >= 0.90:  # 90%+ complete
                completion_boost = int(base_retries * 0.5)  # 50% more retries
            elif completion_ratio >= 0.80:  # 80%+ complete
                completion_boost = int(base_retries * 0.3)  # 30% more retries
    
    # Adjust retry limit based on distance from target
    # Pareto retry bias: if success is slightly above target but cost is high,
    # increase retries so retry service carries more recovery load.
    if current_success_rate is not None and current_success_rate < target_success:
        # Below target: boost retry limit
        # Distance from target: (target - current) / target
        # Boost: up to 50% more retries (for non-wf-8 workflows, wf-8 already boosted above)
        if False:  # Complex workflows already boosted above
            distance_from_target = (target_success - current_success_rate) / target_success
            boost_multiplier = 1.0 + (distance_from_target * 0.5)  # 1.0-1.5 range
            adjusted_retries = int(base_retries * boost_multiplier)
            return max(base_retries, adjusted_retries) + completion_boost  # Add completion boost
        else:
            # Complex workflows already boosted, just add completion boost
            return base_retries + completion_boost
    elif current_success_rate is not None and should_pareto_bias_retries(current_success_rate, target_success, normalized_cost):
        pareto_boost = int(base_retries * PARETO_MAX_RETRY_BOOST)
        adjusted_retries = base_retries + max(1, pareto_boost)
        print(f"   🧮 Pareto retry bias: increasing retries for {workflow} to {adjusted_retries} "
              f"(success={current_success_rate*100:.1f}%≥{target_success*100:.1f}%+{PARETO_SUCCESS_MARGIN*100:.1f}%, "
              f"norm_cost={normalized_cost:.2f}≥{PARETO_COST_NORM_THRESHOLD:.2f})")
        return adjusted_retries + completion_boost
    elif current_success_rate is not None and current_success_rate > target_success * 1.05:
        # Above target by >5%: reduce retry limit to save costs
        # Reduce by up to 30% when well above target
        excess_success = (current_success_rate - target_success) / target_success
        reduction_factor = min(0.3, excess_success * 0.5)  # Up to 30% reduction
        adjusted_retries = int(base_retries * (1.0 - reduction_factor))
        print(f"   💰 Cost optimization: reducing retries for {workflow} by {reduction_factor*100:.0f}% (success={current_success_rate*100:.1f}%>{target_success*100:.1f}%)")
        return max(int(base_retries * 0.7), adjusted_retries) + completion_boost  # Don't reduce below 70% of base
    
    adaptive_base = base_retries + completion_boost  # Add completion boost even if at target
    
    # Final adaptive pressure: blend success gap, cost, and failure hint
    if normalized_cost is None:
        normalized_cost = 1.0
    pressure_multiplier = compute_adaptive_pressure_multiplier(
        current_success_rate if current_success_rate is not None else 0.0,
        target_success,
        normalized_cost=normalized_cost,
        failure_rate_hint=failure_rate_hint
    )
    adaptive_retries = max(1, int(adaptive_base * pressure_multiplier))
    return adaptive_retries

def get_uids_incomplete_workflows(workflow: str, final_tasks: List[str], baseline: str, 
                                  all_expected_tasks: Set[str] = None,
                                  current_success_rate: float = None,
                                  normalized_cost: float = None,
                                  retry_lead_mode: bool = False,
                                  retry_lead_cooldown_factor: float = 1.0,
                                  retry_lead_priority_boost: int = 0) -> List[Tuple[int, str]]:
    """Get incomplete UIDs with priority scores - optimized with batch queries and index hints.
    
    Returns list of (priority_score, uid) tuples, sorted by priority (higher = more urgent).
    Priority factors:
    - Complex workflows get higher base priority
    - Fewer retry attempts = higher priority (retry early, fail fast)
    - More completed tasks = higher priority (closer to completion)
    - Recent failures = higher priority (fresh errors more likely to succeed on retry)
    
    Args:
        workflow: Workflow name
        final_tasks: List of final task IDs
        baseline: Baseline name
        all_expected_tasks: Set of all expected task IDs for this workflow (for completion check)
    
    Returns:
        List of (priority_score, uid) tuples, sorted descending by priority
    """
    with mysql_connection() as conn:
        cursor = conn.cursor()

        # Get all UIDs for this workflow from serverless_workflows (use index)
        query = "SELECT DISTINCT uuid_passed FROM serverless_workflows USE INDEX (idx_swv_wb) WHERE workflow_name=%s AND baseline=%s"
        params = [workflow, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        workflow_uids = {uid for (uid,) in cursor.fetchall() if uid}
        
        # Also get UIDs from checkpoints that may not have workflow entries yet
        # This is critical - checkpoints might exist for UIDs that haven't completed all tasks
        query = "SELECT DISTINCT uid FROM task_checkpoints USE INDEX (idx_tc_uwb) WHERE workflow_name=%s AND baseline=%s"
        params = [workflow, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        checkpoint_uids = {uid for (uid,) in cursor.fetchall() if uid}
        
        # Combine both sets - we need to check UIDs that have either workflow entries OR checkpoints
        all_uids = workflow_uids | checkpoint_uids
        
        if len(checkpoint_uids) > len(workflow_uids):
            print(f"   ℹ️ Found {len(checkpoint_uids)} UIDs with checkpoints, {len(workflow_uids)} with workflow entries")
            print(f"   ℹ️ {len(checkpoint_uids - workflow_uids)} UIDs have checkpoints but no workflow entries (will check for retry)")

        # Debug: Check what task names actually exist in the database
        if all_uids:
            query = "SELECT DISTINCT workflow_stage FROM serverless_workflows USE INDEX (idx_swv_wb) WHERE workflow_name=%s AND baseline=%s"
            params = [workflow, baseline]
            if EXPERIMENT_TAG:
                query += " AND notes = %s"
                params.append(EXPERIMENT_TAG)
            query += " LIMIT 20"
            cursor.execute(query, tuple(params))
            db_task_names = {task[0] for task in cursor.fetchall() if task[0]}
            if final_tasks:
                missing_in_db = set(final_tasks) - db_task_names
                extra_in_db = db_task_names - set(final_tasks)
                if missing_in_db:
                    print(f"   ⚠️ Final tasks from tasks.json not found in DB: {sorted(missing_in_db)}")
                if extra_in_db:
                    print(f"   ℹ️ Task names in DB not in final_tasks: {sorted(list(extra_in_db)[:10])}")
        
        # Always prefer all_expected_tasks if provided (more accurate completion check)
        # For complex workflows, this is critical - we need to check ALL tasks, not just final tasks
        expected_tasks = set(all_expected_tasks) if all_expected_tasks else set()
        if not expected_tasks:
            # Fallback: try to get all tasks from tasks.json if available
            try:
                tasks_path = os.path.join(WORKFLOW_DIR, workflow, "tasks.json")
                if os.path.exists(tasks_path):
                    with open(tasks_path) as f:
                        tasks_data = json.load(f)
                        expected_tasks = set(tasks_data.get("tasks", {}).keys())
            except Exception:
                pass
        
        # If still no expected tasks, fall back to final tasks or DB task names
        if not expected_tasks:
            expected_tasks = {task for task in final_tasks if task}
        if not expected_tasks and all_uids:
            expected_tasks = db_task_names

        expected_task_count = len(expected_tasks)
        
        # Only query completion counts for UIDs that have workflow entries
        # UIDs with only checkpoints (no workflow entries) are definitely incomplete
        completed_counts = query_task_completion_counts(cursor, workflow, baseline, expected_tasks) if expected_task_count else {}
        
        # UIDs are succeeded only if they have workflow entries AND workflow completed successfully
        # For branching workflows (like wf-1), success means completing EITHER branch, not all tasks
        # wf-1: Path 1 = task1→task2→task3→task3a→taskr1, Path 2 = task1→task2→task3→task3b→taskr2
        # Success if common prefix (task1, task2, task3) + EITHER branch endpoint (taskr1 OR taskr2) completed
        branching_workflows = {
            'wf-1': {
                'common_prefix': {'task1', 'task2', 'task3'},
                'branch_endpoints': ['taskr1', 'taskr2']  # Either endpoint completes = success
            }
        }
        
        succeeded = set()
        if workflow in branching_workflows:
            # Branching workflow: check if common prefix + at least one branch endpoint completed
            branch_config = branching_workflows[workflow]
            common_prefix = branch_config['common_prefix']
            branch_endpoints = branch_config['branch_endpoints']
            
            # Query for completed tasks per UID
            for uid in workflow_uids:
                query = """SELECT DISTINCT workflow_stage FROM serverless_workflows 
                       USE INDEX (idx_swv_uid_wb) 
                       WHERE uuid_passed=%s AND workflow_name=%s AND baseline=%s AND response_code=200"""
                params = [uid, workflow, baseline]
                if EXPERIMENT_TAG:
                    query += " AND notes = %s"
                    params.append(EXPERIMENT_TAG)
                cursor.execute(query, tuple(params))
                completed_tasks = {row[0] for row in cursor.fetchall() if row[0]}
                
                # Check if common prefix completed
                if common_prefix.issubset(completed_tasks):
                    # Check if at least one branch endpoint completed
                    if any(endpoint in completed_tasks for endpoint in branch_endpoints):
                        succeeded.add(uid)
        else:
            # Normal workflow: all expected tasks must complete
            succeeded = {
                uid for uid in workflow_uids  # Only check UIDs that have workflow entries
                if expected_task_count > 0 and completed_counts.get(uid, 0) >= expected_task_count
            }
        
        # UIDs with checkpoints but no workflow entries are definitely incomplete
        incomplete_from_checkpoints = checkpoint_uids - workflow_uids

        computed_success_rate = (len(succeeded) / len(all_uids)) if all_uids else 0.0
        if current_success_rate is None:
            current_success_rate = computed_success_rate
        failure_rate_hint = max(0.0, 1.0 - current_success_rate)
        if normalized_cost is None:
            cost_per_attempt = get_recent_cost_per_attempt(workflow, baseline)
            normalized_cost = cost_per_attempt * 3600 / COST_BASELINE_PER_POD_HOUR if cost_per_attempt and cost_per_attempt > 0 else 1.0
        target_success = get_target_success_rate(workflow)
        pareto_bias = should_pareto_bias_retries(current_success_rate, target_success, normalized_cost)
        if pareto_bias:
            print(f"   🧮 Pareto retry bias active: favoring retries (success={current_success_rate*100:.1f}%≥{target_success*100:.1f}%+{PARETO_SUCCESS_MARGIN*100:.1f}%, "
                  f"norm_cost={normalized_cost:.2f}≥{PARETO_COST_NORM_THRESHOLD:.2f})")
        
        max_retries = get_max_retries_for_workflow(
            workflow, baseline, current_success_rate, expected_tasks,
            normalized_cost=normalized_cost, failure_rate_hint=failure_rate_hint
        )
        
        # Use index for over-retried check
        query = "SELECT uid FROM checkpoint_retries USE INDEX (idx_cr_wb_retry) WHERE workflow_name=%s AND baseline=%s AND retry_count >= %s"
        params = [workflow, baseline, max_retries]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        over_retried = {uid for (uid,) in cursor.fetchall() if uid}

        # Get retry counts and last attempt times for priority calculation
        query = """
            SELECT uid, retry_count, last_attempt_at FROM checkpoint_retries USE INDEX (idx_cr_uid_wb)
             WHERE workflow_name=%s AND baseline=%s
        """
        params = [workflow, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        retry_info = {uid: (retry_count, last_attempt_at) 
                     for uid, retry_count, last_attempt_at in cursor.fetchall()}

        # Use index for cooldown check with adaptive cooldown
        # For push-and-forget, use much shorter cooldown since we're not waiting for execution
        # The task pod handles execution, so we can retry more frequently
        cooling = set()
        stale_uids = set()
        if ENABLE_ADAPTIVE_RETRY:
            # Python runs in UTC; DB stores last_attempt_at in EDT → convert for comparison
            now_utc = datetime.now(timezone.utc)
            for uid, (retry_count, last_attempt_at) in retry_info.items():
                if last_attempt_at:
                    last_utc = _last_attempt_at_to_utc(last_attempt_at)
                    # For push-and-forget: use much shorter cooldown
                    # Longest workflows (wf-4, wf-8): retry service is main character - use minimal cooldown
                    if True:
                        # For wf-4 and wf-8: even shorter cooldown (2-4 seconds) since retry service is primary
                        base_push_forget_cooldown = min(4, 2 + retry_count)  # 2-4 seconds max for longest workflows
                    else:
                        # For other workflows: standard push-and-forget cooldown
                        base_push_forget_cooldown = min(10, 5 + retry_count)  # 5-10 seconds max
                    
                    # Apply semantic grouping cooldown multiplier (if available)
                    # Note: semantic grouping is calculated later, so we use base cooldown here
                    # The actual semantic cooldown is applied in the retry loop
                    push_forget_cooldown = base_push_forget_cooldown
                    if pareto_bias:
                        push_forget_cooldown = max(1, int(push_forget_cooldown * PARETO_COOLDOWN_FACTOR))
                    if retry_lead_mode and retry_lead_cooldown_factor < 1.0:
                        push_forget_cooldown = max(1, int(push_forget_cooldown * retry_lead_cooldown_factor))
                    
                    time_since_attempt = (now_utc - last_utc).total_seconds()
                    if time_since_attempt > STALE_UID_THRESHOLD_SEC:
                        stale_uids.add(uid)
                        continue
                    if time_since_attempt < push_forget_cooldown:
                        cooling.add(uid)
        else:
            # For push-and-forget, use shorter cooldown (10 seconds instead of 60)
            PUSH_FORGET_COOLDOWN = 10  # Much shorter since we're not waiting for execution
            query = """
                SELECT uid FROM checkpoint_retries USE INDEX (idx_cr_wb_time)
                 WHERE workflow_name=%s AND baseline=%s
                   AND last_attempt_at IS NOT NULL
                   AND last_attempt_at > (NOW() - INTERVAL %s SECOND)
            """
            params = [workflow, baseline, PUSH_FORGET_COOLDOWN]
            if EXPERIMENT_TAG:
                query += " AND notes = %s"
                params.append(EXPERIMENT_TAG)
            cursor.execute(query, tuple(params))
            cooling = {uid for (uid,) in cursor.fetchall() if uid}

        # Candidates include:
        # 1. UIDs with workflow entries but not all tasks completed
        # 2. UIDs with checkpoints but no workflow entries (definitely incomplete)
        candidates = []
        for uid in all_uids:
            if uid in succeeded:
                continue  # Skip succeeded UIDs
            if uid in over_retried:
                continue  # Skip over-retried UIDs
            if uid in cooling:
                continue  # Skip UIDs in cooldown
            if uid in stale_uids:
                continue  # Skip stale UIDs from prior runs
            # Include UID if it has checkpoints (even without workflow entries) or incomplete workflow
            if uid in incomplete_from_checkpoints or uid in workflow_uids:
                candidates.append(uid)
        
        # Store incomplete_from_checkpoints count for logging
        incomplete_checkpoint_only = len(incomplete_from_checkpoints)
        if stale_uids:
            print(f"   🧹 Filtered {len(stale_uids)} stale UIDs (>{STALE_UID_THRESHOLD_SEC/3600:.1f}h since last attempt)")
    
    target_success_rate = get_target_success_rate(workflow)
    distance_from_target = (target_success_rate - current_success_rate) / target_success_rate if target_success_rate > 0 else 0

    # Checkpointer-aware retry (checkscale): only prioritize/retry from checkpointer-selected tasks when success is OK
    checkpointer_selected = get_checkpointer_selected_tasks(workflow, baseline)
    success_above_threshold = (current_success_rate is not None and
        current_success_rate >= target_success_rate - SUCCESS_RATE_THRESHOLD_FOR_NON_SELECTED)
    only_selected_ckpt = bool(checkpointer_selected) and success_above_threshold
    if checkpointer_selected and only_selected_ckpt:
        print(f"   📌 Checkpointer-aware: success={current_success_rate*100:.1f}% above threshold → retry only from selected tasks ({len(checkpointer_selected)} tasks)")
    elif checkpointer_selected and not success_above_threshold:
        print(f"   📌 Checkpointer-aware: success={current_success_rate*100:.1f}% below threshold → also retry from non-selected checkpoints (selected first)")
    
    # Calculate priority scores for candidates
    priority_queue = []
    # Priority based on workflow type and target
    # Complex workflows (wf-4,6,8,9): Target 65-70% - retry service is PRIMARY recovery mechanism
    # Simple workflows (wf-1,2,3,4,7): Target 95-100% - high priority for high targets
    if True:
        base_priority = 2000  # Highest priority for ALL complex workflows (retry service is primary)
    elif False:
        base_priority = 1800  # High priority for simple workflows (target 97.5%)
    else:
        base_priority = 500   # Standard priority for unknown workflows
    if retry_lead_mode and retry_lead_priority_boost:
        base_priority += retry_lead_priority_boost
    
    # Boost priority for workflows below target success rate
    # Cost optimization: reduce priority when success rate is above target
    if current_success_rate < target_success_rate:
        target_boost = int(distance_from_target * 500)  # Up to 500 point boost
        base_priority += target_boost
    elif current_success_rate > target_success_rate * 1.05:
        # Above target by >5%: reduce priority to save costs (retry less aggressively)
        excess_success = (current_success_rate - target_success_rate) / target_success_rate
        priority_reduction = int(excess_success * 300)  # Up to 300 point reduction
        base_priority = max(base_priority - priority_reduction, base_priority * 0.7)  # Don't reduce below 70% of base
        print(f"   💰 Cost optimization: reducing priority for {workflow} by {priority_reduction} points (success={current_success_rate*100:.1f}%>{target_success_rate*100:.1f}%)")
    
    # Load tasks data for semantic grouping (for longest workflows and workflows needing aggressive retries)
    # wf-2, wf-3, wf-5, wf-7: Need semantic grouping for better retry strategies to beat baselines by 5%+
    tasks_data_for_semantic = None
    workflows_needing_semantic = set()  # all workflows use semantic grouping
    if workflow in workflows_needing_semantic:
        tasks_data_for_semantic = load_workflow_tasks(workflow)
    
    # Burst failure detection: if many UIDs failed recently, prioritize them
    recent_failure_count = 0
    if ENABLE_ADAPTIVE_RETRY:
        now_utc = datetime.now(timezone.utc)  # Python is UTC; DB stores EDT
        for uid in candidates:
            retry_count, last_attempt_at = retry_info.get(uid, (0, None))
            if last_attempt_at:
                last_utc = _last_attempt_at_to_utc(last_attempt_at)
                time_since = (now_utc - last_utc).total_seconds()
                if time_since < 60:  # Failed within last minute
                    recent_failure_count += 1
    
    # Burst mode: if >20% of candidates failed in last minute, we're in burst failure mode
    burst_mode = len(candidates) > 0 and recent_failure_count > len(candidates) * 0.20
    
    for uid in candidates:
        retry_count, last_attempt_at = retry_info.get(uid, (0, None))
        completed_count = completed_counts.get(uid, 0)
        
        # Get latest checkpoint for this UID (used for semantic group and checkscale filtering)
        checkpoint_result = get_latest_checkpoint(uid, workflow, baseline, limit=1)
        checkpoint_task = None
        if isinstance(checkpoint_result, tuple) and len(checkpoint_result) >= 1:
            checkpoint_task = checkpoint_result[0]

        # Checkpointer-aware (checkscale): only retry from selected tasks when success is above threshold
        is_selected_ckpt = (checkpoint_task in checkpointer_selected) if checkpointer_selected else True
        if only_selected_ckpt and not is_selected_ckpt:
            continue  # Skip UIDs whose last checkpoint is not checkpointer-selected when success is OK
        
        # Get checkpoint semantic group for priority boost
        semantic_boost = 0
        checkpoint_group = 'middle'
        if tasks_data_for_semantic and checkpoint_task:
            checkpoint_group = get_checkpoint_semantic_group(tasks_data_for_semantic, checkpoint_task)
            strategy = get_semantic_retry_strategy(checkpoint_group, workflow)
            semantic_boost = strategy.get('priority_boost', 0)
        
        # Priority factors:
        # 1. Fewer retries = higher priority (retry early, fail fast) - subtract retry_count * 10
        # 2. More completed tasks = higher priority (closer to completion) - add completed_count * 5
        # 3. Recent failure = higher priority (fresh errors more likely to succeed) - add recency bonus
        # 4. Below target success rate = higher priority (boost from base_priority)
        # 5. For longest workflows: massive boost for UIDs very close to completion (e.g., 8/9 tasks done)
        # 6. Semantic grouping boost: late checkpoints get highest priority
        # 7. Burst failure mode: boost all recent failures
        priority = base_priority
        priority -= retry_count * 10  # Fewer retries = higher priority
        priority += completed_count * 5  # More completed = higher priority
        priority += min(completed_count, 20)  # Cap completion bonus
        priority += semantic_boost  # Semantic grouping boost
        
        # Burst failure mode: boost recent failures significantly
        if burst_mode and last_attempt_at:
            time_since = (datetime.now() - last_attempt_at).total_seconds()
            if time_since < 60:  # Failed within last minute
                priority += 300  # Large boost for burst failures
        
        # For longest workflows: massive priority boost for UIDs very close to completion
        # This ensures we prioritize finishing workflows that are almost done
        if True and expected_task_count > 0:
            completion_ratio = completed_count / expected_task_count
            if completion_ratio >= 0.80:  # 80%+ complete (e.g., 8/9 or 9/9 tasks)
                # Massive boost: up to 1600 points for UIDs very close to completion
                near_completion_boost = int((completion_ratio - 0.80) * 8000)  # 0-1600 points
                priority += near_completion_boost
                if completion_ratio >= 0.90:  # 90%+ complete
                    priority += 800  # Additional boost for 90%+ complete
        
        # Recency bonus: if failed recently (within last 5 minutes), boost priority
        if last_attempt_at:
            time_since = (datetime.now() - last_attempt_at).total_seconds()
            if time_since < 300:  # Within 5 minutes
                priority += 50  # Recency bonus
        
        # Checkpointer-selected priority boost (checkscale): boost UIDs whose last checkpoint is at a selected task
        if checkpointer_selected and is_selected_ckpt:
            priority += CHECKPOINTER_SELECTED_PRIORITY_BOOST
        
        heapq.heappush(priority_queue, (-priority, uid, checkpoint_group))  # Negative for max-heap, include group
    
    # Extract sorted by priority (highest first)
    prioritized = [(-score, uid, group) for score, uid, group in priority_queue]
    prioritized.sort(reverse=True)  # Highest priority first
    
    # Debug logging
    if len(all_uids) > 0 or len(candidates) > 0:
        target_success = get_target_success_rate(workflow)
        current_success = current_success_rate
        # Note: dynamic max retries are computed per-UID later in the loop
        if expected_task_count:
            check_type = "all expected tasks" if all_expected_tasks else "final tasks"
            print(f"   Completion check using: {check_type} ({expected_task_count} tasks)")
        if len(prioritized) > 0:
            top_3 = prioritized[:3]
            print(f"   Top 3 priority UIDs: {[(score, uid[:20]+'...', group) for score, uid, group in top_3]}")
        if len(all_uids) > 0 and len(candidates) == 0:
            print(f"   ⚠️  All UIDs filtered out! Sample UIDs: {list(all_uids)[:5]}")
            print(f"   ⚠️  Succeeded UIDs: {len(succeeded)} (sample: {list(succeeded)[:3] if succeeded else 'none'})")
            print(f"   ⚠️  Over-retried UIDs: {len(over_retried)}")
            print(f"   ⚠️  Cooling down UIDs: {len(cooling)}")
            if len(succeeded) == len(all_uids):
                print(f"   ℹ️  All UIDs appear to have succeeded")
            elif len(succeeded) == 0 and expected_task_count:
                print(f"   ⚠️  No UIDs succeeded - this suggests task name mismatch or no successful tasks")
    elif len(all_uids) == 0:
        print(f"📊 [{workflow}] No UIDs found in database for baseline={baseline}")
    
    return prioritized

def get_completed_tasks(uid: str, workflow_name: str, baseline: str) -> Set[str]:
    """Get completed tasks for a UID - optimized query with proper index usage."""
    with mysql_connection() as conn:
        cursor = conn.cursor()
        # Use index hint for better performance on indexed columns
        query = """
            SELECT workflow_stage FROM serverless_workflows USE INDEX (idx_swv_uid_wb)
             WHERE uuid_passed=%s AND workflow_name=%s AND baseline=%s AND response_code=200
        """
        params = [uid, workflow_name, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        return {task for (task,) in cursor.fetchall()}

def get_latest_checkpoint(uid: str, workflow_name: str, baseline: str, limit: int = 1) -> Tuple[Any, Any]:
    """Get latest checkpoint(s) from MySQL - optimized with index hints.
    
    Args:
        uid: Workflow instance UID
        workflow_name: Workflow name
        baseline: Baseline name
        limit: Number of checkpoints to return (default 1 for latest only)
    
    Returns:
        If limit=1: (task_id, result) tuple or (None, None) if no checkpoint
        If limit>1: List of (task_id, result) tuples, ordered by timestamp DESC
    """
    with mysql_connection() as conn:
        cursor = conn.cursor()
        # Use composite index for faster lookup
        query = """
            SELECT task_id, result FROM task_checkpoints USE INDEX (idx_tc_uwb_ts)
             WHERE uid=%s AND workflow_name=%s AND baseline=%s
        """
        params = [uid, workflow_name, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

    if not rows:
        return (None, None) if limit == 1 else []
    
    if limit == 1:
        # Single checkpoint: return as tuple
        task_id_val, result_val = rows[0]
        if isinstance(result_val, (bytes, bytearray)):
            try: result_val = result_val.decode("utf-8")
            except Exception: pass
        if isinstance(result_val, str):
            try: result_val = json.loads(result_val)
            except Exception: pass
        return task_id_val, result_val
    else:
        # Multiple checkpoints: return as list
        checkpoints = []
        for row in rows:
            task_id_val, result_val = row
            if isinstance(result_val, (bytes, bytearray)):
                try: result_val = result_val.decode("utf-8")
                except Exception: pass
            if isinstance(result_val, str):
                try: result_val = json.loads(result_val)
                except Exception: pass
            checkpoints.append((task_id_val, result_val))
        return checkpoints

def calculate_adaptive_cooldown(retry_count: int, base_cooldown: int, workflow_name: str = None,
                                current_success_rate: float = None) -> int:
    """Calculate adaptive cooldown with exponential backoff and jitter.
    
    Adjusts cooldown based on distance from target success rate:
    - If below target: reduce cooldown (faster retries, up to 30% reduction)
    - If at or above target: use standard cooldown
    
    For longest workflows (wf-4, wf-8), uses even more aggressive settings:
    - Much shorter backoff times (10s min, 3min max)
    - Retry service is the primary recovery mechanism
    
    Args:
        retry_count: Current retry attempt number (0-indexed)
        base_cooldown: Base cooldown time in seconds
        workflow_name: Optional workflow name for complex workflow-specific settings
        current_success_rate: Optional current success rate (for dynamic adjustment)
    
    Returns:
        Adaptive cooldown time in seconds
    """
    if not ENABLE_ADAPTIVE_RETRY:
        return base_cooldown
    
    min_backoff = WF_MIN_BACKOFF_SEC
    max_backoff = WF_MAX_BACKOFF_SEC
    backoff_multiplier = WF_BACKOFF_MULTIPLIER
    
    # Exponential backoff: base * (multiplier ^ retry_count)
    backoff = base_cooldown * (backoff_multiplier ** retry_count)
    
    # Adjust cooldown based on distance from target success rate
    if current_success_rate is not None and workflow_name:
        target_success = get_target_success_rate(workflow_name)
        if current_success_rate < target_success:
            # Below target: reduce cooldown (faster retries)
            distance_from_target = (target_success - current_success_rate) / target_success
            reduction_factor = 1.0 - (distance_from_target * 0.5)
            backoff = backoff * reduction_factor
        elif current_success_rate > target_success * 1.05:
            # Above target by >5%: increase cooldown to save costs (slower retries)
            excess_success = (current_success_rate - target_success) / target_success
            increase_factor = 1.0 + (excess_success * 0.5)  # Up to 50% increase
            backoff = backoff * increase_factor
            print(f"   💰 Cost optimization: increasing cooldown for {workflow_name} by {(increase_factor-1.0)*100:.0f}% (success={current_success_rate*100:.1f}%>{target_success*100:.1f}%)")
    
    backoff = backoff * 1.1  # 10% buffer to account for VM performance variance
    
    # Apply min/max bounds (workflow-specific)
    backoff = max(min_backoff, min(backoff, max_backoff))
    
    # Add jitter (±20%) to avoid thundering herd
    jitter_range = backoff * BACKOFF_JITTER
    jitter = random.uniform(-jitter_range, jitter_range)
    
    adaptive_cooldown = int(backoff + jitter)
    return max(min_backoff, adaptive_cooldown)

def get_retry_count(uid: str, workflow_name: str, baseline: str) -> int:
    """Get current retry count for a UID."""
    with mysql_connection() as conn:
        cursor = conn.cursor()
        query = "SELECT retry_count FROM checkpoint_retries WHERE uid=%s AND workflow_name=%s AND baseline=%s"
        params = [uid, workflow_name, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        row = cursor.fetchone()
        return row[0] if row else 0

def bump_retry(uid: str, workflow_name: str, baseline: str, task_id: str, status: int):
    """Bump retry count - optimized with single statement."""
    with mysql_connection() as conn:
        cursor = conn.cursor()
        # Single INSERT...ON DUPLICATE KEY UPDATE is efficient
        experiment_tag = os.getenv("EXPERIMENT_TAG", "main_experiment")
        cursor.execute(
            """
            INSERT INTO checkpoint_retries
                    (uid, workflow_name, baseline, retry_count, last_task_id, last_status, last_attempt_at, notes)
            VALUES  (%s,  %s,            %s,       1,           %s,          %s,          NOW(), %s)
            ON DUPLICATE KEY UPDATE
              retry_count   = retry_count + 1,
              last_task_id  = VALUES(last_task_id),
              last_status   = VALUES(last_status),
              last_attempt_at = NOW(),
              notes = VALUES(notes)
            """,
            (uid, workflow_name, baseline, task_id, status, experiment_tag)
        )
        conn.commit()

def clear_retry_state(uid: str, workflow_name: str, baseline: str):
    """Remove retry tracking for a UID once it succeeds."""
    with mysql_connection() as conn:
        cursor = conn.cursor()
        query = "DELETE FROM checkpoint_retries WHERE uid=%s AND workflow_name=%s AND baseline=%s"
        params = [uid, workflow_name, baseline]
        if EXPERIMENT_TAG:
            query += " AND notes = %s"
            params.append(EXPERIMENT_TAG)
        cursor.execute(query, tuple(params))
        conn.commit()

# Redis ingestion removed - all data now goes directly to MySQL

# -------------------------
# Retry
# -------------------------

# Thread pool for parallel retry requests
# RETRY SERVICE IS THE HERO - large thread pool for maximum throughput
# Configurable via env RETRY_THREAD_POOL_SIZE
RETRY_THREAD_POOL_SIZE = int(os.getenv('RETRY_THREAD_POOL_SIZE', '50'))  # Process up to 50 retries concurrently (was 25)

def retry_from_checkpoint(task_id: str, checkpoint: Any, fn_url: str,
                          uid: str, workflow_name: str, baseline: str) -> int:
    """
    Push-and-forget retry: Send retry request to task pod and let it handle the retry.
    The task pod will receive the request and execute the retry functionality.
    We use a very short timeout just to ensure the request was sent successfully.
    
    IMPORTANT: This is truly fire-and-forget with a 2-second timeout.
    We don't wait for the task to complete - just verify the request was sent.
    """
    data = {"uid": uid, "workflow_name": workflow_name, "baseline": baseline}
    if checkpoint is not None:
        data["checkpoint"] = checkpoint

    # Push-and-forget: VERY short timeout - just enough to establish TCP connection
    # The task pod will handle the actual retry execution asynchronously
    # REDUCED from 15s to 2s to prevent blocking the retry service
    SEND_TIMEOUT = 2  # 2 seconds - just enough to verify connection was established
    
    try:
        # Send request with short timeout - we just want to ensure it was sent
        # The task pod will handle the retry execution asynchronously
        resp = session.post(
            fn_url, json=data, timeout=SEND_TIMEOUT,
            headers={'Content-Type': 'application/json'}
        )
        
        # For push-and-forget, we're lenient about HTTP status codes
        # - 200: Request accepted and task executing
        # - 202: Request accepted (even if we didn't wait)
        # - 500: Pod might be starting up, but request might be queued - treat as "sent" for push-and-forget
        # - Other errors: Log but still consider it "sent" (task pod will handle)
        status_code = int(resp.status_code)
        
        if status_code == 200:
            print(f"📤 Pushed retry request to {task_id} for UID {uid}: HTTP {status_code} (task pod will handle execution)")
            return status_code
        elif status_code >= 500:
            # HTTP 500 might mean pod is starting up, but request might still be queued
            # For push-and-forget, we consider this as "sent" - the pod will handle it when ready
            print(f"📤 Pushed retry request to {task_id} for UID {uid}: HTTP {status_code} (pod may be starting, request queued)")
            return 202  # Treat as accepted
        else:
            # Other status codes (4xx, etc.) - log but still consider sent
            print(f"📤 Pushed retry request to {task_id} for UID {uid}: HTTP {status_code} (task pod will handle)")
            return status_code
        
    except requests.Timeout:
        # Timeout on send - this is OK for push-and-forget, request may have been queued
        print(f"📤 Pushed retry request to {task_id} for UID {uid}: request sent (timeout on response, task pod will handle)")
        return 202  # Accepted (even though we didn't wait for response)
        
    except requests.RequestException as e:
        # Network error - for push-and-forget, we're more lenient
        # The error might be transient (pod starting, network hiccup)
        # Log but don't fail immediately - will retry in next cycle
        print(f"⚠️ Network error sending retry request to {task_id} for UID {uid}: {e} (will retry in next cycle)")
        return -1


def batch_retry_requests(retry_jobs: List[Dict]) -> Dict[str, int]:
    """
    Process multiple retry requests in parallel using a thread pool.
    
    This significantly speeds up retry processing by sending requests concurrently
    instead of waiting for each one sequentially.
    
    Args:
        retry_jobs: List of dicts with keys: uid, task_id, checkpoint, fn_url, workflow_name, baseline
    
    Returns:
        Dict mapping uid -> status_code
    """
    results = {}
    
    if not retry_jobs:
        return results
    
    def process_single_retry(job):
        """Worker function for thread pool."""
        uid = job['uid']
        task_id = job['task_id']
        checkpoint = job.get('checkpoint')
        fn_url = job['fn_url']
        workflow_name = job['workflow_name']
        baseline = job['baseline']
        
        status = retry_from_checkpoint(task_id, checkpoint, fn_url, uid, workflow_name, baseline)
        return uid, status
    
    # Use thread pool for parallel processing
    # Limit concurrency to avoid overwhelming the system
    num_jobs = len(retry_jobs)
    max_workers = min(RETRY_THREAD_POOL_SIZE, num_jobs)
    utilization_pct = 100.0 * max_workers / RETRY_THREAD_POOL_SIZE if RETRY_THREAD_POOL_SIZE else 0
    print(f"📊 Thread pool: using {max_workers}/{RETRY_THREAD_POOL_SIZE} threads for {num_jobs} retries ({utilization_pct:.0f}% utilization)")
    
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_uid = {executor.submit(process_single_retry, job): job['uid'] for job in retry_jobs}
        
        # Collect results as they complete
        for future in as_completed(future_to_uid):
            uid = future_to_uid[future]
            try:
                result_uid, status = future.result()
                results[result_uid] = status
            except Exception as e:
                print(f"⚠️ Exception processing retry for {uid}: {e}")
                results[uid] = -1
    
    elapsed = time.time() - t0
    print(f"📊 Thread pool: completed {len(results)} retries in {elapsed:.2f}s")
    return results


# -------------------------
# Main
# -------------------------
def main():
    global COOLDOWN_SEC, POLL_INTERVAL

    parser = argparse.ArgumentParser(description="Checkpoint-based retry service (SQL-only, DAG backtracking)")
    # Allow baseline and workflow to be set via environment variables (for Kubernetes ConfigMap)
    baseline_default = os.getenv("BASELINE")
    workflow_default = os.getenv("WORKFLOW_FILTER") or os.getenv("WORKFLOW")
    
    parser.add_argument("--baseline", required=not bool(baseline_default),
                       default=baseline_default,
                       help="Baseline name (matches what tasks write). Can also be set via BASELINE env var.")
    parser.add_argument("--workflow", default=workflow_default,
                       help="If set, limit to this workflow only (e.g., wf-4). Can also be set via WORKFLOW_FILTER or WORKFLOW env var.")
    parser.add_argument("--cooldown", type=int, default=COOLDOWN_SEC, help="Per-UID cooldown seconds")
    parser.add_argument("--poll", type=int, default=POLL_INTERVAL, help="Polling interval seconds")
    parser.add_argument("--clear", action="store_true", help="TRUNCATE checkpoint_retries at start (default: False, do not clear)")
    args = parser.parse_args()
    
    # Validate baseline is set (either via CLI or env var)
    if not args.baseline:
        parser.error("--baseline is required (can be set via BASELINE environment variable)")

    COOLDOWN_SEC = int(args.cooldown)
    POLL_INTERVAL = int(args.poll)

    # By default, do NOT clear checkpoint_retries table (preserve retry history)
    # Only clear if --clear flag is explicitly provided
    setup_tables(clear=args.clear)
    
    # Initialize MySQL connection pool for better performance
    init_pool()
    
    # Reset retry efficiency tracker at start
    reset_retry_efficiency_tracker()
    print(f"🔄 Retry efficiency tracker reset (max {MAX_RETRIES_PER_CYCLE} per cycle, {MAX_TOTAL_RETRIES_BUDGET} total budget)")

    if args.workflow:
        workflow_names = [args.workflow]
    else:
        workflow_names = sorted([wf for wf in os.listdir(WORKFLOW_DIR) if wf.startswith('wf-')])

    print("Detected workflows:", workflow_names)
    print("WORKFLOW_DIR:", os.path.abspath(WORKFLOW_DIR))
    print("Operating baseline:", args.baseline)
    print(f"MAX_RETRIES_PER_UID={MAX_RETRIES_PER_UID} (default), COOLDOWN_SEC={COOLDOWN_SEC}, POLL_INTERVAL={POLL_INTERVAL}")
    print(f"🚀 Retry performance: batch={MAX_RETRIES_PER_CYCLE}/cycle, thread_pool={RETRY_THREAD_POOL_SIZE} (set RETRY_MAX_RETRIES_PER_CYCLE, RETRY_THREAD_POOL_SIZE to override)")
    if ENABLE_ADAPTIVE_RETRY:
        print(f"✅ Adaptive retry: backoff {WF_BACKOFF_MULTIPLIER}x, jitter {BACKOFF_JITTER*100:.0f}%, range [{WF_MIN_BACKOFF_SEC}s-{WF_MAX_BACKOFF_SEC}s]")
    print(f"🎯 Target success rate: {WF_TARGET_SUCCESS_RATE*100:.1f}%  |  Max retries: {WF_MAX_RETRIES}")
    print(f"✅ Dynamic retry limits: Adjusts based on distance from target (up to +50% boost)")
    print(f"✅ Dynamic cooldown: Adjusts based on distance from target (up to 30% reduction)")
    print("✅ Using SQL-only mode (no Redis layer)")

    dag_checked = set()

    while True:

        for workflow in workflow_names:
            try:
                tasks_data = load_workflow_tasks(workflow)
            except Exception as e:
                print(f"❌ Could not load tasks for {workflow}: {e}")
                continue

            if workflow not in dag_checked:
                verify_dag_once(tasks_data, workflow)
                dag_checked.add(workflow)

            finals = find_final_tasks(tasks_data)
            print(f"🔎 {workflow}: Final tasks = {finals or '[]'}")
            
            # Get all expected tasks for better completion detection
            all_expected_tasks = set(tasks_data.keys()) if tasks_data else None
            # Compute current success rate and cost signal for this workflow
            current_success_rate = get_workflow_success_rate(workflow, args.baseline, all_expected_tasks)
            cost_per_attempt = get_recent_cost_per_attempt(workflow, args.baseline)
            normalized_cost = cost_per_attempt * 3600 / COST_BASELINE_PER_POD_HOUR if cost_per_attempt and cost_per_attempt > 0 else 1.0
            estimated_roi = estimate_scaler_roi(workflow, args.baseline, current_success_rate)
            retry_lead_active, retry_lead_reason, retry_lead_policy = get_retry_lead_policy(
                workflow, current_success_rate, normalized_cost, estimated_roi
            )

            prioritized_uids = get_uids_incomplete_workflows(
                workflow, finals, args.baseline, all_expected_tasks,
                current_success_rate=current_success_rate,
                normalized_cost=normalized_cost,
                retry_lead_mode=retry_lead_active,
                retry_lead_cooldown_factor=retry_lead_policy.get("cooldown_factor", 1.0),
                retry_lead_priority_boost=int(retry_lead_policy.get("priority_boost", 0))
            )
            if retry_lead_active:
                print(f"   🚦 RETRY-LEAD: {retry_lead_reason} (roi={estimated_roi:.2f}, norm_cost={normalized_cost:.2f})")
            
            # Track retries for efficiency monitoring
            retries_this_cycle = 0
            
            if not prioritized_uids:
                print(f"[{workflow}] No incomplete UIDs to retry.")
                continue
            else:
                print(f"[{workflow}] Found {len(prioritized_uids)} incomplete UID(s) to retry (prioritized).")
                # For longest workflows, retry service is the main character - log this
                if True:
                    print(f"   🎯 {workflow} is retry-service-driven: using aggressive retry policy (max_retries={WF_MAX_RETRIES})")
            
            # Extract UIDs from prioritized list (score, uid, group) tuples
            # Process multiple UIDs in parallel batches to avoid blocking
            uids = [uid for _, uid, _ in prioritized_uids]
            # Also extract checkpoint groups for semantic retry strategies
            uid_to_group = {uid: group for _, uid, group in prioritized_uids}
            
            # =========================================================
            # STATE MACHINE: Apply state-machine-based prioritization
            # =========================================================
            # Check for stalled workflows and update state machine
            stalled_instances = check_stalled_workflows(workflow, args.baseline)
            if stalled_instances:
                stalled_uids = [inst.uid for inst in stalled_instances]
                print(f"   ⚠️ STATE-MACHINE: Detected {len(stalled_instances)} stalled workflow instances")
                for inst in stalled_instances:
                    should_abandon, reason = should_abandon_workflow(inst)
                    if should_abandon:
                        print(f"      🛑 Abandoning {inst.uid}: {reason}")
                        inst.mark_abandoned(reason)
            
            # Re-prioritize using state machine scoring
            state_prioritized = get_retry_priority_queue(workflow, args.baseline, uids)
            if state_prioritized:
                # Replace with state-machine ordered UIDs
                uids = [uid for _, uid in state_prioritized]
                print(f"   🔄 STATE-MACHINE: Re-prioritized {len(uids)} UIDs by state machine score")

            # =========================================================
            # ROI-AWARE RETRY BOOST
            # =========================================================
            # Check if scaler has negative ROI and retry service should lead
            roi_should_boost, roi_boost_factor = should_retry_boost_for_negative_roi(
                workflow, args.baseline, current_success_rate, estimated_roi=estimated_roi
            )

            # Adaptive signals reused inside the per-UID loop
            failure_rate_hint = max(0.0, 1.0 - current_success_rate)
            
            # Determine max UIDs based on workflow type and success rate
            # Priority is SUCCESS RATE - be aggressive when below target
            target_success = get_target_success_rate(workflow)
            pareto_bias = should_pareto_bias_retries(current_success_rate, target_success, normalized_cost)
            
            # RETRY SERVICE IS THE HERO - very large batch sizes
            if roi_should_boost:
                # ROI-AWARE: Scaler is ineffective, retry service leads with MASSIVE batch
                effective_max_uids = min(int(150 * roi_boost_factor), MAX_RETRIES_PER_CYCLE)  # Was 100
                if retry_lead_active:
                    effective_max_uids = min(int(effective_max_uids * retry_lead_policy.get("max_uids_factor", 1.0)), MAX_RETRIES_PER_CYCLE)
                print(f"   📈 ROI-BOOST: Expanding to {effective_max_uids} UIDs this cycle (ROI boost={roi_boost_factor:.1f}x) - RETRY IS HERO")
            elif pareto_bias:
                # Scaling is cost-inefficient; retry service leads with LARGE batch
                effective_max_uids = min(120, MAX_RETRIES_PER_CYCLE)  # Was 80
                print(f"   🧮 Pareto retry bias: expanding to {effective_max_uids} UIDs this cycle - RETRY IS HERO")
            elif current_success_rate >= target_success * 1.1:
                # Well above target (10%+): can be more conservative
                effective_max_uids = min(60, MAX_RETRIES_PER_CYCLE // 2)  # Was 40
                print(f"   💰 Success above target ({current_success_rate*100:.1f}%>{target_success*100:.1f}%): limiting to {effective_max_uids} UIDs")
            elif True:
                # Complex workflows (wf-4,6,8,9): RETRY SERVICE IS THE HERO
                # Scaler is throttled, so retry service must handle most recovery
                effective_max_uids = 140  # Very high limit (was 100) - retry service is hero
            else:
                # Simple workflows: larger batch size since scaler is throttled
                effective_max_uids = min(80, MAX_RETRIES_PER_CYCLE)  # Was 50
            
            if retry_lead_active and not roi_should_boost:
                original_max = effective_max_uids
                effective_max_uids = min(int(effective_max_uids * retry_lead_policy.get("max_uids_factor", 1.0)),
                                         MAX_RETRIES_PER_CYCLE)
                if effective_max_uids > original_max:
                    print(f"   🚦 RETRY-LEAD: expanding to {effective_max_uids} UIDs this cycle")
            
            uids_to_process = uids[:effective_max_uids]
            if len(uids) > effective_max_uids:
                print(f"   ℹ️ Processing {effective_max_uids} of {len(uids)} UIDs this cycle (will continue in next cycle)")

            for uid in uids_to_process:
                # Batch database queries to reduce blocking
                # Get checkpoint and completed tasks in parallel where possible
                checkpoint_result = get_latest_checkpoint(uid, workflow, args.baseline, limit=1)
                candidate_task, checkpoint = checkpoint_result if isinstance(checkpoint_result, tuple) else (None, None)
                completed = get_completed_tasks(uid, workflow, args.baseline)
                
                # Get completed count for this UID to calculate adaptive retry limit
                # completed_counts is defined in get_uids_incomplete_workflows, but we need to query it here
                completed_count_for_uid = len(completed) if completed else 0
                
                # Get max retries with completion boost for longest workflows
                max_retries_with_boost = get_max_retries_for_workflow(
                    workflow, args.baseline, current_success_rate, 
                    all_expected_tasks, completed_count_for_uid,
                    normalized_cost=normalized_cost, failure_rate_hint=failure_rate_hint
                )
                
                # Apply ROI-based boost if scaler is ineffective
                if roi_should_boost:
                    original_max_retries = max_retries_with_boost
                    max_retries_with_boost = int(max_retries_with_boost * roi_boost_factor)
                    if max_retries_with_boost > original_max_retries:
                        print(f"   📈 ROI-BOOST: {uid} max_retries {original_max_retries} → {max_retries_with_boost} (scaler ROI low)")
                
                # For longest workflows: if latest checkpoint retry has failed multiple times,
                # try earlier checkpoints as fallback
                retry_count = get_retry_count(uid, workflow, args.baseline)
                earlier_checkpoints = None
                if True and retry_count >= 5 and checkpoint:
                    # After 5 failed retries, try getting multiple checkpoints as fallback
                    earlier_checkpoints = get_latest_checkpoint(uid, workflow, args.baseline, limit=3)
                    if isinstance(earlier_checkpoints, list) and len(earlier_checkpoints) > 1:
                        print(f"   🔄 UID {uid}: {retry_count} retries failed, will try {len(earlier_checkpoints)} checkpoints as fallback")
                
                # Check if UID has exceeded retry limit (with completion boost)
                if retry_count >= max_retries_with_boost:
                    print(f"⏸️  UID {uid}: exceeded retry limit ({retry_count}/{max_retries_with_boost}, boosted from {WF_MAX_RETRIES} due to {completed_count_for_uid}/{len(all_expected_tasks) if all_expected_tasks else '?'} tasks completed)")
                    continue

                # STATE MACHINE: Get/create workflow instance for this UID
                wf_instance = get_or_create_workflow_instance(workflow, args.baseline, uid)
                
                # STATE MACHINE: Update completed tasks in instance
                for task_id in completed:
                    wf_instance.completed_tasks.add(task_id)
                if candidate_task:
                    wf_instance.checkpoint_task = candidate_task
                
                # STATE MACHINE: Check if retry is allowed by state transition guards
                if not wf_instance.can_retry(max_retries_with_boost):
                    print(f"   🛑 STATE-MACHINE: UID {uid} cannot retry (state={wf_instance.state.value})")
                    continue

                if not candidate_task:
                    frontier = determine_next_tasks(tasks_data, completed)
                    if not frontier:
                        expected_tasks = all_expected_tasks or set(tasks_data.keys())
                        if expected_tasks and expected_tasks.issubset(completed):
                            print(f"✅ UID {uid}: already has all tasks completed; clearing retry state")
                            clear_retry_state(uid, workflow, args.baseline)
                            # STATE MACHINE: Mark as completed
                            wf_instance.mark_completed()
                            cleanup_completed_instances(workflow, args.baseline, {uid})
                        else:
                            print(f"❌ UID {uid}: cannot determine next task (completed={sorted(completed)})")
                        continue
                    candidate_task = frontier[0]
                    checkpoint = None
                    print(f"ℹ️ UID {uid}: no checkpoint; initial frontier → '{candidate_task}'")

                # Get retry count and last attempt time
                retry_count = get_retry_count(uid, workflow, args.baseline)
                last_attempt_at = None
                with mysql_connection() as conn:
                    cursor = conn.cursor()
                    query = "SELECT last_attempt_at FROM checkpoint_retries WHERE uid=%s AND workflow_name=%s AND baseline=%s"
                    params = [uid, workflow, args.baseline]
                    if EXPERIMENT_TAG:
                        query += " AND notes = %s"
                        params.append(EXPERIMENT_TAG)
                    cursor.execute(query, tuple(params))
                    row = cursor.fetchone()
                    if row and row[0]:
                        last_attempt_at = row[0]
                
                is_stuck = False
                if last_attempt_at and completed_count_for_uid > 0:
                    # Check if no progress made in last 5 minutes despite retries
                    time_since_last = (datetime.now() - last_attempt_at).total_seconds()
                    if time_since_last > 300 and retry_count > 3:  # >5 min, >3 retries
                        is_stuck = True
                        print(f"   🚨 UID {uid} appears stuck: {completed_count_for_uid} tasks done, {retry_count} retries, last attempt {time_since_last:.0f}s ago")
                
                # Get semantic retry strategy for this checkpoint
                checkpoint_group = uid_to_group.get(uid, 'middle')
                strategy = get_semantic_retry_strategy(checkpoint_group, workflow)
                semantic_cooldown_mult = strategy.get('cooldown_multiplier', 1.0)
                max_retry_boost = strategy.get('max_retry_boost', 0)
                parallel_retry = strategy.get('parallel_retry', False) and True
                
                # Apply semantic retry boost to max retries
                if max_retry_boost > 0:
                    max_retries_with_boost = max_retries_with_boost + max_retry_boost
                    if checkpoint_group != 'middle':
                        print(f"   📍 UID {uid}: {checkpoint_group} checkpoint → +{max_retry_boost} max retries, cooldown ×{semantic_cooldown_mult:.2f}")
                
                # Stuck UID: allow extra retries
                if is_stuck:
                    max_retries_with_boost += 2
                    print(f"   🔄 Stuck UID: allowing +2 extra retries (now {max_retries_with_boost})")
                
                success = False
                backoff_chain_tasks = list(backoff_chain(tasks_data, candidate_task, completed))
                
                # STATE MACHINE: Transition to recovering state
                wf_instance.start_recovery(from_checkpoint=(checkpoint is not None))
                if checkpoint:
                    print(f"   🔄 STATE-MACHINE: {uid} entering CHECKPOINT_RECOVERY (attempt {wf_instance.recovery_attempts})")
                
                # For late checkpoints in longest workflows: try parallel retries
                if parallel_retry and len(backoff_chain_tasks) > 1:
                    print(f"   🔀 Parallel retry mode: attempting {min(3, len(backoff_chain_tasks))} tasks simultaneously for late checkpoint")
                    # Try up to 3 tasks in parallel for late checkpoints
                    parallel_tasks = backoff_chain_tasks[:3]
                    parallel_success = False
                    for t in parallel_tasks:
                        fn_url = FISSION_ROUTER_PREFIX + str(t)
                        is_primary = (t == candidate_task)
                        print(f"   📤 [Parallel] Pushing retry request for UID {uid} to task '{t}'")
                        status = retry_from_checkpoint(t, checkpoint if is_primary else None,
                                                      fn_url, uid, workflow, args.baseline)
                        if status in (200, 202) or status >= 500:
                            parallel_success = True
                            if is_primary:
                                bump_retry(uid, workflow, args.baseline, t, 200 if status == 200 else 202)
                    if parallel_success:
                        success = True
                        print(f"   ✅ Parallel retry succeeded for UID {uid}")
                
                # If parallel retry didn't work or wasn't enabled, try sequential
                if not success:
                    for t in backoff_chain_tasks:
                        fn_url = FISSION_ROUTER_PREFIX + str(t)
                        is_primary = (t == candidate_task)
                        
                        print(f"📤 Pushing retry request for UID {uid} to {workflow} (baseline={args.baseline}) at task '{t}'")
                        status = retry_from_checkpoint(t, checkpoint if is_primary else None,
                                                      fn_url, uid, workflow, args.baseline)

                        if is_primary:
                            # Only count retry attempts for the primary candidate task, not backoff chain ancestors
                            # This prevents a single failure from exhausting all retries when trying multiple ancestors
                            # For push-and-forget, we're lenient about what counts as "sent"
                            # - 200: Request accepted
                            # - 202: Request accepted (timeout/queued)
                            # - 500: Pod might be starting, but request may be queued - treat as sent
                            # - -1: Network error - will retry in next cycle
                            if status in (200, 202):
                                bump_retry(uid, workflow, args.baseline, t, 200)  # Use 200 to indicate request was sent
                                print(f"✅ Retry request sent to '{t}' for UID {uid} (task pod will handle execution)")
                                success = True  # Request was sent successfully
                            elif status >= 500:
                                # HTTP 500 - pod might be starting, but for push-and-forget we consider it sent
                                bump_retry(uid, workflow, args.baseline, t, 202)  # Use 202 to indicate queued
                                print(f"✅ Retry request queued to '{t}' for UID {uid} (HTTP {status}, pod may be starting)")
                                success = True  # Consider it sent - pod will handle when ready
                            else:
                                # Other status codes or network errors - will retry in next cycle
                                if status != -1:  # Don't bump retry for network errors (they'll retry automatically)
                                    bump_retry(uid, workflow, args.baseline, t, status)
                                print(f"⚠️ Retry request to '{t}' returned HTTP {status} (will retry in next cycle if needed)")
                        else:
                            # Log backoff attempts but don't count them toward retry limit
                            if status in (200, 202) or status >= 500:
                                print(f"   ℹ️ Backoff request sent/queued to '{t}' (HTTP {status}, not counted toward retry limit)")
                            else:
                                print(f"   ⚠️ Backoff request to '{t}' returned HTTP {status}")
                        
                        # For push-and-forget, we only need one successful send per UID per cycle
                        # Consider it successful if we got 200, 202, or 500 (queued)
                        if status in (200, 202) or status >= 500:
                            break
                
                # For longest workflows: if primary retry failed and we have earlier checkpoints, try them
                if not success and True and earlier_checkpoints and isinstance(earlier_checkpoints, list):
                    print(f"   🔄 UID {uid}: Primary checkpoint failed, trying earlier checkpoints...")
                    for alt_task_id, alt_checkpoint in earlier_checkpoints[1:]:  # Skip first (already tried)
                        if alt_task_id and alt_task_id != candidate_task:
                            alt_fn_url = FISSION_ROUTER_PREFIX + str(alt_task_id)
                            print(f"   📤 Trying earlier checkpoint at task '{alt_task_id}'...")
                            alt_status = retry_from_checkpoint(alt_task_id, alt_checkpoint, alt_fn_url, uid, workflow, args.baseline)
                            if alt_status in (200, 202) or alt_status >= 500:
                                print(f"   ✅ Earlier checkpoint retry sent successfully to '{alt_task_id}'")
                                success = True
                                break
                
                if success:
                    print(f"✅ UID {uid}: Retry request sent successfully. Task pod will handle execution.")
                    # Don't clear retry state immediately - wait for task to complete
                    # The retry state will be cleared when the workflow completes successfully
                    retries_this_cycle += 1
                else:
                    print(f"⚠️ UID {uid}: Failed to send retry request. Will retry in next polling cycle.")

                # Minimal sleep for push-and-forget (just to avoid overwhelming the system)
                # Since we're not waiting for responses, we can process faster
                # Longest workflows: even faster processing since retry service is main character
                if True:
                    time.sleep(0.005)  # 5ms for longest workflows (very fast processing)
                else:
                    time.sleep(0.01)  # 10ms for others
            
            # Update retry efficiency tracker after processing this workflow
            if retries_this_cycle > 0:
                update_retry_efficiency(current_success_rate, retries_this_cycle)

        # Use faster polling for longest workflows (retry service is the main character)
        # Check if any workflow in the list is a longest workflow
        has_longest_workflow = any(True for wf in workflow_names)
        poll_interval = LONGEST_WORKFLOW_POLL_INTERVAL if has_longest_workflow else POLL_INTERVAL
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()