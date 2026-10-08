#!/usr/bin/env python3
"""
RL State/Context Feature Extraction

This module provides functions to extract context features from the existing
scaler infrastructure for use in the contextual bandit.
"""

import time
import numpy as np
from typing import Tuple, Dict, Optional, List
from collections import deque

from rl_config import (
    N_FEATURES, 
    FEATURE_RANGES, 
    FEATURE_INDICES,
    COLD_START_CONFIG,
    LOGGING_CONFIG,
    get_cold_start_config,
)

# ============================================================================
# STATE TRACKER
# ============================================================================
# Maintains rolling history for delta calculations and cold start tracking

class RLStateTracker:
    """
    Tracks system state over time for RL context extraction.
    Maintains history for calculating deltas and trends.
    """
    
    def __init__(self, history_size: int = 100):
        self.history_size = history_size
        
        # Rolling history of success rates for delta calculation
        self.success_rate_history: deque = deque(maxlen=history_size)
        
        # Cold start tracking
        self.start_time: Optional[float] = None
        self.observation_count: int = 0
        self.cold_start_complete: bool = False
        
        # Last context for comparison
        self.last_context: Optional[np.ndarray] = None
        self.last_context_time: Optional[float] = None
        
        # Workflow-specific tracking
        self.workflow: Optional[str] = None
        self.baseline: Optional[str] = None
        
        # Total pods baseline (for normalization)
        self.base_total_pods: int = 0
        self.max_total_pods: int = 100  # Will be set based on workflow
        
    def reset(self, workflow: str = None, baseline: str = None, 
              base_total_pods: int = 0, max_total_pods: int = 100):
        """Reset state tracker for new experiment."""
        self.success_rate_history.clear()
        self.start_time = time.time()
        self.observation_count = 0
        self.cold_start_complete = False
        self.last_context = None
        self.last_context_time = None
        self.workflow = workflow
        self.baseline = baseline
        self.base_total_pods = base_total_pods
        self.max_total_pods = max_total_pods
        
        if LOGGING_CONFIG.get('verbose'):
            print(f"🔄 RL State Tracker reset for {workflow}/{baseline}")
    
    def record_success_rate(self, success_rate: float):
        """Record a success rate observation."""
        self.success_rate_history.append({
            'rate': success_rate,
            'time': time.time()
        })
        self.observation_count += 1
    
    def get_success_rate_delta(self, window: int = 5) -> float:
        """
        Calculate success rate change over recent window.
        
        Returns:
            Delta between current and N observations ago (positive = improving)
        """
        if len(self.success_rate_history) < 2:
            return 0.0
        
        current = self.success_rate_history[-1]['rate']
        
        # Get rate from 'window' observations ago
        idx = max(0, len(self.success_rate_history) - window - 1)
        previous = self.success_rate_history[idx]['rate']
        
        return current - previous
    
    def get_elapsed_minutes(self) -> float:
        """Get elapsed time since start in minutes."""
        if self.start_time is None:
            return 0.0
        return (time.time() - self.start_time) / 60.0
    
    def is_cold_start_complete(self, current_success_rate: float) -> bool:
        """
        Check if cold start phase is complete.
        
        Cold start ends when:
        1. Minimum observation time has passed
        2. Minimum observations collected
        3. Success rate drops below threshold (cp+retry needs help)
        
        Uses workflow-specific cold start configuration:
        - Simple workflows (wf-1,2,3,4,7): Longer cold start to let retry service work
        - Complex workflows (wf-4,6,8,9): Shorter cold start for faster intervention
        """
        if self.cold_start_complete:
            return True
        
        # Get workflow-specific cold start config
        config = get_cold_start_config(self.workflow)
        
        elapsed_minutes = self.get_elapsed_minutes()
        
        # Check minimum time
        if elapsed_minutes < config['min_observation_minutes']:
            if LOGGING_CONFIG.get('verbose'):
                print(f"❄️ COLD START ({self.workflow}): {elapsed_minutes:.1f}m < {config['min_observation_minutes']}m minimum")
            return False
        
        # Check minimum observations
        if self.observation_count < config['min_observations']:
            if LOGGING_CONFIG.get('verbose'):
                print(f"❄️ COLD START ({self.workflow}): {self.observation_count} < {config['min_observations']} observations")
            return False
        
        # Check success threshold - if success is good, stay in observation mode
        if current_success_rate >= config['activation_threshold']:
            if LOGGING_CONFIG.get('verbose'):
                print(f"❄️ COLD START ({self.workflow}): success {current_success_rate:.1%} >= {config['activation_threshold']:.1%} threshold (cp+retry handling it)")
            return False
        
        # Cold start complete - RL agent should activate
        self.cold_start_complete = True
        print(f"✅ COLD START COMPLETE ({self.workflow}): Activating RL agent (success={current_success_rate:.1%} < {config['activation_threshold']:.1%})")
        return True


# Global state tracker instance
_state_tracker = RLStateTracker()


def get_state_tracker() -> RLStateTracker:
    """Get the global state tracker instance."""
    return _state_tracker


def reset_state_tracker(workflow: str = None, baseline: str = None,
                        base_total_pods: int = 0, max_total_pods: int = 100):
    """Reset the global state tracker for a new experiment."""
    _state_tracker.reset(workflow, baseline, base_total_pods, max_total_pods)


# ============================================================================
# CONTEXT FEATURE EXTRACTION
# ============================================================================

def normalize_feature(value: float, feature_name: str) -> float:
    """
    Normalize a feature value to [0, 1] range.
    
    Args:
        value: Raw feature value
        feature_name: Name of feature (for range lookup)
        
    Returns:
        Normalized value clipped to [0, 1]
    """
    min_val, max_val = FEATURE_RANGES.get(feature_name, (0.0, 1.0))
    
    if max_val == min_val:
        return 0.5  # Avoid division by zero
    
    normalized = (value - min_val) / (max_val - min_val)
    return np.clip(normalized, 0.0, 1.0)


def get_context_vector(
    workflow_name: str,
    baseline: str,
    # These should be passed from the main scaler
    success_rate: float,
    retry_exhausted_count: int,
    retry_exhausted_threshold: int,
    budget_spent: float,
    budget_total: float,
    elapsed_seconds: float,
    expected_runtime_seconds: float,
    total_pods: int,
    max_pods: int = 100,
) -> np.ndarray:
    """
    Extract context vector for the contextual bandit.
    
    Args:
        workflow_name: Name of the workflow
        baseline: Baseline name
        success_rate: Current workflow success rate [0, 1]
        retry_exhausted_count: Number of UIDs that exhausted retries
        retry_exhausted_threshold: Threshold for retry exhaustion
        budget_spent: Amount of budget spent
        budget_total: Total budget available
        elapsed_seconds: Seconds since experiment start
        expected_runtime_seconds: Expected total runtime in seconds
        total_pods: Current total number of pods
        max_pods: Maximum possible pods (for normalization)
        
    Returns:
        Normalized context vector of shape (N_FEATURES,)
    """
    tracker = get_state_tracker()
    
    # Record the success rate for history
    tracker.record_success_rate(success_rate)
    
    # Feature 1: Success rate (already normalized 0-1)
    f_success_rate = success_rate
    
    # Feature 2: Success rate delta (improvement trend)
    f_success_delta = tracker.get_success_rate_delta(window=5)
    f_success_delta = normalize_feature(f_success_delta, 'success_rate_delta')
    
    # Feature 3: Retry exhausted ratio (how much headroom cp+retry has)
    if retry_exhausted_threshold > 0:
        f_retry_exhausted = retry_exhausted_count / retry_exhausted_threshold
    else:
        f_retry_exhausted = 0.0
    f_retry_exhausted = np.clip(f_retry_exhausted, 0.0, 1.0)
    
    # Feature 4: Normalized cost (current spend vs budget)
    if budget_total > 0:
        f_normalized_cost = budget_spent / budget_total
    else:
        f_normalized_cost = 0.0
    f_normalized_cost = normalize_feature(f_normalized_cost, 'normalized_cost')
    
    # Feature 5: Time fraction (where are we in the experiment)
    if expected_runtime_seconds > 0:
        f_time_fraction = elapsed_seconds / expected_runtime_seconds
    else:
        f_time_fraction = 0.0
    f_time_fraction = np.clip(f_time_fraction, 0.0, 1.0)
    
    # Feature 6: Total pods normalized
    if max_pods > 0:
        f_total_pods = total_pods / max_pods
    else:
        f_total_pods = 0.0
    f_total_pods = np.clip(f_total_pods, 0.0, 1.0)
    
    # Assemble context vector
    context = np.array([
        f_success_rate,
        f_success_delta,
        f_retry_exhausted,
        f_normalized_cost,
        f_time_fraction,
        f_total_pods,
    ], dtype=np.float64)
    
    # Store for comparison
    tracker.last_context = context
    tracker.last_context_time = time.time()
    
    if LOGGING_CONFIG.get('verbose') and LOGGING_CONFIG.get('print_action_selection'):
        print(f"📊 Context: success={f_success_rate:.2%}, delta={f_success_delta:.3f}, "
              f"retry_exhaust={f_retry_exhausted:.2f}, cost={f_normalized_cost:.2f}, "
              f"time={f_time_fraction:.2f}, pods={f_total_pods:.2f}")
    
    return context


def get_context_from_scaler_state(
    workflow_name: str,
    baseline: str,
    workflows_dir: str,
    metadata: dict,
    # Import these from preemptive_scaler at runtime
    get_workflow_success_rate_fn,
    check_retry_service_exhausted_fn,
    budget_envelope_tracker: dict,
    get_total_pods_fn,
) -> Tuple[np.ndarray, dict]:
    """
    High-level function to extract context using existing scaler functions.
    
    This is the main entry point called from preemptive_scaler.py.
    
    Returns:
        (context_vector, raw_values_dict)
    """
    # Get success rate
    success_rate, total_runs, successful_runs = get_workflow_success_rate_fn(
        workflow_name, workflows_dir, baseline
    )
    
    # Get retry exhaustion status
    is_exhausted, exhausted_count = check_retry_service_exhausted_fn(workflow_name, baseline)
    
    # Extract budget info
    budget_total = budget_envelope_tracker.get('budget', 0.0)
    budget_spent = budget_envelope_tracker.get('spent', 0.0)
    start_time = budget_envelope_tracker.get('start_time', time.time())
    elapsed_seconds = time.time() - start_time
    runtime_hours = budget_envelope_tracker.get('estimated_runtime_hours', 0.167)
    expected_runtime_seconds = runtime_hours * 3600
    
    # Get total pods
    total_pods = get_total_pods_fn() if callable(get_total_pods_fn) else 0
    
    # Calculate max pods from metadata
    max_pods = sum(
        task_meta.get('minscale', 1) * 3  # Allow up to 3x base
        for task_meta in metadata.values()
    )
    
    # Get context vector
    context = get_context_vector(
        workflow_name=workflow_name,
        baseline=baseline,
        success_rate=success_rate,
        retry_exhausted_count=exhausted_count,
        retry_exhausted_threshold=12,  # MIN_RETRY_EXHAUSTED_UIDS from scaler
        budget_spent=budget_spent,
        budget_total=budget_total,
        elapsed_seconds=elapsed_seconds,
        expected_runtime_seconds=expected_runtime_seconds,
        total_pods=total_pods,
        max_pods=max_pods,
    )
    
    # Return raw values for logging/debugging
    raw_values = {
        'success_rate': success_rate,
        'total_runs': total_runs,
        'successful_runs': successful_runs,
        'retry_exhausted': is_exhausted,
        'retry_exhausted_count': exhausted_count,
        'budget_spent': budget_spent,
        'budget_total': budget_total,
        'elapsed_seconds': elapsed_seconds,
        'total_pods': total_pods,
    }
    
    return context, raw_values


def is_cold_start_complete(current_success_rate: float) -> bool:
    """Check if cold start phase is complete."""
    return _state_tracker.is_cold_start_complete(current_success_rate)


def get_allowed_warmup_actions(workflow_name: str = None) -> List[int]:
    """
    Get list of actions allowed during cold start warmup.
    
    Simple workflows only allow idle/conservative during warmup.
    Complex workflows allow idle/conservative/moderate.
    """
    config = get_cold_start_config(workflow_name)
    return config.get('warmup_actions', COLD_START_CONFIG['warmup_actions'])
