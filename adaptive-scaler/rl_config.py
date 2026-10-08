#!/usr/bin/env python3
"""
RL Configuration for Adaptive Scaler

This module defines the action space, hyperparameters, and cold start settings
for the contextual bandit-based adaptive scaler.

Budget Reference:
    - Total experiment on-demand cost: ~$60
    - Budget cap: 50% of on-demand = ~$30 total
    - This cap includes ALL costs: scaling, retries, pod churn
    - Per-workflow budgets are calculated based on estimated runtime & machines
"""

# ============================================================================
# ACTION SPACE DEFINITIONS
# ============================================================================
# Four discrete scaling intensity levels. cp+retry always runs in tandem;
# these actions control how aggressively the scaler additionally intervenes.

ACTIONS = {
    0: {
        'name': 'idle',
        'scale_factor': 1.0,
        'description': 'Let cp+retry handle - no scaling intervention'
    },
    1: {
        'name': 'conservative',
        'scale_factor': 1.1,
        'description': '10% scale-out - minimal intervention'
    },
    2: {
        'name': 'moderate',
        'scale_factor': 1.3,
        'description': '30% scale-out - balanced intervention'
    },
    3: {
        'name': 'aggressive',
        'scale_factor': 1.5,
        'description': '50% scale-out - strong intervention'
    },
}

# Number of actions and features for the contextual bandit
N_ACTIONS = len(ACTIONS)
N_FEATURES = 6  # success_rate, success_delta, retry_exhausted, normalized_cost, time_fraction, total_pods

# ============================================================================
# COLD START CONFIGURATION
# ============================================================================
# During cold start, the scaler observes but only takes safe actions.
# This allows collecting baseline data for cp+retry performance.

# Workflow classifications for cold start tuning
SIMPLE_WORKFLOWS = {'wf-1', 'wf-2', 'wf-3', 'wf-4', 'wf-7'}
COMPLEX_WORKFLOWS = {'wf-5', 'wf-6', 'wf-8', 'wf-9'}

# Default cold start config (used for complex workflows - need faster intervention)
COLD_START_CONFIG = {
    # Minimum time (minutes) before the RL agent can fully activate
    # Short for complex workflows - they need faster intervention
    'min_observation_minutes': 2,
    
    # Minimum number of observation data points before activation
    'min_observations': 10,
    
    # Success rate threshold - only activate RL agent if success drops below this
    # If success >= threshold, cp+retry is handling it fine, stay in observation mode
    'activation_threshold': 0.75,
    
    # Actions allowed during warmup period - include moderate for complex workflows
    'warmup_actions': [0, 1, 2],
    
    # Tick interval during cold start (seconds)
    'observation_interval': 10,
}

# Workflow-specific cold start overrides
# Simple workflows: VERY long cold start (30 min) to let retry service handle recovery alone
# This prevents unnecessary scaling that drives up cost without improving success
# Complex workflows: shorter cold start for faster scaler intervention
WORKFLOW_COLD_START_OVERRIDES = {
    # Simple workflows - 30 min cold start, idle-only warmup (no scale-ups)
    # Scaler stays completely idle unless success drops below 90%
    'wf-1': {'min_observation_minutes': 30, 'min_observations': 60, 'activation_threshold': 0.94, 'warmup_actions': [0]},
    'wf-2': {'min_observation_minutes': 30, 'min_observations': 60, 'activation_threshold': 0.94, 'warmup_actions': [0]},
    'wf-3': {'min_observation_minutes': 30, 'min_observations': 60, 'activation_threshold': 0.94, 'warmup_actions': [0]},
    'wf-4': {'min_observation_minutes': 30, 'min_observations': 60, 'activation_threshold': 0.94, 'warmup_actions': [0]},
    'wf-7': {'min_observation_minutes': 30, 'min_observations': 60, 'activation_threshold': 0.94, 'warmup_actions': [0]},
    # Complex workflows - use defaults (short cold start, fast intervention)
    # wf-5, wf-6, wf-8, wf-9 use COLD_START_CONFIG defaults
}

def get_cold_start_config(workflow_name: str = None) -> dict:
    """
    Get cold start configuration for a specific workflow.
    
    Simple workflows get longer cold start times to let retry service work first.
    Complex workflows get shorter cold start for faster scaler intervention.
    
    Args:
        workflow_name: Workflow identifier (e.g., 'wf-1', 'wf-5')
        
    Returns:
        Cold start configuration dict
    """
    config = COLD_START_CONFIG.copy()
    
    if workflow_name and workflow_name in WORKFLOW_COLD_START_OVERRIDES:
        config.update(WORKFLOW_COLD_START_OVERRIDES[workflow_name])
    
    return config

# ============================================================================
# REWARD FUNCTION CONFIGURATION
# ============================================================================
# Cost-weighted reward: success_gain / cost_incurred

REWARD_CONFIG = {
    # Penalty applied when budget is exhausted
    'budget_penalty': 10.0,
    
    # Multiplier for free improvements (cost_incurred <= 0)
    'free_improvement_multiplier': 100.0,
    
    # Minimum cost to avoid division by zero
    'min_cost_epsilon': 0.001,
    
    # Reward clipping bounds to prevent extreme values
    'reward_clip_min': -100.0,
    'reward_clip_max': 100.0,
    
    # Discount factor for delayed rewards (not used in immediate reward setting)
    'gamma': 0.95,
}

# ============================================================================
# BUDGET INTEGRATION
# ============================================================================
# Budget remaining fraction determines which actions are allowed
# Note: Total experiment budget is 50% of on-demand cost (~$30 for ~$60 on-demand)
# These thresholds are conservative to ensure we stay within the hard cap

BUDGET_ACTION_THRESHOLDS = {
    # budget_remaining_fraction -> allowed actions
    # RELAXED thresholds to allow more scaling for complex workflows
    # The retry service handles recovery, so we can be more aggressive with scaling
    0.10: [0],          # < 10% budget: only idle (critical reserve)
    0.20: [0, 1],       # < 20% budget: idle or conservative
    0.35: [0, 1, 2],    # < 35% budget: no aggressive scaling
    1.0: [0, 1, 2, 3],  # >= 35% budget: all actions allowed
}

def get_allowed_actions(budget_remaining_fraction: float) -> list:
    """
    Get list of allowed actions based on remaining budget fraction.
    
    Args:
        budget_remaining_fraction: Value between 0 and 1 indicating budget remaining
        
    Returns:
        List of action indices that are allowed
    """
    for threshold, actions in sorted(BUDGET_ACTION_THRESHOLDS.items()):
        if budget_remaining_fraction < threshold:
            return actions
    return [0, 1, 2, 3]  # Default: all actions

# ============================================================================
# THOMPSON SAMPLING HYPERPARAMETERS
# ============================================================================

THOMPSON_SAMPLING_CONFIG = {
    # Prior parameters for Bayesian linear regression
    'prior_mean': 0.0,          # Prior mean for weights
    'prior_variance': 1.0,       # Prior variance for weights
    'noise_variance': 1.0,       # Observation noise variance
    
    # Regularization parameter (lambda in ridge regression)
    'regularization': 1.0,
    
    # Exploration bonus (scales the sampling variance)
    # INCREASED from 0.1 to 0.5 - more exploration to escape local optima
    'exploration_bonus': 0.5,
    
    # Minimum samples before using learned parameters
    # INCREASED from 3 to 5 - require more data before exploiting
    'min_samples_per_action': 5,
}

# ============================================================================
# OBSERVATION AND TIMING
# ============================================================================

TIMING_CONFIG = {
    # Time (seconds) to wait after taking an action before measuring reward
    'observation_window': 60,
    
    # Minimum interval (seconds) between scaling decisions
    'decision_interval': 30,
    
    # Cooldown (seconds) after aggressive scaling before next decision
    'aggressive_cooldown': 120,
    
    # Maximum history size to keep in memory
    'max_history_size': 1000,
}

# ============================================================================
# PERSISTENCE CONFIGURATION
# ============================================================================

PERSISTENCE_CONFIG = {
    # Whether to persist bandit state across experiments
    'enable_persistence': True,
    
    # Path for JSON file persistence (relative to Scaler directory)
    # UPDATED: Store in rl-state-files subdirectory for better organization
    'state_file_template': 'rl-state-files/rl_state_{workflow}_{baseline}.json',
    
    # Whether to use MySQL for persistence (alternative to file)
    'use_mysql': False,
    
    # Table name for MySQL persistence
    'mysql_table': 'rl_bandit_state',
}

# ============================================================================
# CONTEXT FEATURE NORMALIZATION
# ============================================================================
# Expected ranges for each feature, used for normalization

FEATURE_RANGES = {
    'success_rate': (0.0, 1.0),
    'success_rate_delta': (-0.5, 0.5),
    'retry_exhausted_ratio': (0.0, 1.0),
    'normalized_cost': (0.0, 2.0),  # Can exceed 1.0 if over budget
    'time_fraction': (0.0, 1.0),
    'total_pods_normalized': (0.0, 1.0),
}

# Feature indices for the context vector
FEATURE_INDICES = {
    'success_rate': 0,
    'success_rate_delta': 1,
    'retry_exhausted_ratio': 2,
    'normalized_cost': 3,
    'time_fraction': 4,
    'total_pods_normalized': 5,
}

# ============================================================================
# LOGGING AND DEBUGGING
# ============================================================================

LOGGING_CONFIG = {
    # Enable verbose logging of RL decisions
    'verbose': True,
    
    # Log file for RL-specific events
    'log_file': 'rl_scaler.log',
    
    # Log every N decisions
    'log_interval': 1,
    
    # Print action selection details
    'print_action_selection': True,
    
    # Print reward calculations
    'print_rewards': True,
}
