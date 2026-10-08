#!/usr/bin/env python3
"""
RL Configuration for Adaptive Scaler

This module defines the action space, hyperparameters, and cold start settings
for the contextual bandit-based adaptive scaler.

All tunable values can be overridden via environment variables — see .env.example.
"""

import os

# ============================================================================
# ACTION SPACE DEFINITIONS
# ============================================================================

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

N_ACTIONS = len(ACTIONS)
N_FEATURES = 6  # success_rate, success_delta, retry_exhausted, normalized_cost, time_fraction, total_pods

# ============================================================================
# COLD START CONFIGURATION
# ============================================================================
# During cold start, the scaler observes but only takes safe actions.

_cold_start_minutes = int(os.getenv('WF_COLD_START_MINUTES', '5'))

COLD_START_CONFIG = {
    'min_observation_minutes': _cold_start_minutes,
    'min_observations': max(10, _cold_start_minutes * 2),
    'activation_threshold': float(os.getenv('WF_ACTIVATION_THRESHOLD', '0.75')),
    'warmup_actions': [0, 1, 2],
    'observation_interval': 10,
}

def get_cold_start_config(workflow_name: str = None) -> dict:
    return COLD_START_CONFIG.copy()

# ============================================================================
# REWARD FUNCTION CONFIGURATION
# ============================================================================

REWARD_CONFIG = {
    'budget_penalty': 10.0,
    'free_improvement_multiplier': 100.0,
    'min_cost_epsilon': 0.001,
    'reward_clip_min': -100.0,
    'reward_clip_max': 100.0,
    'gamma': 0.95,
}

# ============================================================================
# BUDGET INTEGRATION
# ============================================================================

BUDGET_ACTION_THRESHOLDS = {
    0.10: [0],
    0.20: [0, 1],
    0.35: [0, 1, 2],
    1.0: [0, 1, 2, 3],
}

def get_allowed_actions(budget_remaining_fraction: float) -> list:
    for threshold, actions in sorted(BUDGET_ACTION_THRESHOLDS.items()):
        if budget_remaining_fraction < threshold:
            return actions
    return [0, 1, 2, 3]

# ============================================================================
# THOMPSON SAMPLING HYPERPARAMETERS
# ============================================================================

THOMPSON_SAMPLING_CONFIG = {
    'prior_mean': 0.0,
    'prior_variance': 1.0,
    'noise_variance': 1.0,
    'regularization': 1.0,
    'exploration_bonus': 0.5,
    'min_samples_per_action': 5,
}

# ============================================================================
# OBSERVATION AND TIMING
# ============================================================================

TIMING_CONFIG = {
    'observation_window': 60,
    'decision_interval': 30,
    'aggressive_cooldown': 120,
    'max_history_size': 1000,
}

# ============================================================================
# PERSISTENCE CONFIGURATION
# ============================================================================

PERSISTENCE_CONFIG = {
    'enable_persistence': True,
    'state_file_template': 'rl-state-files/rl_state_{workflow}_{baseline}.json',
    'use_mysql': False,
    'mysql_table': 'rl_bandit_state',
}

# ============================================================================
# CONTEXT FEATURE NORMALIZATION
# ============================================================================

FEATURE_RANGES = {
    'success_rate': (0.0, 1.0),
    'success_rate_delta': (-0.5, 0.5),
    'retry_exhausted_ratio': (0.0, 1.0),
    'normalized_cost': (0.0, 2.0),
    'time_fraction': (0.0, 1.0),
    'total_pods_normalized': (0.0, 1.0),
}

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
    'verbose': True,
    'log_file': 'rl_scaler.log',
    'log_interval': 1,
    'print_action_selection': True,
    'print_rewards': True,
}
