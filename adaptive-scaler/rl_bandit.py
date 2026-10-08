#!/usr/bin/env python3
"""
Contextual Bandit Implementation for Adaptive Scaler

This module implements a Contextual Thompson Sampling bandit for selecting
scaling intensity levels based on system state.
"""

import numpy as np
import json
import os
import time
from typing import List, Tuple, Optional, Dict, Any

# Use numpy.linalg for compatibility (scipy not always available)
from numpy import linalg

from rl_config import (
    ACTIONS,
    N_ACTIONS,
    N_FEATURES,
    THOMPSON_SAMPLING_CONFIG,
    REWARD_CONFIG,
    TIMING_CONFIG,
    PERSISTENCE_CONFIG,
    LOGGING_CONFIG,
    get_allowed_actions,
)


class ContextualBandit:
    """
    Contextual Thompson Sampling Bandit for adaptive scaling decisions.
    
    Uses Bayesian Linear Regression for each action to model the expected
    reward as a linear function of context features. Thompson Sampling
    is used for action selection by sampling from the posterior.
    
    Mathematical Model:
        For each action a, we model: reward = context^T @ weights_a + noise
        Prior: weights_a ~ N(0, lambda^-1 * I)
        Posterior: weights_a | data ~ N(mu_a, Sigma_a)
        
    Action Selection:
        1. For each action, sample weights from posterior
        2. Compute expected reward for each action
        3. Select action with highest expected reward (among allowed actions)
    """
    
    def __init__(self, n_actions: int = N_ACTIONS, n_features: int = N_FEATURES):
        """
        Initialize the contextual bandit.
        
        Args:
            n_actions: Number of discrete actions
            n_features: Dimension of context feature vector
        """
        self.n_actions = n_actions
        self.n_features = n_features
        
        # Thompson Sampling hyperparameters
        self.regularization = THOMPSON_SAMPLING_CONFIG['regularization']
        self.noise_variance = THOMPSON_SAMPLING_CONFIG['noise_variance']
        self.exploration_bonus = THOMPSON_SAMPLING_CONFIG['exploration_bonus']
        self.min_samples_per_action = THOMPSON_SAMPLING_CONFIG['min_samples_per_action']
        
        # Initialize parameters for each action (Bayesian Linear Regression)
        # Each action has its own weight posterior
        self.action_params = {}
        for a in range(n_actions):
            self.action_params[a] = {
                # Precision matrix (inverse covariance) - B_a in the literature
                'B': self.regularization * np.eye(n_features),
                # Weighted sum of contexts times rewards - b_a in the literature  
                'b': np.zeros(n_features),
                # Posterior mean of weights - computed on demand
                'mu': np.zeros(n_features),
                # Number of times this action was selected
                'count': 0,
                # Sum of rewards for this action (for logging)
                'total_reward': 0.0,
            }
        
        # History of (context, action, reward) tuples
        self.history: List[Dict[str, Any]] = []
        
        # Timing tracking
        self.last_action_time: Optional[float] = None
        self.last_action: Optional[int] = None
        
        # Metadata
        self.workflow: Optional[str] = None
        self.baseline: Optional[str] = None
        
    def reset(self, workflow: str = None, baseline: str = None):
        """Reset the bandit for a new experiment (keeps learned parameters)."""
        self.last_action_time = None
        self.last_action = None
        self.workflow = workflow
        self.baseline = baseline
        
        if LOGGING_CONFIG.get('verbose'):
            print(f"🎰 Contextual Bandit reset for {workflow}/{baseline}")
            self._print_action_stats()
    
    def hard_reset(self):
        """Completely reset the bandit, clearing all learned parameters."""
        self.__init__(self.n_actions, self.n_features)
        
        if LOGGING_CONFIG.get('verbose'):
            print("🎰 Contextual Bandit HARD RESET - all learned parameters cleared")
    
    def _update_posterior(self, action: int):
        """
        Update the posterior mean for an action.
        
        Posterior mean: mu_a = B_a^-1 @ b_a
        """
        params = self.action_params[action]
        try:
            # Solve B_a @ mu_a = b_a for mu_a
            params['mu'] = linalg.solve(params['B'], params['b'])
        except linalg.LinAlgError:
            # Fallback to pseudoinverse if singular
            result = linalg.lstsq(params['B'], params['b'], rcond=None)
            params['mu'] = result[0]
    
    def _sample_weights(self, action: int) -> np.ndarray:
        """
        Sample weight vector from posterior for Thompson Sampling.
        
        Sample: weights ~ N(mu_a, noise_variance * B_a^-1)
        """
        params = self.action_params[action]
        
        # If not enough samples, use prior (exploration)
        if params['count'] < self.min_samples_per_action:
            # Sample from prior: N(0, lambda^-1 * I)
            return np.random.multivariate_normal(
                mean=np.zeros(self.n_features),
                cov=(1.0 / self.regularization) * np.eye(self.n_features)
            )
        
        # Update posterior mean
        self._update_posterior(action)
        
        # Compute posterior covariance: noise_variance * B_a^-1
        try:
            cov = self.noise_variance * linalg.inv(params['B'])
            # Add exploration bonus to encourage exploration
            cov += self.exploration_bonus * np.eye(self.n_features)
        except linalg.LinAlgError:
            # Fallback to diagonal covariance
            cov = self.noise_variance * np.eye(self.n_features)
        
        # Ensure covariance is positive definite
        cov = (cov + cov.T) / 2  # Symmetrize
        eigvals = linalg.eigvals(cov)
        min_eig = np.min(np.real(eigvals))
        if min_eig < 1e-10:
            cov += (1e-10 - min_eig) * np.eye(self.n_features)
        
        # Sample from posterior
        try:
            return np.random.multivariate_normal(mean=params['mu'], cov=cov)
        except (linalg.LinAlgError, ValueError):
            # Fallback: add noise to mean
            return params['mu'] + np.random.randn(self.n_features) * 0.1
    
    def select_action(self, context: np.ndarray, 
                      allowed_actions: List[int] = None) -> int:
        """
        Select an action using Thompson Sampling.
        
        Args:
            context: Feature vector of shape (n_features,)
            allowed_actions: List of action indices that are allowed (budget constraint)
            
        Returns:
            Selected action index
        """
        if allowed_actions is None:
            allowed_actions = list(range(self.n_actions))
        
        if len(allowed_actions) == 0:
            # Fallback to idle if no actions allowed
            return 0
        
        if len(allowed_actions) == 1:
            # Only one choice
            action = allowed_actions[0]
            self._record_selection(context, action, "only_choice")
            return action
        
        # Thompson Sampling: sample weights for each allowed action
        sampled_rewards = {}
        for a in allowed_actions:
            weights = self._sample_weights(a)
            # Expected reward = context^T @ weights
            sampled_rewards[a] = np.dot(context, weights)
        
        # Select action with highest sampled reward
        best_action = max(sampled_rewards, key=sampled_rewards.get)
        
        self._record_selection(context, best_action, "thompson_sampling", sampled_rewards)
        return best_action
    
    def select_warmup_action(self, context: np.ndarray,
                             warmup_actions: List[int] = None) -> int:
        """
        Select an action during warmup/cold start phase.
        
        During warmup, we only allow safe actions and do more exploration
        to build up the initial model.
        
        Args:
            context: Feature vector
            warmup_actions: Allowed actions during warmup (default: [0, 1])
            
        Returns:
            Selected action index
        """
        if warmup_actions is None:
            warmup_actions = [0, 1]  # Idle and conservative only
        
        # During warmup, use epsilon-greedy with high exploration
        epsilon = 0.5  # 50% random during warmup
        
        if np.random.random() < epsilon:
            # Random exploration among warmup actions
            action = np.random.choice(warmup_actions)
            self._record_selection(context, action, "warmup_explore")
        else:
            # Thompson Sampling among warmup actions
            action = self.select_action(context, warmup_actions)
        
        return action
    
    def _record_selection(self, context: np.ndarray, action: int, 
                          method: str, sampled_rewards: dict = None):
        """Record action selection for logging."""
        self.last_action = action
        self.last_action_time = time.time()
        
        if LOGGING_CONFIG.get('verbose') and LOGGING_CONFIG.get('print_action_selection'):
            action_info = ACTIONS[action]
            print(f"🎯 RL ACTION: {action_info['name']} (#{action}) via {method}")
            print(f"   Scale factor: {action_info['scale_factor']}x - {action_info['description']}")
            if sampled_rewards:
                rewards_str = ", ".join(f"{a}:{r:.3f}" for a, r in sorted(sampled_rewards.items()))
                print(f"   Sampled rewards: {rewards_str}")
    
    def update(self, context: np.ndarray, action: int, reward: float):
        """
        Update the bandit with observed reward.
        
        Bayesian Linear Regression update:
            B_a <- B_a + context @ context^T
            b_a <- b_a + reward * context
        
        Args:
            context: Feature vector used for decision
            action: Action that was taken
            reward: Observed reward
        """
        # Clip reward to prevent extreme values
        reward = np.clip(reward, REWARD_CONFIG['reward_clip_min'], 
                        REWARD_CONFIG['reward_clip_max'])
        
        # Update sufficient statistics
        params = self.action_params[action]
        context = np.asarray(context).flatten()
        
        # B_a <- B_a + context @ context^T
        params['B'] += np.outer(context, context)
        
        # b_a <- b_a + reward * context  
        params['b'] += reward * context
        
        # Update counts and totals
        params['count'] += 1
        params['total_reward'] += reward
        
        # Record in history
        self.history.append({
            'context': context.tolist(),
            'action': action,
            'reward': reward,
            'timestamp': time.time(),
        })
        
        # Trim history if too long
        if len(self.history) > TIMING_CONFIG['max_history_size']:
            self.history = self.history[-TIMING_CONFIG['max_history_size']:]
        
        if LOGGING_CONFIG.get('verbose') and LOGGING_CONFIG.get('print_rewards'):
            action_info = ACTIONS[action]
            avg_reward = params['total_reward'] / params['count'] if params['count'] > 0 else 0
            print(f"📈 RL UPDATE: action={action_info['name']}, reward={reward:.4f}, "
                  f"count={params['count']}, avg_reward={avg_reward:.4f}")
    
    def get_action_stats(self) -> Dict[int, Dict]:
        """Get statistics for each action."""
        stats = {}
        for a in range(self.n_actions):
            params = self.action_params[a]
            count = params['count']
            avg_reward = params['total_reward'] / count if count > 0 else 0.0
            stats[a] = {
                'name': ACTIONS[a]['name'],
                'scale_factor': ACTIONS[a]['scale_factor'],
                'count': count,
                'total_reward': params['total_reward'],
                'avg_reward': avg_reward,
            }
        return stats
    
    def _print_action_stats(self):
        """Print action statistics."""
        print("📊 Action Statistics:")
        stats = self.get_action_stats()
        for a, s in stats.items():
            print(f"   {s['name']}: count={s['count']}, avg_reward={s['avg_reward']:.4f}")
    
    def get_best_action(self, context: np.ndarray, 
                        allowed_actions: List[int] = None) -> Tuple[int, float]:
        """
        Get the best action based on posterior mean (exploitation only).
        
        Args:
            context: Feature vector
            allowed_actions: Allowed action indices
            
        Returns:
            (best_action, expected_reward)
        """
        if allowed_actions is None:
            allowed_actions = list(range(self.n_actions))
        
        best_action = 0
        best_reward = float('-inf')
        
        for a in allowed_actions:
            params = self.action_params[a]
            if params['count'] >= self.min_samples_per_action:
                self._update_posterior(a)
                expected_reward = np.dot(context, params['mu'])
            else:
                expected_reward = 0.0
            
            if expected_reward > best_reward:
                best_reward = expected_reward
                best_action = a
        
        return best_action, best_reward
    
    # =========================================================================
    # PERSISTENCE
    # =========================================================================
    
    def save_state(self, filepath: str = None):
        """
        Save bandit state to file for persistence across experiments.
        
        Args:
            filepath: Path to save file (uses default if None)
        """
        if filepath is None:
            if not PERSISTENCE_CONFIG['enable_persistence']:
                return
            filename = PERSISTENCE_CONFIG['state_file_template'].format(
                workflow=self.workflow or 'unknown',
                baseline=self.baseline or 'unknown'
            )
            filepath = os.path.join(os.path.dirname(__file__), filename)
        
        state = {
            'n_actions': self.n_actions,
            'n_features': self.n_features,
            'workflow': self.workflow,
            'baseline': self.baseline,
            'action_params': {},
            'history': self.history[-500:],  # Keep last 500 entries
            'saved_at': time.time(),
        }
        
        # Convert numpy arrays to lists for JSON serialization
        for a in range(self.n_actions):
            params = self.action_params[a]
            state['action_params'][a] = {
                'B': params['B'].tolist(),
                'b': params['b'].tolist(),
                'mu': params['mu'].tolist(),
                'count': params['count'],
                'total_reward': params['total_reward'],
            }
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w') as f:
                json.dump(state, f, indent=2)
            
            if LOGGING_CONFIG.get('verbose'):
                print(f"💾 Bandit state saved to {filepath}")
        except Exception as e:
            print(f"⚠️ Failed to save bandit state: {e}")
    
    def load_state(self, filepath: str = None) -> bool:
        """
        Load bandit state from file.
        
        Args:
            filepath: Path to state file (uses default if None)
            
        Returns:
            True if state was loaded, False otherwise
        """
        if filepath is None:
            if not PERSISTENCE_CONFIG['enable_persistence']:
                return False
            filename = PERSISTENCE_CONFIG['state_file_template'].format(
                workflow=self.workflow or 'unknown',
                baseline=self.baseline or 'unknown'
            )
            filepath = os.path.join(os.path.dirname(__file__), filename)
        
        if not os.path.exists(filepath):
            if LOGGING_CONFIG.get('verbose'):
                print(f"📂 No saved state found at {filepath}")
            return False
        
        try:
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            # Validate dimensions
            if state['n_actions'] != self.n_actions or state['n_features'] != self.n_features:
                print(f"⚠️ State dimensions mismatch, ignoring saved state")
                return False
            
            # Load action parameters
            for a in range(self.n_actions):
                a_str = str(a)  # JSON keys are strings
                if a_str in state['action_params']:
                    params = state['action_params'][a_str]
                    self.action_params[a] = {
                        'B': np.array(params['B']),
                        'b': np.array(params['b']),
                        'mu': np.array(params['mu']),
                        'count': params['count'],
                        'total_reward': params['total_reward'],
                    }
            
            # Load history
            self.history = state.get('history', [])
            
            if LOGGING_CONFIG.get('verbose'):
                print(f"📂 Bandit state loaded from {filepath}")
                self._print_action_stats()
            
            return True
            
        except Exception as e:
            print(f"⚠️ Failed to load bandit state: {e}")
            return False


# ============================================================================
# REWARD CALCULATION
# ============================================================================

def calculate_reward(success_before: float, success_after: float, 
                     cost_incurred: float, budget_exhausted: bool = False) -> float:
    """
    Calculate the reward for an action.
    
    Reward = success_gain / cost_incurred (cost-weighted)
    
    Args:
        success_before: Success rate before action (0-1)
        success_after: Success rate after action (0-1)
        cost_incurred: Cost spent during the action window
        budget_exhausted: Whether budget was exhausted
        
    Returns:
        Reward value (higher is better)
    """
    success_gain = success_after - success_before
    
    # Handle free improvement (no cost)
    if cost_incurred <= REWARD_CONFIG['min_cost_epsilon']:
        reward = success_gain * REWARD_CONFIG['free_improvement_multiplier']
    else:
        # Cost-weighted reward: success points gained per dollar
        reward = success_gain / cost_incurred
    
    # Apply budget penalty
    if budget_exhausted:
        reward -= REWARD_CONFIG['budget_penalty']
    
    # Clip to prevent extreme values
    reward = np.clip(reward, REWARD_CONFIG['reward_clip_min'], 
                    REWARD_CONFIG['reward_clip_max'])
    
    if LOGGING_CONFIG.get('verbose') and LOGGING_CONFIG.get('print_rewards'):
        print(f"🎁 Reward: success {success_before:.2%} -> {success_after:.2%} "
              f"(gain={success_gain:.2%}), cost=${cost_incurred:.4f}, reward={reward:.4f}")
    
    return reward


# ============================================================================
# GLOBAL BANDIT INSTANCE
# ============================================================================

_bandit_instance: Optional[ContextualBandit] = None


def get_bandit() -> ContextualBandit:
    """Get or create the global bandit instance."""
    global _bandit_instance
    if _bandit_instance is None:
        _bandit_instance = ContextualBandit()
    return _bandit_instance


def reset_bandit(workflow: str = None, baseline: str = None, 
                 load_saved: bool = True) -> ContextualBandit:
    """
    Reset the global bandit for a new experiment.
    
    Args:
        workflow: Workflow name
        baseline: Baseline name
        load_saved: Whether to load previously saved state
        
    Returns:
        The bandit instance
    """
    global _bandit_instance
    
    if _bandit_instance is None:
        _bandit_instance = ContextualBandit()
    
    _bandit_instance.reset(workflow, baseline)
    
    if load_saved:
        _bandit_instance.load_state()
    
    return _bandit_instance


def save_bandit_state():
    """Save the global bandit state."""
    if _bandit_instance is not None:
        _bandit_instance.save_state()
