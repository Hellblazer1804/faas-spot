#!/usr/bin/env python3
"""
Test script for RL Adaptive Scaler modules.

Run this to validate that the RL modules are working correctly
without requiring the full Kubernetes infrastructure.

Usage:
    cd Scaler
    python test_rl_modules.py
"""

import sys
import os
import numpy as np

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_rl_config():
    """Test rl_config.py"""
    print("=" * 60)
    print("Testing rl_config.py")
    print("=" * 60)
    
    from rl_config import (
        ACTIONS, N_ACTIONS, N_FEATURES, 
        COLD_START_CONFIG, REWARD_CONFIG,
        get_allowed_actions
    )
    
    # Test action definitions
    print(f"\nActions ({N_ACTIONS} total):")
    for action_id, action_info in ACTIONS.items():
        print(f"  {action_id}: {action_info['name']} (scale={action_info['scale_factor']}x)")
    
    # Test get_allowed_actions with different budget levels
    print(f"\nBudget-constrained actions:")
    for budget_frac in [0.05, 0.2, 0.4, 0.7, 1.0]:
        allowed = get_allowed_actions(budget_frac)
        allowed_names = [ACTIONS[a]['name'] for a in allowed]
        print(f"  Budget {budget_frac:.0%} remaining: {allowed_names}")
    
    print(f"\nCold start config: {COLD_START_CONFIG}")
    print(f"Features: {N_FEATURES}")
    
    print("\n✅ rl_config.py tests passed")
    return True


def test_rl_state():
    """Test rl_state.py"""
    print("\n" + "=" * 60)
    print("Testing rl_state.py")
    print("=" * 60)
    
    from rl_state import (
        RLStateTracker, get_state_tracker, reset_state_tracker,
        get_context_vector, is_cold_start_complete
    )
    from rl_config import N_FEATURES
    
    # Reset state tracker
    reset_state_tracker(workflow='wf-test', baseline='test_baseline')
    tracker = get_state_tracker()
    
    print(f"\nState tracker initialized:")
    print(f"  Workflow: {tracker.workflow}")
    print(f"  Baseline: {tracker.baseline}")
    print(f"  Cold start complete: {tracker.cold_start_complete}")
    
    # Test context vector extraction
    print("\nTesting context vector extraction...")
    context = get_context_vector(
        workflow_name='wf-test',
        baseline='test_baseline',
        success_rate=0.7,
        retry_exhausted_count=5,
        retry_exhausted_threshold=12,
        budget_spent=50.0,
        budget_total=100.0,
        elapsed_seconds=300,
        expected_runtime_seconds=600,
        total_pods=30,
        max_pods=100,
    )
    
    print(f"  Context shape: {context.shape}")
    print(f"  Context values: {context}")
    assert context.shape == (N_FEATURES,), f"Expected {N_FEATURES} features, got {context.shape[0]}"
    assert np.all(context >= 0) and np.all(context <= 1), "Context values should be normalized to [0,1]"
    
    # Test cold start detection
    print("\nTesting cold start detection...")
    # Should still be in cold start (not enough observations)
    is_complete = is_cold_start_complete(0.5)
    print(f"  Cold start complete (early): {is_complete}")
    assert not is_complete, "Cold start should not be complete yet"
    
    print("\n✅ rl_state.py tests passed")
    return True


def test_rl_bandit():
    """Test rl_bandit.py"""
    print("\n" + "=" * 60)
    print("Testing rl_bandit.py")
    print("=" * 60)
    
    from rl_bandit import (
        ContextualBandit, get_bandit, reset_bandit,
        save_bandit_state, calculate_reward
    )
    from rl_config import N_ACTIONS, N_FEATURES, ACTIONS
    
    # Create a fresh bandit
    bandit = ContextualBandit(n_actions=N_ACTIONS, n_features=N_FEATURES)
    bandit.reset(workflow='wf-test', baseline='test_baseline')
    
    print(f"\nBandit initialized:")
    print(f"  Actions: {bandit.n_actions}")
    print(f"  Features: {bandit.n_features}")
    
    # Test action selection
    print("\nTesting action selection...")
    np.random.seed(42)
    
    context = np.random.rand(N_FEATURES)
    context = np.clip(context, 0, 1)  # Normalize
    
    # Select action with all actions allowed
    action = bandit.select_action(context, allowed_actions=[0, 1, 2, 3])
    print(f"  Selected action (all allowed): {ACTIONS[action]['name']}")
    
    # Select action with limited actions
    action = bandit.select_action(context, allowed_actions=[0, 1])
    print(f"  Selected action (idle/conservative only): {ACTIONS[action]['name']}")
    assert action in [0, 1], "Action should be from allowed set"
    
    # Test warmup action selection
    warmup_action = bandit.select_warmup_action(context, warmup_actions=[0, 1])
    print(f"  Warmup action: {ACTIONS[warmup_action]['name']}")
    
    # Test reward calculation
    print("\nTesting reward calculation...")
    reward1 = calculate_reward(
        success_before=0.5,
        success_after=0.6,
        cost_incurred=1.0,
        budget_exhausted=False
    )
    print(f"  Reward (success 50% -> 60%, $1 cost): {reward1:.4f}")
    
    reward2 = calculate_reward(
        success_before=0.5,
        success_after=0.4,
        cost_incurred=1.0,
        budget_exhausted=False
    )
    print(f"  Reward (success 50% -> 40%, $1 cost): {reward2:.4f}")
    assert reward1 > reward2, "Positive improvement should have higher reward"
    
    # Test update
    print("\nTesting bandit update...")
    bandit.update(context, action=1, reward=0.5)
    bandit.update(context, action=2, reward=0.3)
    bandit.update(context, action=1, reward=0.6)
    
    stats = bandit.get_action_stats()
    for action_id, stat in stats.items():
        print(f"  Action {stat['name']}: count={stat['count']}, avg_reward={stat['avg_reward']:.4f}")
    
    # Test persistence
    print("\nTesting persistence...")
    test_file = '/tmp/test_rl_bandit_state.json'
    bandit.save_state(test_file)
    print(f"  Saved state to {test_file}")
    
    # Create new bandit and load state
    bandit2 = ContextualBandit(n_actions=N_ACTIONS, n_features=N_FEATURES)
    bandit2.workflow = 'wf-test'
    bandit2.baseline = 'test_baseline'
    loaded = bandit2.load_state(test_file)
    print(f"  Loaded state: {loaded}")
    assert loaded, "State should be loadable"
    
    # Verify loaded state
    stats2 = bandit2.get_action_stats()
    assert stats2[1]['count'] == stats[1]['count'], "Action counts should match after loading"
    print(f"  Verified: action counts match after loading")
    
    # Cleanup
    os.remove(test_file)
    
    print("\n✅ rl_bandit.py tests passed")
    return True


def test_integration():
    """Test integration of all modules"""
    print("\n" + "=" * 60)
    print("Testing Integration")
    print("=" * 60)
    
    from rl_config import ACTIONS, get_allowed_actions, COLD_START_CONFIG
    from rl_state import reset_state_tracker, get_context_vector, is_cold_start_complete
    from rl_bandit import reset_bandit, calculate_reward
    
    # Simulate a workflow run
    print("\nSimulating workflow run...")
    
    # Initialize
    reset_state_tracker(workflow='wf-5', baseline='rl_test')
    bandit = reset_bandit(workflow='wf-5', baseline='rl_test', load_saved=False)
    
    # Simulate 10 ticks
    np.random.seed(123)
    success_rate = 0.3  # Start with low success
    budget_remaining = 1.0
    
    for tick in range(10):
        # Get context
        context = get_context_vector(
            workflow_name='wf-5',
            baseline='rl_test',
            success_rate=success_rate,
            retry_exhausted_count=tick,
            retry_exhausted_threshold=12,
            budget_spent=(1 - budget_remaining) * 100,
            budget_total=100.0,
            elapsed_seconds=tick * 60,
            expected_runtime_seconds=600,
            total_pods=30 + tick * 2,
            max_pods=100,
        )
        
        # Check cold start
        cold_start_complete = is_cold_start_complete(success_rate)
        
        # Get allowed actions based on budget
        allowed = get_allowed_actions(budget_remaining)
        
        # Select action
        if not cold_start_complete:
            action = bandit.select_warmup_action(context, [0, 1])
        else:
            action = bandit.select_action(context, allowed)
        
        # Simulate outcome
        if action > 0:
            success_delta = 0.05 * action  # Higher action = more improvement
            cost = 0.5 * action
            budget_remaining -= 0.05 * action
        else:
            success_delta = 0.01  # cp+retry handles some improvement
            cost = 0.1
        
        new_success = min(1.0, success_rate + success_delta + np.random.randn() * 0.02)
        
        # Calculate reward and update
        reward = calculate_reward(success_rate, new_success, cost, budget_remaining < 0)
        bandit.update(context, action, reward)
        
        print(f"  Tick {tick+1}: action={ACTIONS[action]['name']}, "
              f"success={success_rate:.2%}->{new_success:.2%}, "
              f"reward={reward:.3f}, budget={budget_remaining:.1%}")
        
        success_rate = new_success
    
    # Print final stats
    print("\nFinal action statistics:")
    stats = bandit.get_action_stats()
    for action_id, stat in stats.items():
        print(f"  {stat['name']}: count={stat['count']}, avg_reward={stat['avg_reward']:.4f}")
    
    print("\n✅ Integration tests passed")
    return True


def main():
    """Run all tests"""
    print("=" * 60)
    print("RL Adaptive Scaler Module Tests")
    print("=" * 60)
    
    tests = [
        ("rl_config", test_rl_config),
        ("rl_state", test_rl_state),
        ("rl_bandit", test_rl_bandit),
        ("integration", test_integration),
    ]
    
    results = []
    for name, test_fn in tests:
        try:
            result = test_fn()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} test FAILED: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All tests passed! RL modules are working correctly.")
        print("\nTo use RL mode, run the scaler with --rl-mode flag:")
        print("  python preemptive_scaler.py --workflow wf-5 --rl-mode ...")
        return 0
    else:
        print("\n⚠️ Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
