# FaaS-on-Spot: Component Reference

This document describes the five core components of FaaS-on-Spot: Ranker, Checkpointer, Scaler, Retry Service, and Emulator. For instructions on running experiments, see [README.md](../README.md).

---

## Table of Contents

1. [Ranker](#ranker)
2. [Checkpointer](#checkpointer)
3. [Scaler](#scaler)
4. [Retry Service](#retry-service)
5. [Emulator](#emulator)

---

## Ranker

**Location:** `task-ranker/`  
**Entry point:** `task-ranker/heft_rank_identifier.py`

### Purpose

The Ranker performs offline analysis of a workflow's task graph and decides which tasks should be replicated (scaled up) on spot instances, and by how much. It uses the HEFT (Heterogeneous Earliest Finish Time) algorithm to score each task by its criticality to the overall makespan, then applies a hybrid budget-aware strategy to assign scaling factors.

This runs once before an experiment starts. Its output (`metadata.json`) is consumed by the Scaler at runtime.

### How It Works

1. **HEFT ranking:** For each task, the upward rank is computed as its own execution time plus the maximum rank of any successor. Tasks on the critical path receive the highest ranks.

2. **Hybrid scaling assignment:** Given a spot budget, tasks are ranked by their HEFT score. Tasks with the highest ranks (most critical to makespan) are prioritized for scale-out. A cap of `2.5×` prevents over-provisioning any single task.

3. **Kubernetes patching:** After computing scaling factors, the Ranker patches the corresponding ConfigMaps in Kubernetes (`IS_SCALABLE`, `SCALING_FACTOR`) and triggers a rollout restart so Fission picks up the new replica counts.

4. **Metadata output:** Writes `metadata.json` into the workflow directory for downstream components.

### Key Functions

| Function | Description |
|---|---|
| `calculate_heft_ranks(tasks)` | Returns `{task: rank}` — higher rank = more critical |
| `assign_scaling_with_hybrid_strategy(tasks, ranks, spot_budget)` | Returns per-task dict with `scaling_factor`, `is_scalable`, `heft_rank` |
| `patch_configmap_kv(name, key, value)` | Updates a Kubernetes ConfigMap key |
| `rollout_restart_task(task_name)` | Triggers a `kubectl rollout restart` for the task's deployment |

### Inputs & Outputs

**Input — `tasks.json`** (per workflow):
```json
{
  "tasks": {
    "task1": { "exec_time": 1, "successors": ["task2", "task3"], "minscale": 1 },
    "task2": { "exec_time": 4, "successors": ["task4"], "minscale": 1 }
  }
}
```

**Output — `metadata.json`**:
```json
{
  "task1": { "scaling_factor": 1.0, "is_scalable": false, "heft_rank": 2.1 },
  "task2": { "scaling_factor": 2.2, "is_scalable": true,  "heft_rank": 5.8 }
}
```

### CLI Reference

```
python3 task-ranker/heft_rank_identifier.py
  --workflow       wf-1                 # Workflow name
  --budget         300                  # Spot budget in dollars
  --workflows-dir  test-runner/workflows # Directory containing workflow subdirs
  --baseline       ours                 # Baseline label
```

---

## Checkpointer

**Location:** `checkpoint-planner/`  
**Entry point:** `checkpoint-planner/checkpointer.py`

### Purpose

The Checkpointer performs offline checkpoint placement planning. Given a workflow DAG and historical spot instance lifetime data (as a CDF), it decides which tasks should be checkpointed and assigns each a priority level. The goal is to minimize expected recovery time if a spot instance is preempted, while keeping checkpoint overhead within a budget fraction.

The resulting plan is stored in MySQL and read by the Retry Service at runtime to decide where to resume failed executions.

### How It Works

1. **Critical path analysis:** A forward pass computes earliest start times; a backward pass identifies which tasks are on the critical path. Critical-path tasks are higher priority for checkpointing.

2. **Survival curve integration:** The CDF file gives the probability that a spot instance survives to time `t`. Tasks that execute when survival probability is low are at higher risk and are preferentially checkpointed.

3. **Budget-constrained DP:** For linear task chains, dynamic programming finds the checkpoint placement that minimizes expected re-execution cost subject to a budget fraction (default: 20% of total execution time overhead).

4. **Progressive checkpointing:** Extra checkpoints are added in the late stages of a workflow (depth ratio > 0.67) and immediately before expensive bottleneck tasks to ensure no large block of work is unprotected.

5. **Level assignment:** Each checkpointed task gets a level (1, 2, or 3) based on how many risk factors it has — execution time, position on critical path, stage depth, and branching factor.

### Checkpoint Levels

| Level | Name | Criteria |
|---|---|---|
| 1 | Standard | Selected by DP placement or progressive rules |
| 2 | Critical | Expensive task (`exec_time > 4.0s`), late stage, or on critical path |
| 3 | Saga | Multiple risk factors combined, or a branch/join point |

### Key Functions

| Function | Description |
|---|---|
| `compute_checkpoint_set(tasks_json_path, cdf_path, ...)` | Main entry: returns `(checkpoint_tasks, levels, task_depths)` |
| `compute_critical_path_tasks(tasks)` | Returns set of tasks on the critical path |
| `optimize_linear_path(chain, task_time, ...)` | DP optimizer for linear chains |
| `compute_checkpoint_levels(tasks, checkpointed, task_depths)` | Assigns levels 1–3 |
| `compute_progressive_checkpoints(tasks, checkpointed, task_depths)` | Adds late-stage and pre-bottleneck checkpoints |
| `add_checkpoints_before_risky_tasks(tasks, checkpointed, ...)` | Ensures coverage before high-cost tasks |
| `write_plan(workflow, baseline, checkpoint_tasks, levels)` | Writes plan to `workflow_checkpoint_plan` in MySQL |

### Inputs & Outputs

**Required files:**
- `{workflows_dir}/{workflow}/tasks.json` — task graph
- `checkpoint-planner/spot-traces-csv/{az}_{instance}_cdf.csv` — spot lifetime CDF

**CDF format:**
```csv
Lifetime,CDF
60,0.10
300,0.50
1800,0.90
```

**Output (printed and optionally written to DB):**
```
Checkpoint plan for wf-1:
  task2  level=2  (critical path, expensive)
  task4  level=1  (standard)
  task6  level=3  (saga: branch point)
```

### CLI Reference

```
python3 checkpoint-planner/checkpointer.py
  --workflows-dir     test-runner/workflows  # Workflow definitions
  --workflow          wf-1                   # Workflow name
  --az                us-west-2a             # Availability zone for CDF lookup
  --instance          v100                   # Instance type for CDF lookup
  --checkpoint-factor 0.2                    # Max checkpoint overhead (fraction)
  --recovery-time     1.0                    # Estimated recovery overhead (seconds)
  --baseline          ours                   # Baseline label for DB write
  --write-plan-to-db                         # Persist plan to MySQL (flag)
```

### Configuration Constants

| Constant | Default | Description |
|---|---|---|
| `CRITICAL_EXEC_TIME_THRESHOLD` | `4.0` | Exec time above which a task is "expensive" |
| `EARLY_STAGE_DEPTH_RATIO` | `0.33` | First 33% of workflow depth |
| `LATE_STAGE_DEPTH_RATIO` | `0.67` | Last 33% — progressive checkpoints enforced here |

---

## Scaler

**Location:** `adaptive-scaler/`  
**Entry point:** `adaptive-scaler/preemptive_scaler.py`  
**Supporting modules:** `adaptive-scaler/rl_bandit.py`, `adaptive-scaler/rl_config.py`, `adaptive-scaler/rl_state.py`

### Purpose

The Scaler monitors a running workflow and adaptively adjusts replica counts using a contextual Thompson Sampling bandit. The objective is to boost success rates above a per-workflow target when the Checkpointer + Retry Service alone are insufficient, without exceeding a cost budget.

The Scaler operates continuously in a polling loop (every 30 seconds) alongside the Retry Service throughout the workflow's lifetime.

### How It Works

1. **State observation:** Every tick, the Scaler queries MySQL for the current success rate, retry exhaustion count, elapsed time, and pod count. These form a 6-dimensional context vector.

2. **Cold start phase:** At startup, the Scaler observes but restricts its actions. Simple workflows (wf-1–4, 7) have a 30-minute cold start during which the Scaler remains idle (action 0 only), allowing the Retry Service to handle recovery first. Complex workflows (wf-4, 6, 8, 9) use a 2-minute cold start to enable faster intervention.

3. **Action selection:** A Thompson Sampling bandit samples reward estimates for each allowed action and selects the highest. Budget remaining determines which actions are permitted (see budget thresholds below).

4. **Scaling execution:** The selected action's `scale_factor` is multiplied against the task's baseline `scaling_factor` from `metadata.json`, then applied to Kubernetes deployments via `kubectl scale`.

5. **Reward computation:** After an observation window (60 seconds), the change in success rate divided by cost incurred is the reward signal. The bandit updates its posterior for the (context, action) pair.

6. **Persistence:** Bandit state (posterior parameters) is saved to `adaptive-scaler/rl-state-files/rl_state_{workflow}_{baseline}.json` after each update so state is preserved across restarts. The `rl-state-files/` directory is created at runtime.

### Action Space

| Action | Name | Scale factor | When used |
|---|---|---|---|
| 0 | idle | 1.0× | Default; cp+retry handling well |
| 1 | conservative | 1.1× | Minor intervention (10% scale-out) |
| 2 | moderate | 1.3× | Balanced intervention (30% scale-out) |
| 3 | aggressive | 1.5× | Strong intervention (50% scale-out) |

### Context Features (6-dimensional)

| Index | Feature | Range |
|---|---|---|
| 0 | `success_rate` | 0 – 1 |
| 1 | `success_rate_delta` | −0.5 – 0.5 |
| 2 | `retry_exhausted_ratio` | 0 – 1 |
| 3 | `normalized_cost` | 0 – 2 (can exceed 1 if over budget) |
| 4 | `time_fraction` | 0 – 1 |
| 5 | `total_pods_normalized` | 0 – 1 |

### Budget Action Thresholds

| Budget remaining | Allowed actions |
|---|---|
| < 10% | idle only |
| < 20% | idle, conservative |
| < 35% | idle, conservative, moderate |
| ≥ 35% | all four actions |

### Target Success Rates

| Workflow class | Workflows | Target |
|---|---|---|
| Simple | wf-1, 2, 3, 4, 7 | 97.5% |
| Complex | wf-4, 6, 8, 9 | 67.5% |

### Budget Defaults

| Workflow | Budget (% of on-demand) |
|---|---|
| wf-4, wf-8 | 60% |
| All others | 50% |

### Key Classes & Functions

| Symbol | Description |
|---|---|
| `ContextualBandit` (`rl_bandit.py`) | Thompson Sampling bandit; `select_action(ctx)`, `update(ctx, a, r)`, `save_state()` |
| `ScalerState` (`rl_state.py`) | Tracks observations, rewards, and history |
| `get_cold_start_config(workflow)` (`rl_config.py`) | Returns workflow-specific cold start settings |
| `get_allowed_actions(budget_fraction)` (`rl_config.py`) | Returns list of permitted action indices |

### CLI Reference

```
python3 adaptive-scaler/preemptive_scaler.py
  --workflows-dir   test-runner/workflows  # Workflow definitions
  --workflow        wf-1                   # Workflow name
  --baseline        ours                   # Baseline label
  --experiment-tag  main_experiment        # DB tag (can also use EXPERIMENT_TAG env)
```

### Unit Tests

```bash
cd adaptive-scaler
python3 test_rl_modules.py
```

Tests cover: action selection determinism, context normalization, Thompson Sampling posterior updates, cold-start activation logic, budget gate enforcement, and state persistence round-trips.

---

## Retry Service

**Location:** `retry-manager/`  
**Entry point:** `retry-manager/retry_service.py`  
**Alternative:** `retry-manager/retry_service_with_http_health.py` (adds `/health` HTTP endpoint)

### Purpose

The Retry Service monitors workflow executions in MySQL and retries failed tasks from the most recent checkpoint, rather than from the beginning of the workflow. This is the primary recovery mechanism. The Scaler acts as a secondary mechanism that reduces failure frequency, but the Retry Service handles recovery when failures do occur.

The Retry Service runs as a long-lived Kubernetes pod throughout the experiment.

### How It Works

1. **Polling loop:** Every 5 seconds, the service queries `serverless_workflows` for incomplete UIDs — workflow invocations that have not reached a terminal success state.

2. **Resumption point selection:** For each incomplete UID, the service checks `workflow_checkpoint_plan` (written by the Checkpointer) to find the latest checkpoint that the UID has successfully passed. The retry resumes from that task.

3. **Priority-ordered retries:** Tasks with Saga-level checkpoints (level 3) are retried concurrently. Critical-level (level 2) tasks get higher queue priority. Standard (level 1) retries are queued normally.

4. **Retry limits:** Simple workflows allow 12–18 retries per UID. Complex workflows (wf-4, 6, 8, 9) allow 25–35 retries because the Retry Service is the primary recovery mechanism for deep DAGs where preemptions are more frequent.

5. **Adaptive backoff:** On repeated failures, backoff grows exponentially from 30 seconds up to 300 seconds, with jitter to prevent thundering herd.

6. **Skip completed UIDs:** The service is idempotent — UIDs that already succeeded are skipped immediately.

### Retry Limits by Workflow

| Workflow class | Max retries per UID |
|---|---|
| Simple (wf-1–4, 7) | 12–18 |
| Complex (wf-4, 6, 8, 9) | 25–35 |

### Key Functions

| Function | Description |
|---|---|
| `determine_next_tasks(tasks_data, completed_set)` | Returns tasks whose predecessors are all complete |
| `find_resumption_point(uid, workflow, baseline, checkpointed_tasks)` | Returns the task to retry from |
| `execute_retry(uid, workflow, baseline, resume_task)` | Sends retry request to Fission router; returns `(success, message)` |

### Kubernetes Deployment

**Deploy with health checks (recommended):**

```bash
kubectl apply -f retry-manager/kube/deployment.yaml
```

The pod uses `health_check.py` as its liveness and readiness probe. This script verifies Python functionality, the `/app/workflows` directory, required module imports, and environment variables.

**Deploy without health checks:**

```bash
kubectl apply -f retry-manager/kube/deployment-no-health-checks.yaml
```

**Required Kubernetes resources:**

- ConfigMap `retry-config` — runtime configuration (workflow name, baseline, poll interval)
- Secret `mysql-auth` — database credentials
- Secret `redis-auth` — Redis credentials (optional, for distributed state)
- Namespace `retry`

**Troubleshoot a failing pod:**

```bash
kubectl logs -n retry deployment/retry-service
kubectl exec -n retry deployment/retry-service -- python3 /app/health_check.py
```

### CLI Reference (local run)

```
python3 retry-manager/retry_service.py
  --workflow      wf-1   # Workflow name
  --baseline      ours   # Baseline label
  --poll-interval 5      # Seconds between DB polls
  --max-retries   12     # Max retry attempts per UID
```

### Environment Variables

| Variable | Description |
|---|---|
| `MYSQL_HOST` | Database host |
| `MYSQL_USER` | Database user |
| `MYSQL_PASSWORD` | Database password |
| `MYSQL_DB` | Database name |
| `WORKFLOW_DIR` | Path to workflows directory |
| `FISSION_ROUTER_PREFIX` | Fission router base URL |
| `EXPERIMENT_TAG` | Tag written to `checkpoint_retries.notes` |

---

## Emulator

**Location:** `spot-emulator/`  
**Entry point:** `spot-emulator/Emulator.py`

### Purpose

The Emulator simulates the behavior of a real spot instance environment without requiring actual AWS spot instances. It reads historical spot lifetime data (as CDFs), assigns pod lifetimes stochastically, injects preemptions by deleting Kubernetes pods at the appropriate time, and logs cost based on pod type and duration.

The Emulator enables reproducible experiments: the same CDF produces statistically equivalent preemption patterns, and time is compressed (1 real second = 60 emulated seconds) so that multi-hour spot scenarios complete in minutes.

### How It Works

1. **Pod type assignment:** When new pods appear (via Kubernetes watch), the Emulator classifies them as `spot`, `burstable`, or `on-demand` based on a configurable protection ratio (0.2–0.8). Spot pods are eligible for preemption; on-demand and burstable pods are not.

2. **Lifetime sampling:** Each spot pod is assigned a lifetime drawn from the CDF. The CDF is sampled at percentile breakpoints (deflection points) to reproduce the historical distribution.

3. **Machine packing:** Pods are grouped into virtual machines (3 pods per machine by default). When a machine is "preempted", all its pods are deleted together, mirroring real spot instance behavior.

4. **Preemption injection:** When a spot pod's assigned lifetime expires (in emulator time), the Emulator calls `kubectl delete pod` to simulate the preemption. The Retry Service detects the resulting task failure and initiates recovery.

5. **Cost logging:** Pod uptime and type are recorded in the `cost_logs` MySQL table using pricing from `spot-emulator/tracing/spot-cost-csv/{az}_{instance}_cost.csv`. The Scaler reads these logs to compute normalized cost for its context features.

6. **Time compression:** All timing is multiplied by `EMULATOR_TIME_COMPRESSION = 60`. A CDF entry of 300 seconds (5 minutes) becomes a 5-second event in real time.

### Pod Types

| Type | Preemptable | Pricing source |
|---|---|---|
| `spot` | Yes | Spot price from cost CSV |
| `burstable` | No | Burstable price from cost CSV |
| `on-demand` | No | On-demand price from cost CSV |

### Key Functions

| Function | Description |
|---|---|
| `load_avg_prices(az, instance)` | Returns `(spot_price, ondemand_price, burstable_price)` from cost CSV |
| `get_deflection_points(xs, cdf, percentiles)` | Returns lifetime thresholds at given CDF percentiles |
| `get_pod_age_seconds(pod)` | Returns pod uptime in (emulated) seconds |
| `inject_preemption(pod_name)` | Deletes a pod to simulate spot preemption |
| `ensure_cost_table()` | Creates `cost_logs` table if it doesn't exist |

### Data Files

| File pattern | Location | Description |
|---|---|---|
| `{az}_{instance}_cdf_survival_curve.json` | `adaptive-scaler/survival_curves/` | Survival curve for Scaler KM threshold (generated by `adaptive-scaler/generate_survival_curve.py`) |
| `{az}_{instance}_cdf.csv` | `checkpoint-planner/spot-traces-csv/` | CDF used by Checkpointer |
| `{az}_{instance}_1_cdf.csv` | `spot-emulator/tracing/spot-traces-csv/` | CDF used by Emulator (directory must be created and populated) |
| `{az}_{instance}_cost.csv` | `spot-emulator/tracing/spot-cost-csv/` | Pricing per pod type (directory must be created and populated) |

### Emulator CLI Reference

```text
python3 spot-emulator/Emulator.py
  --workflow                  wf-1          # Workflow name
  --baseline                  ours          # Baseline label
  --az                        us-west-2a    # Availability zone
  --instance                  v100          # Instance type
  --pods-per-machine          3             # Virtual machine packing ratio
  --emulator-interval-seconds 5             # How often to check pod ages (seconds)
```

### Emulator Configuration

| Parameter | Default | Description |
| --- | --- | --- |
| `PODS_PER_MACHINE` | `3` | Pods grouped per virtual machine |
| `EMULATOR_TIME_COMPRESSION` | `60` | 1 real second = 60 emulated seconds |
| Protection ratio | `0.2–0.8` | Dynamic fraction of pods that are spot vs. protected |

---

## Component Interaction Diagram

```text
                  ┌─────────────┐
                  │  tasks.json │  (per workflow)
                  └──────┬──────┘
                         │
              ┌──────────┴────────────┐
              │                       │
         ┌────▼────┐            ┌─────▼──────┐
         │  Ranker  │            │ Checkpointer│
         │  (HEFT)  │            │  (offline)  │
         └────┬────┘            └─────┬───────┘
              │ metadata.json         │ checkpoint plan
              │                       │ (→ MySQL)
              └──────────┬────────────┘
                         │
                  ┌──────▼──────────────────────────────┐
                  │          test_runner.py              │
                  │  (orchestrates the live experiment)  │
                  └──┬──────────┬────────────┬──────────┘
                     │          │            │
              ┌──────▼──┐  ┌───▼───┐  ┌─────▼────────┐
              │Emulator  │  │Scaler │  │ Retry Service│
              │(preempt) │  │ (RL)  │  │  (recovery)  │
              └──────┬───┘  └───┬───┘  └─────┬────────┘
                     │          │             │
                     └──────────┴─────────────┘
                                │
                         ┌──────▼──────┐
                         │    MySQL    │
                         │  (wms DB)   │
                         └─────────────┘
```

All five components share the same MySQL database. The Ranker and Checkpointer are offline planners that run before the experiment. The Emulator, Scaler, and Retry Service run concurrently during the experiment, coordinating through the database.

`test_runner.py` lives in `test-runner/test_runner.py`.
