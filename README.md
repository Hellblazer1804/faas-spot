# FaaSpot

**Fault-Tolerant Serverless Workflow Execution on Spot Instances**

> Paper: FaaSpot: Fault-Tolerant Serverless Workflow Execution on Spot Instances  
> Accepted at ACM Middleware 2026

---

## Developers
Bandhan Patel --> bandhanpatel1804@gmail.com 

Sushruth Harsha --> hsushruth@gmail.com

---
## Overview

Serverless workflows on spot instances fail frequently — complex DAG workflows see as low as 13% completion when deployed naively on spot VMs. A single preemption at a fan-out or synchronization barrier cascades into every parallel branch that shares it, turning one failure into many.

FaaSpot addresses this by analyzing the workflow DAG **once at submission time** and classifying each task by the cascade impact of its preemption. This single offline classification drives three online fault-tolerance mechanisms — selective checkpointing, survival-aware scaling, and lineage-based retry — all running entirely on spot instances with no on-demand fallback.

**Results across 8 DAG workflows:** 89.03% average success rate at $0.018 per successful run — 21 pp above the best baseline, at less than half the cost, with 2.3× lower mean latency.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              (1) Offline DAG Analysis                │
│                                                      │
│   Workflow DAG ──► Task Ranker ──► κ(v)              │
│                         │                            │
│                         └──► Checkpoint Planner      │
│                                     │                │
│                                     ▼                │
│                           Tier: CRITICAL / MEDIUM /  │
│                                   LOW  ──► ℓ(v)      │
└──────────────────────────┬──────────────────────────┘
                           │  κ(v), ℓ(v)
┌──────────────────────────▼──────────────────────────┐
│          (2) Online Fault-Tolerance Controllers      │
│                                                      │
│  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │ Checkpoint Mgr   │  │    Adaptive Scaler        │  │
│  │  ℓ(v)-based CP   │  │  KM Survival Model +      │  │
│  │  at CRITICAL /   │  │  HEFT-ordered scale-out   │  │
│  │  MEDIUM tasks    │  │  (proactive/warning/      │  │
│  └─────────┬────────┘  │   critical tiers)         │  │
│            │           └────────────┬─────────────┘  │
│            │  checkpoints           │ new replicas   │
│            └─────────────┐          │                │
│                          ▼          ▼                │
│                   ┌─────────────────────┐            │
│                   │    Retry Manager    │            │
│                   │  Lineage-based retry│            │
│                   │  from nearest CP    │            │
│                   └─────────────────────┘            │
│                                                      │
│   Execution plane: Kubernetes + Fission (spot VMs)   │
└─────────────────────────────────────────────────────┘
```

---

## Repository Layout

| Directory | Paper component | Description |
|-----------|----------------|-------------|
| `task-ranker/` | §4.1 Task Ranker | HEFT-based criticality score κ(v) per task |
| `checkpoint-planner/` | §4.1 Checkpoint Planner | Offline tier assignment ℓ(v) · [docs](docs/checkpoint-classification-policy.md) |
| `checkpoint-manager/` | §4.2 Checkpoint Manager | Runtime selective checkpointing |
| `adaptive-scaler/` | §4.3 Adaptive Scaler | KM survival model + state-based scale-out · [docs](docs/scaler-optimizations.md) |
| `retry-manager/` | §4.4 Retry Manager | Lineage-based retry with adaptive batch sizing · [docs](docs/retry-manager.md) |
| `spot-emulator/` | §5.2 Spot Emulator | Trace-driven spot preemption emulator |
| `load-generator/` | — | Workload generators for each workflow type |
| `test-runner/` | §5.1 | Experiment orchestrator, deploy scripts, and custom workflow creator |
| `experiments/` | §6 Evaluation | Baseline evaluation — Snape, Hourglass, MScheduler, Burst-HADS |
| `docs/` | — | All documentation |

---

## Prerequisites

- Python 3.8+
- Kubernetes cluster with `kubectl` configured
- [Fission](https://fission.io/) deployed on the cluster with functions registered
- MySQL 8.x (any host — configure via `.env`)
- Redis (required by `retry-manager`)

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/faas-spot.git
cd faas-spot

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env: fill in DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, ROUTER_BASE_URL

# 4. Run offline DAG analysis
python3 task-ranker/heft_rank_identifier.py --workflow wf-4
python3 checkpoint-planner/checkpointer.py --workflow wf-4

# 5. Run experiments
python3 test-runner/test_runner.py --workflows wf-4
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your values — **never commit `.env`**.

### Infrastructure

| Variable | Description |
| --- | --- |
| `DB_HOST` | MySQL host (default: `localhost`) |
| `DB_USER` | MySQL username |
| `DB_PASSWORD` | MySQL password |
| `DB_NAME` | MySQL database name |
| `ROUTER_BASE_URL` | Fission router NodePort URL, e.g. `http://<node-ip>:32363` |
| `REQUEST_RECORDER_URL` | Request recorder URL (load-generator only) |
| `DISCORD_WEBHOOK_URL` | Discord webhook for notifications (optional) |
| `EXPERIMENT_TAG` | Tag applied to all DB rows from this run |

### Workflow Behavior

All components read these; set once in `.env`, applies everywhere.

| Variable | Default | Description |
| --- | --- | --- |
| `WF_TARGET_SUCCESS_RATE` | `0.85` | Fraction of requests that must succeed (0–1). Lower = accept more failures before intervening. |
| `WF_MAX_RETRIES` | `20` | Max retry attempts per UID before giving up. |
| `WF_MIN_BACKOFF_SEC` | `10` | Retry backoff floor (seconds). |
| `WF_MAX_BACKOFF_SEC` | `240` | Retry backoff ceiling (seconds). |
| `WF_BACKOFF_MULTIPLIER` | `1.3` | Exponential backoff growth rate. |
| `WF_BUDGET_PERCENT` | `0.50` | Scaler budget as a fraction of estimated on-demand cost. |
| `WF_ESTIMATED_RUNTIME_HOURS` | `1.5` | Expected experiment duration — used to compute the budget ceiling. |
| `WF_ESTIMATED_MACHINES` | `8` | Expected machine count — used to compute the budget ceiling. |
| `WF_COLD_START_MINUTES` | `5` | Minutes the scaler stays idle at startup to let the retry service handle early failures first. |

---

## Workflow Suite

Eight DAG workflows from four application domains (§6.1 of the paper):

| ID | Name | Domain | Structure |
| --- | --- | --- | --- |
| wf-1 | Chain | Synthetic | Simple sequential chain |
| wf-2 | Fork-Join | Synthetic | Single fan-out / fan-in |
| wf-3 | 1000Genome | Genomics | Large fan-out / fan-in stages |
| wf-4 | Epigenomics | Genomics | Multiple synchronization barriers |
| wf-5 | LIGO | Physics | Parallel sub-pipelines with barriers |
| wf-6 | SRAsearch | Bioinformatics | Two-branch DAG with parallel tasks |
| wf-7 | CyberShake | Seismology | Wide DAG with repeated fan-out/fan-in |
| wf-8 | Montage | Astronomy | Three sub-pipelines with deep barriers |

DAG definitions and deploy scripts live in `test-runner/workflows/`.

---

## Custom Workflows

You can run FaaSpot against any DAG workflow you define — not just the 8 built-in ones.

### 1. Define your workflow

Create a `tasks.json` describing the DAG:

```json
{
  "tasks": {
    "task1": { "exec_time": 2.5, "successors": ["task2", "task3"], "minscale": 1 },
    "task2": { "exec_time": 4.0, "successors": ["task4"],          "minscale": 2 },
    "task3": { "exec_time": 3.0, "successors": ["task4"],          "minscale": 2 },
    "task4": { "exec_time": 1.5, "successors": [],                 "minscale": 1 }
  }
}
```

| Field | Description |
| --- | --- |
| `exec_time` | Simulated task duration in seconds (float) |
| `successors` | Task IDs this task forwards to — empty list for leaf tasks |
| `minscale` | Minimum Fission replica count; controls parallelism |

### 2. Scaffold and deploy

```bash
python3 test-runner/create_workflow.py \
  --name  my-wf \
  --tasks path/to/tasks.json \
  --run-ranker \
  --run-checkpointer
```

This validates the DAG, generates all deploy scripts, and runs the offline HEFT analysis and checkpoint planning. Then deploy to Fission:

```bash
bash test-runner/workflows/my-wf/deploy_ours.sh
```

### 3. Run the experiment

```bash
python3 test-runner/test_runner.py --workflows my-wf
```

All three fault-tolerance mechanisms (checkpointing, adaptive scaling, lineage-based retry) apply to your custom workflow exactly as they do to the built-in ones.

**Full CLI reference:**

```text
python3 test-runner/create_workflow.py
  --name               my-wf           # Workflow ID (used as directory name and Fission route)
  --tasks              tasks.json      # Path to your tasks.json
  --baseline           ours            # Baseline label (default: ours)
  --az                 us-west-2a      # Availability zone for spot trace lookup
  --instance           v100            # Instance type for spot trace lookup
  --budget             300             # Spot budget in dollars passed to the ranker
  --run-ranker                         # Run HEFT Task Ranker (writes metadata.json)
  --run-checkpointer                   # Run Checkpoint Planner (writes plan to MySQL)
```

---

## Baselines

Evaluated against four state-of-the-art systems (see `experiments/`):

- **Snape** — spot/on-demand ratio adjustment via eviction prediction and RL
- **Hourglass** — temporal slack as a risk budget, falls back to on-demand
- **MScheduler** — application-native restart + cost-based instance selection
- **Burst-HADS** — bag-of-tasks scheduling across spot and burstable instances

---

## Kubernetes Deployment

The `retry-manager` runs as a Kubernetes Deployment. See [docs/retry-manager.md](docs/retry-manager.md) for instructions, including how to create the required MySQL and Redis Kubernetes Secrets before applying `retry-manager/kube/deployment.yaml`.

---

## Spot Trace Data

The `spot-emulator` uses two public datasets:

- **SkyPilot** availability traces — spot instance lifetimes across AWS regions and instance types
- **SpotLake** — historical spot pricing

Trace CSV files must be placed in `spot-emulator/tracing/spot-traces-csv/` and `spot-emulator/tracing/spot-cost-csv/` before running experiments.

---

## Discord Notifications

Set `DISCORD_WEBHOOK_URL` in `.env` to receive experiment progress notifications in a Discord channel. See [docs/discord-integration.md](docs/discord-integration.md).

---

## Documentation

| Document | Description |
|----------|-------------|
| [System Components](docs/components.md) | Per-component architecture overview |
| [Checkpoint Classification Policy](docs/checkpoint-classification-policy.md) | CRITICAL / MEDIUM / LOW tier rules |
| [Checkpointer Algorithm](docs/checkpointer-algorithm.md) | Offline DAG analysis walkthrough |
| [Scaler Optimizations](docs/scaler-optimizations.md) | KM survival model and RL bandit design |
| [Retry Manager](docs/retry-manager.md) | Deployment and configuration |
| [Retry Service Architecture](docs/retry-service-architecture.md) | Internal architecture |
| [Discord Integration](docs/discord-integration.md) | Setting up notifications |

---

## License

MIT License. See [LICENSE](LICENSE).

-- 

