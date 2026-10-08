# Checkpoint Classification Policy

## Overview

The Checkpoint Planner classifies each task into three priority levels based on a **single signal: minscale**.

The policy is simple:
- **CRITICAL**: Tasks at parallelism boundaries (minscale transitions)
- **MEDIUM**: Tasks in parallel zones (minscale > 1, no transition)
- **LOW**: Tasks in sequential zones (minscale = 1, no transition)

---

## Classification Rules

| Level | Rule | Meaning |
|-------|------|---------|
| **CRITICAL** | Any successor has minscale ≠ this task's minscale | Parallelism boundary: work expands (fan-out) or contracts (fan-in) |
| **MEDIUM** | minscale > 1 AND no transition to successor | Inside a parallel zone; multiple instances of this task run |
| **LOW** | minscale = 1 AND no transition | Purely sequential; single instance runs |

---

## Examples

### wf-1 — Pure sequential (all minscale = 1)

```
task1(ms=1) → task2(ms=1) → task3(ms=1) → task4(ms=1)
```

No transitions, all minscale = 1:

| Task  | minscale | Transition | Classification |
|-------|----------|------------|----------------|
| task1 | 1        | none       | LOW            |
| task2 | 1        | none       | LOW            |
| task3 | 1        | none       | LOW            |
| task4 | 1        | none       | LOW            |

---

### wf-2 — Sandwich (1 → 3 → 3 → 3 → 1)

```
task1(ms=1) → task2(ms=3) → task3(ms=3) → task4(ms=3) → task5(ms=1)
```

| Task  | minscale | Successor minscale | Transition | Classification |
|-------|----------|-------------------|------------|----------------|
| task1 | 1        | 3                  | 1 → 3      | **CRITICAL**   |
| task2 | 3        | 3                  | none       | **MEDIUM**     |
| task3 | 3        | 3                  | none       | **MEDIUM**     |
| task4 | 3        | 1                  | 3 → 1      | **CRITICAL**   |
| task5 | 1        | —                  | none       | **LOW**        |

---

### wf-6 — Decreasing (6 → 6 → 2 → 1)

```
task1(ms=6) → task2(ms=6) → task3(ms=2) → task4(ms=1)
```

| Task  | minscale | Successor minscale | Transition | Classification |
|-------|----------|-------------------|------------|----------------|
| task1 | 6        | 6                  | none       | **MEDIUM**     |
| task2 | 6        | 2                  | 6 → 2      | **CRITICAL**   |
| task3 | 2        | 1                  | 2 → 1      | **CRITICAL**   |
| task4 | 1        | —                  | none       | **LOW**        |

---

### wf-4 — Long workflow with middle plateau (2 → 6 → 6 → 6 → 6 → 2 → 1 → 1 → 1)

```
task1(ms=2) → task2(ms=6) → task3(ms=6) → task4(ms=6) → task5(ms=6) → task6(ms=2) → task7(ms=1) → task8(ms=1) → task9(ms=1)
```

| Task  | minscale | Successor minscale | Transition | Classification |
|-------|----------|-------------------|------------|----------------|
| task1 | 2        | 6                  | 2 → 6      | **CRITICAL**   |
| task2 | 6        | 6                  | none       | **MEDIUM**     |
| task3 | 6        | 6                  | none       | **MEDIUM**     |
| task4 | 6        | 6                  | none       | **MEDIUM**     |
| task5 | 6        | 2                  | 6 → 2      | **CRITICAL**   |
| task6 | 2        | 1                  | 2 → 1      | **CRITICAL**   |
| task7 | 1        | 1                  | none       | **LOW**        |
| task8 | 1        | 1                  | none       | **LOW**        |
| task9 | 1        | —                  | none       | **LOW**        |

---

## Why Minscale?

Minscale directly maps to recovery cost:

1. **CRITICAL (transitions)**: Losing a task at a minscale transition means:
   - Fan-out (scale up): Re-run all expanded parallel instances downstream
   - Fan-in (scale down): Re-aggregate results from parallel work
   - High recovery cost → worth checkpointing

2. **MEDIUM (in parallel zone)**: minscale > 1 means multiple instances of this task run in parallel.
   - If one instance fails, only that instance needs recovery, not the whole task.
   - Cost is moderate.

3. **LOW (sequential)**: minscale = 1 with no transitions.
   - Cheap to re-run.
   - Low priority for checkpointing.

---

## Implementation

### Files Modified

| File | Function |
|------|----------|
| `Checkpoint-Planner/checkpointer.py` | `classify_single_task()` |
| `verify_checkpoint_plan.py` | `classify_single_task()` |

### Code

```python
def classify_single_task(task_id, tasks, t2d, t2a, task_depths=None):
    """
    Classify a single task as CRITICAL, MEDIUM, or LOW based purely on minscale.

    Classification rules:
    - CRITICAL: any successor has minscale != this task's minscale (transition/boundary)
    - MEDIUM: this task's minscale > 1 AND no transition (in parallel zone)
    - LOW: this task's minscale = 1 AND no transition (purely sequential)
    """
    successors = t2d.get(task_id, [])
    task_minscale = tasks[task_id].get("minscale", 1)

    # === CRITICAL: Minscale transitions ===

    # Any transition to a different minscale (fan-in or fan-out)
    for succ_id in successors:
        succ_minscale = tasks[succ_id].get("minscale", 1)
        if succ_minscale != task_minscale:
            return CHECKPOINT_LEVEL_CRITICAL

    # === No transition — classify by this task's minscale ===

    # MEDIUM: in a parallel zone (minscale > 1)
    if task_minscale > 1:
        return CHECKPOINT_LEVEL_MEDIUM

    # LOW: purely sequential (minscale = 1)
    return CHECKPOINT_LEVEL_LOW
```

---

## Usage

### Run the Checkpoint Planner

```bash
python3 Checkpoint-Planner/checkpointer.py \
  --workflows-dir Algorithm-Tester/workflows \
  --workflow wf-1 \
  --baseline ours \
  --write-plan-to-db
```

### Verify Classification

```bash
python3 verify_checkpoint_plan.py \
  --tasks-json Algorithm-Tester/workflows/wf-1/tasks.json
```

### Batch all workflows

```bash
for wf in wf-1 wf-1 wf-2 wf-3 wf-4 wf-5 wf-6 wf-7 wf-8; do
  python3 Checkpoint-Planner/checkpointer.py \
    --workflows-dir Algorithm-Tester/workflows \
    --workflow $wf --baseline ours --write-plan-to-db
  python3 verify_checkpoint_plan.py \
    --tasks-json Algorithm-Tester/workflows/$wf/tasks.json
done
```

---

## Database Table

```sql
CREATE TABLE workflow_checkpoint_plan (
  workflow_name VARCHAR(256),
  baseline      VARCHAR(128),
  task_id       VARCHAR(256),
  checkpoint_level VARCHAR(32) DEFAULT 'LOW',
  notes         VARCHAR(255) DEFAULT 'main_experiment',
  PRIMARY KEY (workflow_name, baseline, task_id)
);
```

### Query examples

```sql
-- List all CRITICAL tasks
SELECT workflow_name, task_id FROM workflow_checkpoint_plan 
WHERE checkpoint_level = 'CRITICAL' 
ORDER BY workflow_name, task_id;

-- Count by level per workflow
SELECT workflow_name, checkpoint_level, COUNT(*) as count 
FROM workflow_checkpoint_plan 
GROUP BY workflow_name, checkpoint_level 
ORDER BY workflow_name, checkpoint_level;
```
