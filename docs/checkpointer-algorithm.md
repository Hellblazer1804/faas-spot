# Simplified Checkpointer Algorithm

**Status**: Planned Rewrite (Structure-Based Classification, No CDF, No DP, No Exec-Time Thresholds)

---

## Overview

The Checkpointer classifies each task in a workflow DAG into one of three criticality levels: **CRITICAL**, **MEDIUM**, or **LOW**. Classification is based purely on **minscale values and successor/predecessor structure** from tasks.json, without relying on execution time, CDFs, or complex optimization.

The levels encode **checkpoint strategy**:
- **CRITICAL**: Must protect (parallelism boundaries)
- **MEDIUM**: Should protect (parallel work or joins)
- **LOW**: Can skip (simple sequential work)

---

## Criticality Levels

### CRITICAL: "Parallelism Boundaries"

Tasks that **bridge between single-instance and parallel work**.

**Definition**: 
- Successor has minscale > 1, OR
- Predecessor has minscale > 1

**Examples**:
1. **Feeding into parallel**: a(minscale=1) → b(minscale=3)
   - a is the boundary where work diverges into parallel replicas
   - Recovery must coordinate distribution across replicas

2. **Receiving from parallel**: c(minscale=3) → e(minscale=1)
   - e is the boundary where parallel results converge
   - Recovery must coordinate collection from replicas

3. **Between replicas and single**: b(minscale=1) → c(minscale=2)
   - b is critical because its successor becomes parallel
   - If b fails, recovery affects how c's 2 replicas are fed

**Why checkpoint**: 
- Marks synchronization boundaries between serial and parallel execution
- Essential for correct recovery coordination

**Checkpoint strategy**: Must checkpoint these tasks

---

### MEDIUM: "Parallel Work or Joins"

Tasks with **replicas** or tasks that **feed into joins**.

**Definition**:
- minscale > 1 (this task runs with multiple replicas), OR
- Any successor has multiple predecessors (this task feeds into a join point)

**Examples**:
1. **Parallel replicas**: b(minscale=3) in any context
   - Runs as b[0], b[1], b[2] in parallel
   - Each replica does independent work
   - Loss of work is expensive; worth checkpointing

2. **Join feeder**: task feeds into a task with multiple predecessors
   - Example: b1 → join_task (where join_task has predecessors [b1, b2, b3])
   - If join_task fails, restarting from b1 avoids re-running all branches

**Why checkpoint**:
- Parallel replicas: expensive to re-run all instances
- Join feeders: prevent cascading re-execution from join point

**Checkpoint strategy**: Should checkpoint these tasks

---

### LOW: "Simple Sequential Work"

Single-instance tasks in **linear pipelines** with no parallelism role.

**Definition**:
- minscale = 1, AND
- Not a CRITICAL task (neither successor nor predecessor has minscale > 1), AND
- Not a MEDIUM task (no successor with multiple predecessors)

**Examples**:
1. **Source in linear pipeline**: a(minscale=1) → b(minscale=1)
   - Single instance, feeds into single instance
   - No parallelism role

2. **Intermediate in linear pipeline**: b(minscale=1) → c(minscale=1)
   - Simple handoff between tasks
   - No branching, no merging

**Why skip checkpoint**:
- Cheap to re-run (single instance, linear flow)
- No coordination complexity

**Checkpoint strategy**: Can skip checkpoint to reduce overhead

---

## Classification Algorithm

```python
def classify_tasks(tasks_json):
    """
    Classify each task as CRITICAL, MEDIUM, or LOW
    based purely on minscale and DAG structure.
    
    Input: tasks.json
    Returns: {task_id: level}
    """
    
    # Parse DAG
    tasks = load_tasks_json(tasks_json)
    t2d, t2a, _, _, _ = build_dep_maps(tasks)
    
    levels = {}
    
    for task_id in tasks:
        level = classify_single_task(task_id, tasks, t2d, t2a)
        levels[task_id] = level
    
    return levels


def classify_single_task(task_id, tasks, t2d, t2a):
    """
    Classify a single task.
    
    Pure minscale + structure, no exec_time, no thresholds.
    """
    
    minscale = tasks[task_id].get("minscale", 1)
    successors = t2d.get(task_id, [])
    predecessors = t2a.get(task_id, [])
    
    # === CRITICAL ===
    # Parallelism boundaries: successor or predecessor has minscale > 1
    
    # Check if any successor has minscale > 1
    for succ_id in successors:
        succ_minscale = tasks[succ_id].get("minscale", 1)
        if succ_minscale > 1:
            return "CRITICAL"
    
    # Check if any predecessor has minscale > 1
    for pred_id in predecessors:
        pred_minscale = tasks[pred_id].get("minscale", 1)
        if pred_minscale > 1:
            return "CRITICAL"
    
    # === MEDIUM ===
    # Parallel work or join feeders
    
    # This task has replicas
    if minscale > 1:
        return "MEDIUM"
    
    # This task feeds into a join (successor has multiple predecessors)
    for succ_id in successors:
        succ_preds = len(t2a.get(succ_id, []))
        if succ_preds > 1:
            return "MEDIUM"
    
    # === LOW ===
    # Everything else: simple sequential work
    return "LOW"
```

---

## Examples

### Example 1: Simple Linear Pipeline

```
a(minscale=1) → b(minscale=1) → c(minscale=1)
```

| Task | minscale | Succ minscale | Pred minscale | Succ preds | Level |
|---|---|---|---|---|---|
| a | 1 | 1 | - | 1 | **LOW** |
| b | 1 | 1 | 1 | 1 | **LOW** |
| c | 1 | - | 1 | - | **LOW** |

**Checkpoint strategy**: Skip all (linear sequential work)

---

### Example 2: Linear with Parallel Stage

```
a(minscale=1) → b(minscale=3) → c(minscale=3) → e(minscale=1)
```

| Task | minscale | Succ minscale | Pred minscale | Level |
|---|---|---|---|---|
| a | 1 | 3 | - | **CRITICAL** (succ has minscale > 1) |
| b | 3 | 3 | 1 | **MEDIUM** (minscale > 1) |
| c | 3 | 1 | 3 | **MEDIUM** (minscale > 1) |
| e | 1 | - | 3 | **CRITICAL** (pred has minscale > 1) |

**Checkpoint strategy**: Checkpoint a, e (boundaries), b, c (parallel work)

---

### Example 3: Transition to Parallel

```
a(minscale=1) → b(minscale=1) → c(minscale=2)
```

| Task | minscale | Succ minscale | Pred minscale | Succ preds | Level |
|---|---|---|---|---|---|
| a | 1 | 1 | - | 1 | **LOW** |
| b | 1 | 2 | 1 | 1 | **CRITICAL** (succ has minscale > 1) |
| c | 2 | - | 1 | 0 | **MEDIUM** (minscale > 1) |

**Checkpoint strategy**: Checkpoint b (boundary), c (parallel work); skip a

---

### Example 4: Fan-Out / Fan-In (Explicit Tasks)

```
a(1) → b1(1) → c(1)
     → b2(1) ↗
     → b3(1) ↗
```

| Task | Succ | Pred | Level |
|---|---|---|---|
| a | b1,b2,b3 | - | **CRITICAL** (multiple successors + minscale=1) |
| b1 | c | a | **MEDIUM** (c has 3 predecessors: b1,b2,b3) |
| b2 | c | a | **MEDIUM** (feeds into join) |
| b3 | c | a | **MEDIUM** (feeds into join) |
| c | - | b1,b2,b3 | **CRITICAL** (multiple predecessors + minscale=1) |

---

## Database Schema

```sql
CREATE TABLE workflow_checkpoint_plan (
  workflow_name VARCHAR(256),
  baseline VARCHAR(128),
  task_id VARCHAR(256),
  checkpoint_level VARCHAR(32),  -- "CRITICAL", "MEDIUM", "LOW"
  notes VARCHAR(255),
  PRIMARY KEY (workflow_name, baseline, task_id)
);
```

---

## Implementation

### Functions to Keep
- `build_dep_maps()` — extract successors/predecessors
- `load_tasks_json()` — parse tasks.json

### Functions to Remove
- `optimize_linear_path()` — DP no longer needed
- `expected_block_time()` — no cost calculations
- `compute_critical_path_tasks()` — not used
- `compute_task_depth()` — not used
- `load_cdf()` — no CDF files
- `survival_prob()` — no survival curves
- All progressive checkpointing logic

### New Functions
- `classify_tasks()` — main entry point (~5 lines)
- `classify_single_task()` — single task classification (~25 lines)

**Total new code**: ~30 lines

---

## Simplicity Summary

| Aspect | Before | After |
|---|---|---|
| Dependencies | CDF files, critical path, depth | None |
| Algorithms | DP, cost functions | if-then rules |
| Code lines | ~250 | ~30 |
| Input | tasks.json + CDF CSV | tasks.json only |
| Output | {task_id: level} | {task_id: level} |
| Readability | Complex cost functions | 3 simple checks |

---

## Rules Summary (Quick Reference)

```
CRITICAL if:
  - Any successor has minscale > 1, OR
  - Any predecessor has minscale > 1

MEDIUM if (not CRITICAL):
  - minscale > 1, OR
  - Any successor has multiple predecessors

LOW:
  - Everything else
```

---

## Checkpoint Behavior (Defined Elsewhere)

This algorithm **only classifies tasks**. Checkpoint placement decisions are made elsewhere:

- **CRITICAL tasks**: Always checkpoint (non-negotiable)
- **MEDIUM tasks**: Checkpoint when budget allows
- **LOW tasks**: Skip checkpoint to save overhead

The Retry Service and Scaler use these levels to decide retry behavior and scaling decisions.

---

## Future Enhancements

- Budget-aware checkpoint selection: keep all CRITICAL + select MEDIUM by budget
- Per-workflow tuning based on runtime statistics
- Integration with Scaler for adaptive decisions

