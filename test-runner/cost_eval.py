#!/usr/bin/env python3
"""
cost_eval.py — cost and budget evaluation utilities for FaaSpot experiments.

Public API used by test_runner:
    get_total_cost_from_db(workflow, baseline) -> dict
    get_budget_for_workflow(workflow, az, instance) -> float | None
    extract_cost_from_scaler_log(log_path) -> dict
    get_budget_override_percent(workflow) -> float | None
"""

import json
import os
import re
from typing import Dict, Optional

import mysql.connector

# ─── Config (reads from environment / .env) ──────────────────────────────────

_DB_CONFIG = {
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', ''),
}

_EXPERIMENT_TAG  = os.getenv('EXPERIMENT_TAG', 'main_experiment')
_WORKFLOWS_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'workflows')

_WF_BUDGET_PERCENT   = float(os.getenv('WF_BUDGET_PERCENT', '0.50'))
_WF_RUNTIME_HOURS    = float(os.getenv('WF_ESTIMATED_RUNTIME_HOURS', '1.5'))
_WF_MACHINES         = int(os.getenv('WF_ESTIMATED_MACHINES', '8'))
_CHURN_OVERHEAD      = 1.0

_BUDGET_OVERRIDE_PERCENT   = None
_BUDGET_OVERRIDE_WORKFLOWS = set()

def _parse_env_percent(raw: str) -> Optional[float]:
    if not raw:
        return None
    try:
        v = float(raw)
        return (v / 100.0) if v > 1.0 else v
    except (TypeError, ValueError):
        return None

def _parse_env_wf_set(raw: str) -> set:
    if not raw:
        return set()
    return {w.strip() for w in raw.split(',') if w.strip()}

_BUDGET_OVERRIDE_PERCENT   = _parse_env_percent(os.getenv('CHECKSCALE_BUDGET_OVERRIDE_PERCENT', ''))
_BUDGET_OVERRIDE_WORKFLOWS = _parse_env_wf_set(os.getenv('CHECKSCALE_BUDGET_OVERRIDE_WORKFLOWS', ''))

# ─── Internal helpers ─────────────────────────────────────────────────────────

def _db_connect():
    return mysql.connector.connect(**_DB_CONFIG)


def _normalize_baseline(value: Optional[str]) -> str:
    if not value:
        return ''
    v = str(value).strip().lower()
    aliases = {'ours': 'ours', 'our': 'ours', 'faas_spot': 'ours', 'faasspot': 'ours'}
    return aliases.get(v, v)


def _task_from_podname(pod_name: str) -> str:
    if not pod_name:
        return ''
    return pod_name.split('-')[0]


def _compute_cost_per_wf_baseline(cost_df, evals_df, task_to_wf):
    """
    Compute total cost per (workflow, baseline) pair using machine packing
    (3 pods per machine) and the experiment time window from evals_df.
    """
    import numpy as np
    import pandas as pd

    if cost_df is None or cost_df.empty:
        return pd.DataFrame(columns=['workflow_name', 'baseline', 'total_cost'])

    c = cost_df.copy()
    c.columns = [x.strip() for x in c.columns]
    c['workflow_name'] = c.get('workflow_name', '').astype(str).str.strip().str.lower()
    c['baseline']      = c.get('baseline', '').astype(str).map(_normalize_baseline)
    c['ts']            = pd.to_datetime(c.get('timestamp'), errors='coerce') if 'timestamp' in c.columns else pd.NaT

    for col in ('pod_age_seconds', 'price_per_hour'):
        if col in c.columns:
            c[col] = pd.to_numeric(c[col], errors='coerce')

    c['task'] = c['pod_name'].apply(_task_from_podname)
    mask = (c['workflow_name'] == '') | (c['workflow_name'].isna())
    if mask.any():
        c.loc[mask, 'workflow_name'] = c.loc[mask, 'task'].map(task_to_wf).fillna('unknown')
    c = c[c['workflow_name'] != 'unknown'].copy()

    # Assign machine IDs: 3 pods share 1 machine
    c['machine_id'] = 1
    for (wf, task), group_idx in c.groupby(['workflow_name', 'task']).groups.items():
        sorted_pods = sorted(c.loc[group_idx, 'pod_name'].unique())
        pod_index   = {p: i for i, p in enumerate(sorted_pods)}
        for idx in group_idx:
            c.loc[idx, 'machine_id'] = (pod_index.get(c.loc[idx, 'pod_name'], 0) // 3) + 1
    c['machine_id'] = c['machine_id'].astype(int)

    windows = evals_df.groupby(['workflow_name', 'baseline'], as_index=False).agg(
        t0=('start_time', 'min'), t1=('end_time', 'max')
    )
    windows['t0_dt'] = pd.to_datetime(windows['t0'], unit='s', errors='coerce')
    windows['t1_dt'] = pd.to_datetime(windows['t1'], unit='s', errors='coerce')

    totals = []
    for wf, base, t0, t1, t0d, t1d in windows[['workflow_name', 'baseline', 't0', 't1', 't0_dt', 't1_dt']].itertuples(index=False):
        d = c[(c['workflow_name'] == wf) & (c['baseline'] == base)]
        if d.empty:
            totals.append((wf, base, 0.0))
            continue

        di = d[(d['ts'] >= t0d) & (d['ts'] <= t1d)].copy() if (
            d['ts'].notna().any() and pd.notna(t0d) and pd.notna(t1d)
        ) else d.copy()
        if di.empty:
            di = d.copy()

        di = di.sort_values(['pod_name', 'ts'], kind='mergesort')
        di['prev_age']   = di.groupby('pod_name')['pod_age_seconds'].shift(1)
        di['delta_age_sec'] = (di['pod_age_seconds'] - di['prev_age']).clip(lower=0)
        if 'check_interval_seconds' in di.columns:
            sel = di['delta_age_sec'].isna() | (di['delta_age_sec'] == 0)
            di.loc[sel, 'delta_age_sec'] = di.loc[sel, 'check_interval_seconds'].fillna(0)
        else:
            di['delta_age_sec'] = di['delta_age_sec'].fillna(0)

        di['price_per_hour'] = pd.to_numeric(di['price_per_hour'], errors='coerce').fillna(0)

        machine_stats = di.groupby(['task', 'machine_id'], observed=False).agg(
            max_age=('pod_age_seconds', 'max'),
        ).reset_index()
        price_map = (
            di.sort_values(['task', 'machine_id', 'pod_age_seconds'], ascending=[True, True, False])
              .groupby(['task', 'machine_id'], observed=False)
              .first()
              .reset_index()[['task', 'machine_id', 'price_per_hour']]
        )
        machine_stats = machine_stats.merge(price_map, on=['task', 'machine_id'], how='left')
        machine_stats['price_per_hour'] = pd.to_numeric(machine_stats['price_per_hour'], errors='coerce').fillna(0)
        machine_stats['cost'] = machine_stats['price_per_hour'] * machine_stats['max_age'].fillna(0) / 3600.0
        totals.append((wf, base, float(machine_stats['cost'].sum())))

    res = pd.DataFrame(totals, columns=['workflow_name', 'baseline', 'total_cost'])
    res = res[res['workflow_name'].isin(evals_df['workflow_name'].unique())]
    return res

# ─── Public API ───────────────────────────────────────────────────────────────

def get_total_cost_from_db(workflow_name: str, baseline: str) -> Dict[str, Optional[float]]:
    """
    Return cost metrics for a completed experiment run from the database.

    Returns a dict with keys:
        total_cost, avg_price_per_hour, sample_count,
        total_runtime_seconds, normalized_cost (always None here).
    """
    import pandas as pd

    result: Dict[str, Optional[float]] = {
        'total_cost': None,
        'avg_price_per_hour': None,
        'sample_count': 0,
        'total_runtime_seconds': None,
        'normalized_cost': None,
    }

    try:
        conn   = _db_connect()
        cursor = conn.cursor(dictionary=True)

        tag_clause = ' AND notes = %s' if _EXPERIMENT_TAG else ''

        # Experiment time windows
        eval_params = [workflow_name, baseline] + ([_EXPERIMENT_TAG] if _EXPERIMENT_TAG else [])
        cursor.execute(
            f'SELECT workflow_name, baseline, start_time, end_time '
            f'FROM serverless_workflows WHERE workflow_name = %s AND baseline = %s{tag_clause}',
            tuple(eval_params)
        )
        evals_df = pd.DataFrame(cursor.fetchall())
        if evals_df.empty:
            evals_df = pd.DataFrame([{
                'workflow_name': str(workflow_name).strip().lower(),
                'baseline':      _normalize_baseline(baseline),
                'start_time':    None, 'end_time': None,
            }])
            print('⚠️  No serverless_workflows rows found; using unbounded cost window.')
        else:
            evals_df['workflow_name'] = evals_df['workflow_name'].astype(str).str.strip().str.lower()
            evals_df['baseline']      = evals_df['baseline'].astype(str).map(_normalize_baseline)

        # Cost logs
        cost_params = [workflow_name, baseline] + ([_EXPERIMENT_TAG] if _EXPERIMENT_TAG else [])
        cursor.execute(
            f'SELECT workflow_name, baseline, pod_name, pod_age_seconds, '
            f'price_per_hour, check_interval_seconds, timestamp '
            f'FROM cost_logs WHERE workflow_name = %s AND baseline = %s{tag_clause} '
            f'ORDER BY pod_name, timestamp',
            tuple(cost_params)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        print(f'📊 Found {len(rows)} cost_logs records for {workflow_name}/{baseline}')
        if not rows:
            print(f'⚠️  No cost data found in DB for {workflow_name}/{baseline}')
            return result

        cost_df          = pd.DataFrame(rows)
        wf_norm          = str(workflow_name).strip().lower()
        baseline_norm    = _normalize_baseline(baseline)

        task_to_wf = {}
        tasks_path = os.path.join(_WORKFLOWS_DIR, workflow_name, 'tasks.json')
        if os.path.exists(tasks_path):
            try:
                with open(tasks_path) as f:
                    task_to_wf = {t: wf_norm for t in json.load(f).get('tasks', {})}
            except Exception:
                pass
        if not task_to_wf:
            task_to_wf = {_task_from_podname(p): wf_norm
                          for p in cost_df['pod_name'].dropna().unique()}

        summary = _compute_cost_per_wf_baseline(cost_df, evals_df, task_to_wf)
        if summary.empty:
            print('⚠️  Cost computation returned no rows.')
            return result

        match = summary[(summary['workflow_name'] == wf_norm) & (summary['baseline'] == baseline_norm)]
        if match.empty:
            print(f'⚠️  No cost row matched for {workflow_name}/{baseline}.')
            return result

        total_cost = float(match['total_cost'].iloc[0])
        avg_price  = float(pd.to_numeric(cost_df.get('price_per_hour'), errors='coerce').mean()) \
                     if 'price_per_hour' in cost_df else 0.0

        runtime_sec = None
        try:
            t0 = pd.to_numeric(evals_df['start_time'], errors='coerce').min()
            t1 = pd.to_numeric(evals_df['end_time'],   errors='coerce').max()
            if pd.notna(t0) and pd.notna(t1):
                runtime_sec = float(max(t1 - t0, 0))
        except Exception:
            pass

        result.update({
            'total_cost':           total_cost,
            'avg_price_per_hour':   avg_price,
            'sample_count':         len(cost_df),
            'total_runtime_seconds': runtime_sec,
        })
        win = '(filtered to experiment window)' if evals_df['start_time'].notna().any() else '(no time filter)'
        print(f'💾 Cost: ${total_cost:.2f} total, ${avg_price:.4f}/machine/hr, {len(cost_df)} samples {win}')

    except Exception as e:
        import traceback
        print(f'⚠️  Error reading cost from DB: {e}')
        traceback.print_exc()

    return result


def get_budget_for_workflow(workflow_name: str,
                            az: str = 'us-west-2a',
                            instance: str = 'v100') -> Optional[float]:
    """
    Return the experiment budget (dollars) for a workflow.
    Budget = WF_BUDGET_PERCENT × estimated on-demand cost.
    On-demand price is loaded from the spot cost CSV if available,
    otherwise falls back to $3.06/machine/hr.
    """
    ondemand_price = 3.06
    try:
        import pandas as pd
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cost_csv  = os.path.join(repo_root, 'adaptive-scaler', 'spot-cost-csv',
                                 f'{az}_{instance}_cost.csv')
        if os.path.exists(cost_csv):
            df = pd.read_csv(cost_csv)
            avg_spot    = df['SpotPrice ($)'].mean()
            avg_savings = df['Savings (%)'].mean() / 100.0
            ondemand_price = avg_spot / (1 - avg_savings) if avg_savings < 1.0 else avg_spot * 3
    except Exception as e:
        print(f'⚠️  Could not load spot prices ({e}), using fallback ${ondemand_price:.2f}/hr')

    budget_pct     = get_budget_override_percent(workflow_name) or _WF_BUDGET_PERCENT
    ondemand_cost  = ondemand_price * _WF_RUNTIME_HOURS * _WF_MACHINES
    return ondemand_cost * budget_pct * _CHURN_OVERHEAD


def get_budget_override_percent(workflow_name: str) -> Optional[float]:
    """Return env-specified budget override percent, or None if not set."""
    if _BUDGET_OVERRIDE_PERCENT is None:
        return None
    if _BUDGET_OVERRIDE_WORKFLOWS and workflow_name not in _BUDGET_OVERRIDE_WORKFLOWS:
        return None
    return _BUDGET_OVERRIDE_PERCENT


def extract_cost_from_scaler_log(log_path: str) -> Dict[str, Optional[float]]:
    """
    Fallback: parse cost metrics from a scaler log file when the DB is unavailable.
    """
    result: Dict[str, Optional[float]] = {
        'total_cost': None, 'budget': None,
        'budget_remaining': None, 'normalized_cost': None,
        'cost_per_pod_hour': None,
    }
    if not os.path.exists(log_path):
        return result
    try:
        content = open(log_path).read()
        m = re.findall(r'💵 Budget: \$([0-9.]+)/\$([0-9.]+) \(\$([0-9.]+) remaining\)', content)
        if m:
            result['total_cost']        = float(m[-1][0])
            result['budget']            = float(m[-1][1])
            result['budget_remaining']  = float(m[-1][2])
        n = re.findall(r'norm_cost=([0-9.]+)', content)
        if n:
            result['normalized_cost'] = float(n[-1])
        e = re.findall(r'Budget: \$([0-9.]+) \(([0-9]+)% of on-demand\)', content)
        if e and result['budget'] is None:
            result['budget'] = float(e[0][0])
    except Exception as ex:
        print(f'⚠️  Error parsing scaler log: {ex}')
    return result
