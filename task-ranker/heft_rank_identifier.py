#!/usr/bin/env python3
import os
import re
import json
import argparse
from collections import defaultdict, OrderedDict
from kubernetes import client, config

NAMESPACE = "default"

def dns1123_name(s: str) -> str:
    s = s.lower().replace('_', '-')
    s = re.sub(r'[^a-z0-9\.-]+', '-', s)
    s = re.sub(r'^[^a-z0-9]+', '', s)
    s = re.sub(r'[^a-z0-9]+$', '', s)
    return (s[:63] or 'x')

def configmap_name_for_task(wf_id: str, task_id: str, baseline: str = "ours") -> str:
    return dns1123_name(f"{wf_id}-{task_id}-{baseline}-cfg")

def rollout_restart_task(task_id: str, namespace: str = NAMESPACE):
    apps = client.AppsV1Api()
    deps = apps.list_namespaced_deployment(namespace, label_selector=f"functionName={task_id}").items
    if not deps:
        print(f"⚠️  No deployment found for task {task_id}")
        return
    dep_name = deps[0].metadata.name
    os.system(f"kubectl -n {namespace} rollout restart deployment/{dep_name}")
    os.system(f"kubectl -n {namespace} rollout status deployment/{dep_name} --timeout=180s")
    print(f"✅ rollout-restarted {dep_name}")

def patch_configmap_kv(cm_name: str, kv: dict, namespace: str = NAMESPACE):
    v1 = client.CoreV1Api()
    body = {"data": {k: str(v) for k, v in kv.items()}}
    v1.patch_namespaced_config_map(name=cm_name, namespace=namespace, body=body)
    print(f"🧩 Patched ConfigMap {cm_name} with {kv}")

def calculate_heft_ranks(tasks):
    ranks = {}
    def rank(tid):
        if tid in ranks: return ranks[tid]
        max_succ = max([rank(s) for s in tasks[tid].get("successors", [])], default=0)
        ranks[tid] = tasks[tid].get("exec_time", 0.25) + max_succ
        return ranks[tid]
    for tid in tasks: rank(tid)
    return ranks

def assign_scaling_with_hybrid_strategy(tasks, ranks, spot_budget, unit_cost=1, checkpoint_cost=5, max_scaling_cap=2.5, pods_per_machine=3):
    machines, task_pod_map = {}, defaultdict(lambda: defaultdict(int))
    machine_id, machine, current_pods = 1, [], 0
    # pack pods into "machines"
    for tid in sorted(tasks.keys()):
        pod_count = int(tasks[tid].get("minscale", 1))
        while pod_count > 0:
            room = pods_per_machine - current_pods
            to_allocate = min(pod_count, room)
            machine.append((tid, to_allocate))
            task_pod_map[tid][f"machine_{machine_id}"] += to_allocate
            current_pods += to_allocate
            pod_count -= to_allocate
            if current_pods == pods_per_machine:
                machines[f"machine_{machine_id}"] = machine
                machine_id, machine, current_pods = machine_id + 1, [], 0
    if machine:
        machines[f"machine_{machine_id}"] = machine

    machine_costs, machine_ranks = {}, {}
    total_provisioning, total_checkpointing = 0, 0
    for mid, allocs in machines.items():
        base_cost = sum(tasks[tid]["exec_time"] * cnt * unit_cost for tid, cnt in allocs)
        task_ids = {tid for tid, _ in allocs}
        ckpt_cost = checkpoint_cost if any(tasks[t].get("is_checkpoint", False) for t in task_ids) else 0
        rank = max(ranks[tid] for tid in task_ids)
        machine_costs[mid] = {"total_cost": base_cost + ckpt_cost}
        machine_ranks[mid] = rank
        total_provisioning += base_cost
        total_checkpointing += ckpt_cost

    total_cost = total_provisioning + total_checkpointing
    max_scaling = min(spot_budget / total_cost, max_scaling_cap) if total_cost > 0 else 1.0

    sorted_machines = sorted(machine_ranks.items(), key=lambda x: x[1], reverse=True)
    selected_machines, used_budget = OrderedDict(), 0
    for mid, _ in sorted_machines:
        proj = machine_costs[mid]["total_cost"] * max_scaling
        if used_budget + proj <= spot_budget:
            selected_machines[mid], used_budget = machine_ranks[mid], used_budget + proj

    total_selected_rank = sum(selected_machines.values())
    machine_scaling_factors = {}
    for mid in machines:
        if mid in selected_machines and total_selected_rank > 0:
            machine_scaling_factors[mid] = round(1 + (max_scaling - 1) * (selected_machines[mid] / total_selected_rank), 2)
        else:
            machine_scaling_factors[mid] = 1.0

    for tid in tasks:
        pod_allocs = task_pod_map[tid]
        total_pods = sum(pod_allocs.values())
        agg_scale = sum(machine_scaling_factors[mid] * cnt for mid, cnt in pod_allocs.items())
        tasks[tid]["scaling_factor"] = round(agg_scale / total_pods, 2) if total_pods > 0 else 1.0
        tasks[tid]["is_scalable"] = tasks[tid]["scaling_factor"] > 1.0
        tasks[tid]["machine_ids"] = dict(pod_allocs)
        tasks[tid]["heft_rank"] = ranks[tid]
    return tasks

def main():
    parser = argparse.ArgumentParser(description="HEFT Ranker -> patch per-task ConfigMaps")
    parser.add_argument("--workflow", required=True, help="Workflow ID (e.g., wf-3)")
    parser.add_argument("--budget", type=int, default=300, help="Total spot budget for scaling calculations")
    parser.add_argument("--workflows-dir", required=True, help="Directory containing workflow definitions")
    parser.add_argument("--baseline", default="ours")
    args = parser.parse_args()

    config.load_kube_config()
    
    tasks_path = os.path.join(args.workflows_dir, args.workflow, "tasks.json")
    if not os.path.exists(tasks_path):
        print(f"❌ tasks.json not found at {tasks_path}")
        return
    with open(tasks_path) as f:
        tasks = json.load(f)["tasks"]

    ranks = calculate_heft_ranks(tasks)
    updated = assign_scaling_with_hybrid_strategy(tasks, ranks, args.budget)

    print(f"\nApplying scaling factors via ConfigMaps for workflow {args.workflow}...")
    for tid, info in updated.items():
        cm = configmap_name_for_task(args.workflow, tid, args.baseline)
        kv = {
            "IS_SCALABLE": str(info["is_scalable"]).lower(),
            "SCALING_FACTOR": str(info["scaling_factor"])
        }
        patch_configmap_kv(cm, kv)
        rollout_restart_task(tid)  # pick up new values

    # persist metadata for the scaler
    out_dir = os.path.join(args.workflows_dir, args.workflow)
    with open(os.path.join(out_dir, "metadata.json"), "w") as f:
        json.dump(updated, f, indent=2)
    print(f"✅ metadata.json written to {out_dir}/metadata.json")

if __name__ == "__main__":
    main()
