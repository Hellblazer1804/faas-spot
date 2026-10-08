import csv
import os

def evaluate_multi_tenant_cost(cost_log_path, pods_per_machine=3):
    machines = []
    current_machine = []
    total_machine_cost = 0.0

    with open(cost_log_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # Unpack the columns
            iteration, pod_name, lifetime, price_per_hr, cost, pod_type = row
            cost = float(cost)
            current_machine.append({
                "pod_name": pod_name,
                "cost": cost,
                "pod_type": pod_type.strip()
            })
            # Once we have pods_per_machine pods, consider as a machine
            if len(current_machine) == pods_per_machine:
                machine_cost = sum(pod['cost'] for pod in current_machine)
                machines.append({
                    "pods": [pod['pod_name'] for pod in current_machine],
                    "total_cost": machine_cost,
                    "pod_types": [pod['pod_type'] for pod in current_machine]
                })
                total_machine_cost += machine_cost
                current_machine = []

        # Handle leftover pods as a machine (if not a multiple of pods_per_machine)
        if current_machine:
            machine_cost = sum(pod['cost'] for pod in current_machine)
            machines.append({
                "pods": [pod['pod_name'] for pod in current_machine],
                "total_cost": machine_cost,
                "pod_types": [pod['pod_type'] for pod in current_machine]
            })
            total_machine_cost += machine_cost

    print("=== Multi-Tenant Machine Cost Evaluation ===")
    for idx, machine in enumerate(machines):
        print(f"Machine {idx+1}: Pods = {machine['pods']}, Cost = ${machine['total_cost']:.4f}, Pod Types = {machine['pod_types']}")
    print(f"\nTotal Machine Cost: ${total_machine_cost:.4f}")
    print(f"Total Machines: {len(machines)}")

    return {
        "machine_costs": [m["total_cost"] for m in machines],
        "total_machine_cost": total_machine_cost,
        "num_machines": len(machines)
    }

if __name__ == "__main__":
    COST_LOG_PATH = f"{os.path.dirname(os.path.abspath(__file__))}/cost-logs/static_wf1.csv"
    results = evaluate_multi_tenant_cost(COST_LOG_PATH, pods_per_machine=3)
