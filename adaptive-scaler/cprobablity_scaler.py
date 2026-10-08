from re import match
import time
import json
import subprocess
from datetime import datetime, timezone
from kubernetes import client, config
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt



config.load_kube_config()
core_api = client.CoreV1Api()
apps_api = client.AppsV1Api()
def load_metadata():
    config.load_kube_config()
    with open("../generated-workflows/wf-1/metadata.json") as f:
        metadata = json.load(f)
   
    return metadata
metadata = load_metadata()

def calculate_cdf_survival():
    df = pd.read_csv('us-west-2a_k80.csv')
    df["Lifetime_minutes"] = df["Lifetime_seconds"] / 60
    sorted_lifetimes = np.sort(df["Lifetime_minutes"].values)
    cdf = np.arange(1, len(sorted_lifetimes) + 1) / len(sorted_lifetimes)
    survival = 1 - cdf
    return sorted_lifetimes, cdf, survival



def conditional_survival_probability(t, delta_t, x_vals, survival_vals):
    s_t = np.interp(t, x_vals, survival_vals)
    s_t_dt = np.interp(t + delta_t, x_vals, survival_vals)
    return s_t_dt / s_t if s_t > 0 else 0.0


def track_lifetime():
    x_vals, cdf_vals, survival_vals = calculate_cdf_survival()
    
    #lets get the current time from k8s namespace 'default' where pods time is uptime of the pods
    config.load_kube_config()
    core_api = client.CoreV1Api()
    pod_survival_probs = {}

    while True:
        pods = core_api.list_namespaced_pod("default")
        for pod in pods.items:
            pod_name = pod.metadata.name
            pod_start_time = pod.status.start_time
            if pod_start_time:
                uptime = (datetime.now(timezone.utc) - pod_start_time).total_seconds() / 60
                #print(f"Pod: {pod_name}, Uptime: {uptime:.2f} minutes")
                initial_delta_t = 5  # minutes
                survival_prob = conditional_survival_probability(uptime, initial_delta_t, x_vals, survival_vals)
                pod_survival_probs[pod_name] = survival_prob
                print(f"Pod: {pod_name}, Survival Probability: {survival_prob:.4f}")
        
        for pod_name, prob in pod_survival_probs.items():
            if prob < 0.60:
                print(f"Pod {pod_name} has low survival probability ({prob:.4f}), consider scaling out")
                scale_out_pod(pod_name)
                # Here you can add logic to scale down the pod or deployment if needed
        time.sleep(5)



def scale_out_pod(pod_name):
    pod = core_api.read_namespaced_pod(pod_name, namespace="default")

    # Step 1: Find owning ReplicaSet
    owners = pod.metadata.owner_references
    rs_name = None
    for owner in owners:
        if owner.kind == "ReplicaSet":
            rs_name = owner.name
            break

    if not rs_name:
        print(f"No ReplicaSet owner found for pod {pod_name}")
        return

    # Step 2: Find Deployment that owns the ReplicaSet
    rs = apps_api.read_namespaced_replica_set(name=rs_name, namespace="default")
    rs_owners = rs.metadata.owner_references
    deployment_name = None
    for owner in rs_owners:
        if owner.kind == "Deployment":
            deployment_name = owner.name
            break

    if not deployment_name:
        print(f"No Deployment owner found for ReplicaSet {rs_name}")
        return

    # Step 3: Apply scaling logic using metadata
    task_id = deployment_name  # assume deployment_name == task_id

    if task_id not in metadata:
        print(f"Deployment {deployment_name} not found in metadata")
        return

    task_meta = metadata[task_id]
    minscale = task_meta.get("minscale", 1)
    scaling_factor = task_meta.get("scaling_factor", 1.0)
    desired_scale = math.ceil(minscale * scaling_factor)

    # Step 4: Get current replicas
    deployment = apps_api.read_namespaced_deployment(name=deployment_name, namespace="default")
    current_replicas = deployment.spec.replicas or 1

    if current_replicas < desired_scale:
        apps_api.patch_namespaced_deployment(
            name=deployment_name,
            namespace="default",
            body={"spec": {"replicas": desired_scale}}
        )
        print(f"Scaled deployment {deployment_name} from {current_replicas} to {desired_scale} replicas.")
    else:
        print(f"Deployment {deployment_name} already at or above desired scale ({current_replicas} >= {desired_scale})")


if __name__ == "__main__":
    # Uncomment the line below to run the lifetime tracking
    track_lifetime()

    # Uncomment the line below to run the preemptive scaling
    # preemptive_scale(deflection_lifetime=180)

    # Uncomment the line below to plot the graph






























#def plot_graph():

    # x_vals, cdf_vals, survival_vals = calculate_cdf_survival()
    # plt.figure(figsize=(10, 5))

    # # CDF
    # plt.subplot(1, 2, 1)
    # plt.plot(x_vals, cdf_vals, drawstyle='steps-post')
    # plt.title("CDF of Spot VM Lifetimes")
    # plt.xlabel("Lifetime (minutes)")
    # plt.ylabel("CDF")
    # plt.grid(True)

    # # Survival Function
    # plt.subplot(1, 2, 2)
    # plt.plot(x_vals, survival_vals, drawstyle='steps-post', color='orange')
    # plt.title("Survival Function (1 - CDF)")
    # plt.xlabel("Lifetime (minutes)")
    # plt.ylabel("Survival Probability")
    # plt.grid(True)

    # plt.tight_layout()
    # plt.savefig("spot_vm_lifetime_analysis.png")