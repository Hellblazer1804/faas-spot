import subprocess
import time
import math
import random
import requests
import sys
import os
from threading import Thread
from queue import Queue
import mysql.connector
from mysql.connector import pooling
import argparse
from threading import Lock

valid_combinations = {
    'v100': {"us-east-1a", "us-east-1c", "us-east-1d", "us-east-1f", "us-east-2a", "us-east-2b", "us-west-2a", "us-west-2b", "us-west-2c"},
    'k80': {"us-west-2a", "us-west-2b"}
}

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
emulator_path = os.path.join(parent_dir, "New-Emulator", "Emulator.py")
scaler_path = os.path.join(parent_dir, "Scaler", "preemptive_scaler.py")

parser = argparse.ArgumentParser()
wf_choices = ['wf-1', 'wf-2', 'wf-3', 'wf-4', 'wf-5', 'wf-6', 'wf-7', 'wf-8', 'wf-9']
parser.add_argument("--workflow", type=str, default="wf-1", choices=wf_choices)
parser.add_argument("--max_requests", type=int, default=500)
parser.add_argument("--burst_size", type=int, default=3)
availability_zone_choices=["us-east-1a", "us-east-1c", "us-east-1d", "us-east-1f","us-east-2a", "us-east-2b", "us-west-2a", "us-west-2b", "us-west-2c"]
parser.add_argument("--az", type=str, default="us-east-1a", choices=availability_zone_choices)
parser.add_argument("--instance_type", type=str, default="v100_1", choices=valid_combinations.keys())
parser.add_argument("--no_emulator", action="store_false")
parser.add_argument("--no_scaler", action="store_false")
args = parser.parse_args()

if args.az not in valid_combinations[args.instance_type]:
    print(f"Error: instance type '{args.instance_type}' is not available in AZ '{args.az}'")
    sys.exit(1)

# Configuration
workflow_name = args.workflow
url = ff"{os.getenv('ROUTER_BASE_URL', 'http://localhost:32363')}/task1"  # Replace with actual
max_requests = args.max_requests
burst_size = args.burst_size
az = args.az
instance_type = args.instance_type
use_emulator = args.no_emulator
use_scaler = args.no_scaler
successful_completions = 0
request_count = 0
count_lock = Lock()

print("------------------------------RUN CONFIGURATION------------------------------")
print(f"Workflow: {workflow_name}\nURL: {url}\nMax Requests: {max_requests}\nBurst Size: {burst_size}\nAvailability Zone: {az}\nInstace Type: {instance_type}\nUse Emulator: {use_emulator}\nUse Scaler: {use_scaler}")
print("-----------------------------------------------------------------------------")
confirm = input("\nProceed? (y/yes to continue, h/help for options, n/no to cancel): ").strip().lower()
if not confirm:
    confirm = 'y'
if confirm not in ("y", "yes"):
    if confirm in ("h", "help"):
        parser.print_help()
    print("Aborted by user.")
    sys.exit(0)

print("------------------------------------RUNNING----------------------------------")

# Initialize a MySQL connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    user=os.getenv('DB_USER', ''),
    password=os.getenv('DB_PASSWORD', ''),
    host=os.getenv('DB_HOST', 'localhost'),
    database=os.getenv('DB_NAME', '')
)

# Function to launch emulator
def start_emulator():
    print("🚀 Starting emulator...")
    subprocess.Popen([
        "python3", emulator_path,
        "--az", az,
        "--instance", instance_type
    ])

# Function to launch preemptive scaler
def start_scaler():
    print("🚀 Starting scaler...")
    subprocess.Popen([
        "python3", scaler_path
    ])

# Adaptive Poisson-based inter-arrival generator
def generate_adaptive_intervals(rps_start=1, rps_max=30, rps_step=1):
    current_rps = rps_start
    total_requests = 0
    intervals = []
    while total_requests < max_requests:
        batch_size = min(burst_size, max_requests - total_requests)
        for _ in range(batch_size):
            interval = random.expovariate(current_rps)
            intervals.append(interval)
        current_rps = min(current_rps + rps_step, rps_max)
        total_requests += batch_size
    return intervals

# Request sender
def send_request():
    global request_count, successful_completions
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    start_time = int(time.time())
    try:
        print(f"⏰ Sending request at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}") 
        response = requests.post(url, timeout=60)
        with count_lock:
            request_count += 1
            if response.status_code == 200:
                successful_completions += 1
                print(f"✅ Request {request_count} successful!")
        if response.status_code == 504:
            cursor.execute(
                "INSERT INTO router_failures (workflow_name, timestamp, response_code) VALUES (%s, NOW(), %s)",
                (workflow_name, response.status_code)
            )
            conn.commit()
            print(f"🚨 (504) Exception)")

    except Exception as e:
        cursor.execute(
            "INSERT INTO router_failures (workflow_name, timestamp, response_code) VALUES (%s, NOW(), %s)",
            (workflow_name, 500)
        )
        conn.commit()
        print(f"🚨 (500) Exception message: {repr(e)})")

    finally:
        cursor.close()
        conn.close()

# Worker thread
def worker(q):
    while True:
        delay = q.get()
        if delay is None:
            break
        time.sleep(delay)
        send_request()
        q.task_done()

# Main execution
def main():
    if use_emulator or use_scaler:
        if use_emulator: start_emulator()
        if use_scaler: start_scaler()
        time.sleep(10)  # Let emulator and scaler warm up

    print("📈 Generating load...")
    intervals = generate_adaptive_intervals()
    q = Queue()
    threads = []

    for _ in range(burst_size):
        t = Thread(target=worker, args=(q,))
        t.start()
        threads.append(t)

    for interval in intervals:
        q.put(interval)

    q.join()

    for _ in range(burst_size):
        q.put(None)
    for t in threads:
        t.join()

    print(f"ℹ️ {successful_completions} successful executions!")
    print("✅ Load test complete.")

if __name__ == "__main__":
    main()
