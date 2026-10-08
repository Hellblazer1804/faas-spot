import os
import math
import random
import time
import requests
import mysql.connector
from mysql.connector import pooling, errorcode
from threading import Thread
from queue import Queue
from typing import List

# Initialize a connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    user=os.getenv('DB_USER', ''),
    password=os.getenv('DB_PASSWORD', ''),
    host=os.getenv('DB_HOST', 'localhost'),
    database=os.getenv('DB_NAME', '')
)

def generate_inter_arrival_times(arrival_rate: int, decay_rate: float, max_requests: int) -> List[float]:
    inter_arrival_times = []
    time_elapsed = 0
    min_rate = 1e-5

    for _ in range(max_requests):
        current_rate = arrival_rate * math.exp(-decay_rate * time_elapsed)
        # Ensure the current_rate does not drop below a minimum threshold
        current_rate = max(current_rate, min_rate)
        inter_arrival_time = random.expovariate(current_rate)
        inter_arrival_times.append(inter_arrival_time)
        time_elapsed += inter_arrival_time

    return inter_arrival_times

def send_request(url):
    connection = connection_pool.get_connection()  # Use a pooled connection
    cursor = connection.cursor()
    start_time = round(time.time(), 3)  # Capture time with 3 decimal places
    response_code = None
    try:
        response = requests.post(url, timeout=240)
        response_code = response.status_code
        end_time = round(time.time(), 3)

        if response_code != 200:
            # Log the failed request in the database
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code) VALUES (%s, %s, %s, %s, %s, %s)"
            values = ("serverless_branch_merge_wf", "router", start_time, end_time, '', response_code)
            cursor.execute(query, values)
            connection.commit()
        
        return response_code, response.elapsed.total_seconds()
    
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        end_time = round(time.time(), 3)
        query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code) VALUES (%s, %s, %s, %s, %s, %s)"
        values = ("serverless_branch_merge_wf", "router", start_time, end_time, '', 500)
        cursor.execute(query, values)
        connection.commit()
        
        return "Error", "Request failed"
    
    finally:
        cursor.close()
        connection.close()

def worker(queue, url):
    while True:
        scheduled_time = queue.get()
        if scheduled_time is None:
            break
        time_to_wait = scheduled_time - time.time()
        if time_to_wait > 0:
            time.sleep(time_to_wait)
        response = send_request(url)
        print(f"Request sent at {time.time()}, Response: {response}")
        queue.task_done()

def main():
    url = f"{os.getenv('ROUTER_BASE_URL', 'http://localhost:32363')}/bmwf1"
    arrival_rate = 10  # Average arrival rate of requests
    decay_rate = 0.005  # Decay rate
    max_requests = 2000  # Total maximum requests to be sent
    inter_arrival_times = generate_inter_arrival_times(arrival_rate, decay_rate, max_requests)

    burst_size = 4  # Concurrent requests per burst

    queue = Queue()
    threads = []

    # Start worker threads
    for _ in range(burst_size):
        t = Thread(target=worker, args=(queue, url))
        t.start()
        threads.append(t)

    current_time = time.time()
    for i in range(max_requests):
        scheduled_time = current_time + inter_arrival_times[i]
        queue.put(scheduled_time)
        current_time = scheduled_time

    queue.join()  # Wait for all tasks to complete

    # Signal workers to exit
    for _ in range(burst_size):
        queue.put(None)
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()