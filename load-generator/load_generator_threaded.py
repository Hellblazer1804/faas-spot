import argparse
import time
import threading
import requests
import csv
from queue import Queue

def worker(queue, router_error_writer, url):
    while True:
        req_id = queue.get()
        if req_id is None:
            break
        try:
            t0 = time.time()
            resp = requests.post(url, timeout=60)
            elapsed = time.time() - t0
            if resp.status_code == 504:
                router_error_writer.writerow([req_id, time.time(), resp.status_code, elapsed])
            # Optionally log all responses elsewhere if needed
        except Exception as e:
            # Router/network errors can be treated as 504 for logging purposes
            router_error_writer.writerow([req_id, time.time(), 504, 0])
        queue.task_done()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True, help='Workflow endpoint URL')
    parser.add_argument('--concurrency', type=int, default=2, help='Concurrent requests')
    parser.add_argument('--total', type=int, default=250, help='Total requests')
    parser.add_argument('--router_log', default='router_504_errors.csv')
    args = parser.parse_args()

    queue = Queue()
    threads = []
    router_log_file = open(args.router_log, 'w', newline='')
    router_error_writer = csv.writer(router_log_file)
    router_error_writer.writerow(["req_id", "timestamp", "status_code", "elapsed"])

    # Start worker threads
    for _ in range(args.concurrency):
        t = threading.Thread(target=worker, args=(queue, router_error_writer, args.url))
        t.daemon = True
        t.start()
        threads.append(t)

    interval = 1.0 / args.concurrency  # space out requests roughly evenly per second

    for req_id in range(args.total):
        print(f"Sending request {req_id}")
        queue.put(req_id)
        time.sleep(interval)

    # Wait for all tasks to finish
    queue.join()
    for _ in range(args.concurrency):
        queue.put(None)
    for t in threads:
        t.join()
    router_log_file.close()

    print(f"✅ Load generation complete. Router-level errors logged to {args.router_log}")

if __name__ == '__main__':
    main()
