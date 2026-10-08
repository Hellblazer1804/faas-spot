import os
#!/usr/bin/python3

import concurrent.futures
import csv
import requests

def send_requests(count_scaled, max_threads):
    url_record=f"{os.getenv('REQUEST_RECORDER_URL', 'http://localhost:5001')}/requestRecorder"
    count_scaled = int(float(count_scaled))
    payload={
        "request_count": count_scaled
    }

    try:
        response= requests.post(url_record,json=payload)
        print("Sent the data to the database")
    except Exception as e:
        print("Failed to send the data to the database",e)
    # Calculate the number of batches and the remaining requests
    num_batches, remaining_requests = divmod(count_scaled, max_threads)

    # Create a ThreadPoolExecutor with a dynamic thread pool size for this row
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        for _ in range(num_batches):
            # Submit tasks to the thread pool
            executor.map(lambda _: send_request(), range(max_threads))

        # Handle remaining requests
        executor.map(lambda _: send_request(), range(remaining_requests))

def send_request():
    url = f"{os.getenv('ROUTER_BASE_URL', 'http://localhost:31314')}/wf1"  
    response = requests.get(url,timeout=180)
    # Process the response as needed
    print(f"URL: {url}, Status Code: {response.status_code}")

def main():
    # Read values from CSV file
    with open('trace.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    # Set the maximum number of concurrent threads for the entire program
    max_threads = 15 

    for row in rows:
        send_requests(row['Count_Scaled'], max_threads)

if __name__ == "__main__":
    main()
