import requests
import time
import os
import uuid
import json
import random
import mysql.connector
from mysql.connector import errorcode

def main():
    # Generate UUID and prepare data
    data = str(uuid.uuid4())
    print(f"Workflow step 1 generated data: {data}")
    # Convert data to a dictionary if the receiving service expects JSON format
    json_data = {"data": data}
    start_time = int((time.time()))
    # Simulate some processing with a delay (optional)
    time.sleep(5)
    connection = mysqlconnection()
    cursor = connection.cursor()
    
    # Get Fission URL from environment variable
    
    # Define the forward URL
    forward_url = "http://router.fission.svc.cluster.local/fission-function/wf52"
    
    try:
        # Send the POST request
        response = requests.post(forward_url, json=json_data)
        
        # Check response status and return appropriate messages
        if response.status_code == 200:
            end_time = int(time.time())
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_wf_5", "wf5_1",start_time,end_time,data,200)
            cursor.execute(query,values)
            print("Query successfully executed")
            connection.commit()
            connection.close()
            return "Data forwarded successfully", 200
        else:
            end_time = int(time.time())
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_wf_5", "wf5_1",start_time,end_time,'',response.status_code)
            cursor.execute(query,values)
            connection.commit()
            connection.close()
            return f"Failed to forward data: {response.status_code} - {response.text}", response.status_code
    
    except requests.RequestException as e:
        # Return exception details
        end_time = int(time.time())
        query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
        values = ("serverless_wf_5", "wf5_1",start_time,end_time,'',500)
        cursor.execute(query,values)
        connection.commit()
        connection.close()
        return f"Request failed: {str(e)}", 500

def mysqlconnection():
    try:
        cnx = mysql.connector.connect(user=os.getenv('DB_USER', ''), password=os.getenv('DB_PASSWORD', ''), host=os.getenv('DB_HOST', 'localhost'), database=os.getenv('DB_NAME', ''))
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)