import os
from flask import request
import requests
import time
import mysql.connector

def main():
    data = request.get_json()
    start_time = round(time.time(), 3)
    time.sleep(5)
    connection = mysqlconnection()
    if connection is None:
        return "Database connection failed", 500
    
    cursor = connection.cursor()
    forward_url = "http://router.fission.svc.cluster.local/fission-function/chain4"
    
    try:
        response = requests.post(forward_url, json=data)
        end_time = round(time.time(), 3)
        log_to_database(cursor, "serverless_chain_wf", "chain_3", start_time, end_time, data['data'], response.status_code)
        
        connection.commit()
        
        if response.status_code == 200:
            return "Data Forwarded successfully", 200
        else:
            log_to_database(cursor, "serverless_chain_wf", "chain_3", time.time(), time.time(), data['data'], response.status_code)
            return "Failed to forward data", response.status_code
    
    except requests.RequestException as e:
        print(f"Request error: {e}")
        end_time = round(time.time(), 3)
        log_to_database(cursor, "serverless_chain_wf", "chain_3", start_time, end_time, data['data'], 500)
        connection.commit()
        return "Failed to forward data", 500
    
    finally:
        connection.close()


def mysqlconnection():
    try:
        return mysql.connector.connect(user=os.getenv('DB_USER', ''), password=os.getenv('DB_PASSWORD', ''), host=os.getenv('DB_HOST', 'localhost'), database=os.getenv('DB_NAME', ''))
    except mysql.connector.Error as err:
        print("Database connection error:", err)
        return None

def log_to_database(cursor, workflow_name, stage, start_time, end_time, uuid, response_code):
    query = ("INSERT INTO serverless_workflows "
             "(workflow_name, workflow_stage, start_time, end_time, uuid_passed, response_code) "
             "VALUES (%s, %s, %s, %s, %s, %s)")
    values = (workflow_name, stage, start_time, end_time, uuid, response_code)
    cursor.execute(query, values)
