from flask import Flask, request, jsonify
import requests
import time
import os
import random
import mysql.connector
from mysql.connector import errorcode

def main():
    # Receive JSON data from the POST request
    data = request.get_json()
    print(f"Workflow step 2 received data: {data}")
    start_time = int(time.time())
    # Simulate some processing with a delay
    time.sleep(5)
    connection = mysqlconnection()
    cursor = connection.cursor()
    
    # Forward the data to the next service
    forward_url = "http://router.fission.svc.cluster.local/fission-function/chain3"
    response = requests.post(forward_url, json=data)
    try:
        if response.status_code == 200:
            end_time = int((time.time()))                  
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_chain_wf", "chain_2",start_time,end_time,data['data'],200)
            cursor.execute(query,values)
            print("Query successfully executed")
            connection.commit()
            connection.close()
            return "Data forwarded successfully",200
        else:
            end_time = int((time.time()))                  
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_chain_wf", "chain_2",start_time,end_time,'',response.status_code)
            cursor.execute(query,values)
            connection.commit()
            connection.close()
            return "Failed to forward data", response.status_code
    except requests.RequestException as e:
        end_time = int((time.time()))                  
        query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
        values = ("serverless_chain_wf", "chain_2",start_time,end_time,'',500)
        cursor.execute(query,values)
        connection.commit()
        connection.close()
        return f"Failed to forward data {str(e)}", 500

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