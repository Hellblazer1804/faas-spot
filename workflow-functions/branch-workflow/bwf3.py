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
    print(f"Workflow 3 received data: {data}")
    start_time = int(time.time())
    # Simulate some processing with a delay
    time.sleep(5)
    branch = random.randint(0,1)
    connection = mysqlconnection()
    cursor = connection.cursor()

    # Forward the data to the next service branch
    if branch == 0:
        forward_url_1 = "http://router.fission.svc.cluster.local/fission-function/branch3a"
        response = requests.post(forward_url_1, json=data)
        try:
            if response.status_code == 200:
                end_time = int((time.time()))                  
                query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
                values = ("serverless_branch_wf", "branch_3",start_time,end_time,data['data'],200)
                cursor.execute(query,values)
                print("Query successfully executed")
                connection.commit()
                connection.close()
                return "Data forwarded successfully",200
            else:
                end_time = int((time.time()))                  
                query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
                values = ("serverless_branch_wf", "branch_3",start_time,end_time,'',response.status_code)
                cursor.execute(query,values)
                connection.commit()
                connection.close()
                return "Failed to forward data", response.status_code
        except requests.RequestException as e:
            end_time = int((time.time()))                  
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_branch_wf", "branch_3",start_time,end_time,'',500)
            cursor.execute(query,values)
            connection.commit()
            connection.close()
            return f"Failed to forward data {str(e)}", 500
    
    # Forward the data to the parallel service branch
    else:
        forward_url_2 = "http://router.fission.svc.cluster.local/fission-function/branch3b"
        response = requests.post(forward_url_2, json=data)
        try:
            if response.status_code == 200:
                end_time = int((time.time()))                  
                query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
                values = ("serverless_branch_wf", "branch_3",start_time,end_time,data['data'],200)
                cursor.execute(query,values)
                print("Query successfully executed")
                connection.commit()
                connection.close()
                return "Data forwarded successfully",200
            else:
                end_time = int((time.time()))                  
                query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
                values = ("serverless_branch_wf", "branch_3",start_time,end_time,'',response.status_code)
                cursor.execute(query,values)
                connection.commit()
                connection.close()
                return "Failed to forward data", response.status_code
        except requests.RequestException as e:
            end_time = int((time.time()))                  
            query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
            values = ("serverless_branch_wf", "branch_3",start_time,end_time,'',500)
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


