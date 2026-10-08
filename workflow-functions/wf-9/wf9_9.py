from flask import Flask, request, jsonify
import requests
import time
import os
import random
import mysql.connector
from mysql.connector import errorcode

def main():
    # Receive JSON data from the POST request
    start_time = int(time.time())
    connection = mysqlconnection()
    cursor = connection.cursor()

    data = request.get_json()
    if data == {}:
        time.sleep(5)
        end_time = int((time.time()))               
        query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
        values = ("serverless_wf_9", "wf9_9",start_time,end_time,data['data'],500)
        cursor.execute(query,values)
        print("Query failed")
        connection.commit()
        connection.close()
        return str(data), 500
    else:
        print(f"Workflow step 9 received data: {data}")
        # Simulate some processing with a delay
        time.sleep(5)
        end_time = int((time.time()))               
        query = "INSERT INTO serverless_workflows (workflow_name, workflow_stage,start_time,end_time,uuid_passed,response_code) VALUES (%s,%s,%s,%s,%s,%s)"
        values = ("serverless_wf_9", "wf9_9",start_time,end_time,data['data'],200)
        cursor.execute(query,values)
        print("Query successfully executed")
        connection.commit()
        connection.close()
        return str(data), 200

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
