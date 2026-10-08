import os
from flask import request
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
    
    try:
        end_time = round(time.time(), 3)
        log_to_database(cursor, "serverless_chain_wf", "chain_4", start_time, end_time, data['data'], 200)
        
        connection.commit()
        return "Final data received and logged", 200
    
    except Exception as e:
        end_time = round(time.time(), 3)
        log_to_database(cursor, "serverless_chain_wf", "chain_4", start_time, end_time, '', 500)
        connection.commit()
        return str(e), 500
    
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
