import os
import pandas as pd
import logging
from dotenv import load_dotenv
import pyodbc
from datetime import datetime
from logging_config import setup_logging
logging = setup_logging()

# Load environment variables from .env file
load_dotenv()



DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Connect to the MS SQL database
def connect_db():
    try:
        if os.getenv("DB_Local").lower() != 'yes':
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("DB_SERVER")};;DATABASE={os.getenv("DB_DATABASE")};;UID={os.getenv("DB_USER")};;PWD={os.getenv("DB_PASSWORD")};;'
        else:
            connection_string=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("DB_SERVER")};;DATABASE={os.getenv("DB_DATABASE")};;trusted_connection=yes'
        
        
        return connection_string
    except pyodbc.Error as e:
        logging.error(f"Error forming DB connection string: {e}")
        return None

def execute_stored_procedure(stored_procedure, params, returnAsTuple_KeyValue = True):
    try:
        with pyodbc.connect(connect_db()) as conn:
            # Create a cursor
            cursor = conn.cursor()

            # Create the SQL statement to execute the stored procedure
            # For example, if the procedure has 3 parameters, this will be: EXEC stored_procedure ?, ?, ?
            placeholders = ', '.join('?' * len(params)) if params  else ''

            sql = f"EXEC {stored_procedure} {placeholders if placeholders and placeholders.strip() != '' else ''}"

            if params != None and len(params) > 0:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            if cursor.description:
                columns  = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                if returnAsTuple_KeyValue and columns:
                    dict_list = []
                    for row in results:
                        # Create a dictionary for each row using column names as keys
                        row_dict = {columns[i]: row[i] for i in range(len(columns))}
                        dict_list.append(row_dict)
                    
                    return columns,dict_list
            else:
                return None, None
    except pyodbc.Error as e:
        logging.error(f"Error occured while executing SP, error: {e}")
        return None
    
# Handle database operations (insert if not exists)
def insert_entries(table, insert_data):
    try:
        columns = ', '.join(insert_data.keys())
        placeholders = ', '.join(['?'] * len(insert_data))
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, tuple(insert_data.values()))
            cursor.connection.commit()
    except pyodbc.Error as e:
        logging.error(f"Error while checking or inserting for {table}: {e}")


# Handle database operations (insert if not exists)
def getData(table, lookup_field, lookup_value,returnAsTuple_KeyValue=True):
    try:
        search_query = f"SELECT * FROM {table} WHERE {lookup_field} = ?"
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(search_query, lookup_value)
            columns = [column[0] for column in cursor.description]
            results = cursor.fetchall()

            if returnAsTuple_KeyValue:
                dict_list = []
                for row in results:
                    # Create a dictionary for each row using column names as keys
                    row_dict = {columns[i]: row[i] for i in range(len(columns))}
                    dict_list.append(row_dict)
                
                return columns,dict_list
            else:
                return columns, results
    except pyodbc.Error as e:
        logging.error(f"Error while checking or inserting for {table}: {e}")


# Handle database operations (insert if not exists)
def custom_query_execute(query, isCommittRequire=False):
    try:
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            if(isCommittRequire): cursor.connection.commit()
    except pyodbc.Error as e:
        logging.error(f"Error while executing custom query {query}, error: {e}")


def InsertDocumentdetails_DB(params):
    # Prepare the SQL statement to call the stored procedure
    placeholders = ', '.join('?' * len(params))

    sql = f"""
    DECLARE @Return INT;
    EXEC {os.getenv("sql_insertDocument")} {placeholders if placeholders and placeholders.strip() != '' else ''}, @Return OUTPUT;
    SELECT @Return AS ReturnOutput;
    """
    try:
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)

            ReturnOutput = cursor.fetchone()[0]

            cursor.commit()

            return ReturnOutput
    except pyodbc.Error as e:
        logging.error(f"Error while executing custom query {sql}, error: {e}")