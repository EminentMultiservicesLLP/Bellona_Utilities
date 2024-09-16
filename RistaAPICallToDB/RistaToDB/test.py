import pyodbc
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

def runSQL(query, is_DML =False):
    conn=pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+os.getenv("db_server")+';DATABASE='+os.getenv("database")+';UID='+os.getenv("UID")+';PWD='+os.getenv("PWD")+'')
    cursor = conn.cursor()
    cursor.execute(query)
    if is_DML : 
        cursor.commit()
        rows = cursor.rowcount
    else:
        rows = cursor.fetchall()
    conn.close()

    return rows

def Get_OutletList():
    rows = runSQL("SELECT OutletName, OutletCode, OutletID FROM MST_OUTLET where IsActive =1;")
    # conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=(localdb)\MSSQLLocalDB;DATABASE=BELLONA_LIVE;trusted_connection=yes')
    # cursor = conn.cursor()

    # query = 

    # cursor.execute(query)
    # rows = cursor.fetchall()

    # conn.close()
    return rows

def Excute_SQL(mapped_data,table_name,check_for_recordExists = 0, id_columns=None, isDML=False):

    where_clause = ""
    columns = ', '.join(mapped_data.keys())
    values = ', '.join([f"'{mapped_data[k]}'" for k in mapped_data.keys()])
    
    if (id_columns != None):
        update_clause = ', '.join([f"{k} = '{mapped_data[k]}'" for k in mapped_data.keys() if k not in id_columns])
    else:
        update_clause = ', '.join([f"{k} = '{mapped_data[k]}'" for k in mapped_data.keys()])
        
    if (id_columns != None):
        where_clause = ' and '.join([f"{field_key} = '{str(mapped_data[field_key])}'" for field_key in mapped_data.keys() if (field_key in id_columns)])
    
    if (where_clause.strip() != ""):
        where_clause = " WHERE " + where_clause

    query = f"""
    IF EXISTS (SELECT 1 FROM {table_name} {where_clause}) 
    BEGIN 
        UPDATE {table_name} SET {update_clause} {where_clause}
    END 
    ELSE 
    BEGIN 
        INSERT INTO {table_name} ({columns}) VALUES ({values})
    END
    """

    rows = runSQL(query.replace("\n",""), isDML)
    row_count    = rows
    return row_count

