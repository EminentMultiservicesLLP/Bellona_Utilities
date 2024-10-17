import pyodbc
import os
import time
from dotenv import load_dotenv, dotenv_values 
from Logging_config import setup_logging
from RistaCalls.GenericClass import executeType as executeType

load_dotenv() 
# Initialize the logger
logger = setup_logging()

def connect_db_sqlalchemy():
    try:
        password = os.getenv("PWD") .replace("@", "%40")
        if os.getenv("db_environment", "").lower() in ["prod", "production", "live"]:
            connection_string = f'mssql+pyodbc://{os.getenv("UID")}:{password}@{os.getenv("DB_SERVER")}/{os.getenv("database")}?driver=ODBC+Driver+17+for+SQL+Server'
        else:
            connection_string = f'mssql+pyodbc://{os.getenv("DB_SERVER")}/{os.getenv("database")}?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes'
        return connection_string
    except pyodbc.Error as e:
        logger.error(f"Error forming DB connection string: {e}")
        return None

global_cursor = None
def set_cursor(cursor):
    global global_cursor
    global_cursor = cursor


def reconnect_and_execute(query, data=None, executeAs=executeType):
    max_retries = 3
    retries = 0
    while retries < max_retries:
        try:
            logger.info(f"data object is null for  {executeAs.name}")
            if data is None :
                logger.info(f"Executing query without data for {executeAs.name}")
                global_cursor.execute(query)
            else:
                logger.info(f"Executing query with data for {executeAs.name}")
                global_cursor.execute(query, data)
            logger.info(f"Query executed for  {executeAs.name}")

            if executeAs == executeType.Scalar: 
                result = global_cursor.fetchone()
                if result is None:
                    logger.info("Query returned no results")
                    output = None
                else:
                    logger.info(f"Query executed and first row, first column value fetched for {executeAs.name}")
                    output =  result[0]  # Return the scalar value
            elif executeAs == executeType.NonQuery: 
                output= global_cursor.rowcount
                logger.info(f"Query executed and affected row count returned for {executeAs.name}")
            elif executeAs == executeType.Reader : 
                rows=global_cursor.fetchall()
                if not rows:
                    logger.info("Query returned no results")
                    output =  []  # Return an empty list if no rows are found
                else:
                    output = rows
                logger.info(f"Query executed and all rows fetched for {executeAs.name}")

            if not data is None : 
                global_cursor.commit()
                logger.info(f"Committed change to Database for {executeAs.name}")
            return output
        except pyodbc.OperationalError as e:
            retries += 1
            logger.error(f"Database OperationalError: {e}. Retrying ({retries}/{max_retries})...")
            time.sleep(5)  # Delay before retry
        except pyodbc.Error as e:
            logger.error(f"Database error occurred in  {executeAs.name}: {str(e)}", exc_info=True)
            raise  # Re-raise the exception after logging

        except Exception as e:
            logger.error(f"An error occurred in  {executeAs.name}: {str(e)}", exc_info=True)
            raise  # Re-raise the exception after logging
    raise Exception("Failed after multiple retries")

def executeQuery(query, data=None, executeAs=executeType):
    return reconnect_and_execute(query, data, executeAs)


def executeMany(query, data=None):
    try:
        logger.info(f"executeMany call received")
        if not data is None and len(data) >0 :
            logger.info(f"Executing query with data")
            if not (isinstance(data, list) or isinstance(data,tuple)):
                global_cursor.executemany(query, list(data))
            else : global_cursor.executemany(query, data)
            logger.info(f"Query executed")

        if not data is None : 
            global_cursor.commit()
            logger.info(f"Committed change to Database")

    except pyodbc.Error as e:
        logger.error(f"Database error occurred : {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging

#Execute Non-Query:
def executeNonQuery(query, data =None):
    logger.info("data object is null for executeNonQuery")
    if data is None :
        global_cursor.execute(query)
    else:
        global_cursor.execute(query, data)

    logger.info("Query executed for executeNonQuery")
    global_cursor.commit()
    logger.info("Query commited for executeNonQuery")
    rows = global_cursor.rowcount
    logger.info("Call completed for executeNonQuery")
    return rows

def executeScalar(query, data =None):
    try:
        logger.info("Call received for executeScalar")
        
        logger.info("data object is null for executeScalar")
        if data is None :
            logger.info("Executing query without data for executeScalar")
            global_cursor.execute(query)
        else:
            logger.info("Executing query with data for executeScalar")
            global_cursor.execute(query, data)
        logger.info("Query executed for executeScalar")

        scalar_value = global_cursor.fetchone()[0]
        logger.info("Query executed and first row, first column value fetched for executeScalar")
       
        return scalar_value
     
    except pyodbc.Error as e:
        logger.error(f"Database error occurred in executeScalar: {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging

    except Exception as e:
        logger.error(f"An error occurred in executeScalar: {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging
    
#Execute Reader (fetching multiple rows)
def executeReader(query, data =None):
    try:
        logger.info("Call received for executeReader")
        if data is None :
            logger.info("Executing query without data for executeReader")
            global_cursor.execute(query)
        else:
            logger.info("Executing query with data for executeReader")
            global_cursor.execute(query, data)
        logger.info("Query executed for executeReader")

        rows = global_cursor.fetchall()
        logger.info("Query executed and rows fetched for executeReader")
        
        return rows
    except pyodbc.Error as e:
        logger.error(f"Database error occurred: {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise  # Re-raise the exception after logging

def runSQL(query, data =None, is_DML =False):
    if data is None :
        global_cursor.execute(query)
    else:
        global_cursor.execute(query, data)
                       
    if is_DML : 
        global_cursor.commit()
        rows = global_cursor.rowcount
    else:
        rows = global_cursor.fetchall()

    return rows

def Get_OutletList():
    # rows = runSQL("SELECT OutletName, OutletCode, OutletID FROM MST_OUTLET where IsActive =1;")
    # return rows
    return executeReader("SELECT OutletName, OutletCode, OutletID FROM MST_OUTLET where IsActive =1;")


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

def insert_into_table(table_name, data, ownQuery=None):
    columns = ', '.join(data.keys())
    placeholders = ', '.join('?' * len(data))  # Use ? for each value to prevent SQL injection

    # if ownQuery is None:
    #     query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    #     runSQL(query, list(data.values()), True)
    # else:
    
    query = f"""SET NOCOUNT ON;
                DECLARE @table_identity TABLE(InvoiceID int);
                INSERT INTO {table_name} ({columns})
                OUTPUT inserted.InvoiceID INTO @table_identity(InvoiceID) 
                VALUES({placeholders});
                SELECT InvoiceID FROM @table_identity;
                """
    
    if os.getenv("db_environment", "").lower() in ["prod", "production", "live"]:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("db_server")};;DATABASE={os.getenv("database")};;UID={os.getenv("UID")};;PWD={os.getenv("PWD")};;'
    else:
        connection_string=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("db_server")};;DATABASE={os.getenv("database")};;trusted_connection=yes'
    conn=pyodbc.connect(connection_string)
    cursor = conn.cursor()

    global_cursor.execute(query, list(data.values()))
    Invoice_Id = global_cursor.fetchone()[0]
    print(Invoice_Id)



