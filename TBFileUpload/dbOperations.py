import pyodbc
import config
from logging_config import setup_logging

logging = setup_logging()

DB_CONFIG = {
    'server': config.get_env_variable('DB_SERVER'),
    'database': config.get_env_variable('DB_DATABASE'),
    'username': config.get_env_variable('DB_USER'),
    'password': config.get_env_variable('DB_PASSWORD')
}

# Connect to the MS SQL database
def connect_db():
    try:
        if config.get_env_variable("DB_Local").lower() != 'yes':
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.get_env_variable("DB_SERVER")};;DATABASE={config.get_env_variable("DB_DATABASE")};;UID={config.get_env_variable("DB_USER")};;PWD={config.get_env_variable("DB_PASSWORD")};;'
        else:
            connection_string=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.get_env_variable("DB_SERVER")};;DATABASE={config.get_env_variable("DB_DATABASE")};;trusted_connection=yes'
        
        
        return connection_string
    except pyodbc.Error as e:
        logging.error(f"Error forming DB connection string: {e}")
        return None

def connect_db_sqlalchemy():
    try:
        password = config.get_env_variable("DB_PASSWORD") .replace("@", "%40")
        if config.get_env_variable("DB_Local").lower() != 'yes':
            connection_string = f'mssql+pyodbc://sa:{password}@{config.get_env_variable("DB_SERVER")}/{config.get_env_variable("DB_DATABASE")}?driver=ODBC+Driver+17+for+SQL+Server'
        else:
            connection_string = f'mssql+pyodbc://{config.get_env_variable("DB_SERVER")}/{config.get_env_variable("DB_DATABASE")}?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes'
        return connection_string
    except pyodbc.Error as e:
        logging.error(f"Error forming DB connection string: {e}")
        return None


def execute_stored_procedure(stored_procedure, params, outputParams = False, returnAsTuple_KeyValue = True):
    try:
        with pyodbc.connect(connect_db()) as conn:
            # Create a cursor
            cursor = conn.cursor()

            # Create the SQL statement to execute the stored procedure
            # For example, if the procedure has 3 parameters, this will be: EXEC stored_procedure ?, ?, ?
            if outputParams:
                sql = f"""\
                    SET NOCOUNT ON;
                    DECLARE @out INT;
                    EXEC {stored_procedure} {params if not params is None else ''};
                    SELECT @out AS the_output;
                    """
            else:
                #sql = f"Exec {stored_procedure} ({placeholders if placeholders and placeholders.strip() != '' else ''})"
                sql = f" Exec {stored_procedure} {params if not params is None else ''}"
            
            cursor.execute(sql)

            if outputParams:
                result = cursor.fetchval()
                while True:
                    if cursor.nextset():
                        rows = cursor.fetchall()
                    else:
                        break
            
                return result
            else:
                return None

    except pyodbc.Error as e:
        logging.error(f"Error occured while executing SP, error: {e}")
        return None
    except pyodbc.ProgrammingError:
        # In case there's no result set, return None
        return None
# Handle database operations (insert if not exists)
def insert_entry_single(table, insert_data):
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

def insert_entries_many(table, columns, insert_data):
    try:
        columns_joined = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table} ({columns_joined}) VALUES ({placeholders})"
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.executemany(insert_query, insert_data)
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

def getData_withoutFilter(table, returnAsTuple_KeyValue=True):
    try:
        search_query = f"SELECT * FROM {table}"
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(search_query)
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

def getData_scalar(cursor, table, return_col_name, lookup_field, lookup_value):
    newCursorOpenhere = False
    try:
        if cursor is None or not isinstance(cursor, pyodbc.Cursor):
            conn = pyodbc.connect(connect_db())
            cursor = conn.cursor()
            newCursorOpenhere = True

        if lookup_field and lookup_value:
            search_query = f"SELECT {return_col_name} FROM {table} WHERE {lookup_field} =?"
            cursor.execute(search_query, lookup_value)
        else:
            search_query = f"SELECT {return_col_name} FROM {table}"
            cursor.execute(search_query)
        result = cursor.fetchone()

        return result[0]
    except pyodbc.Error as e:
        logging.error(f"Error while checking or inserting for {table}: {e}")
    finally:
        if newCursorOpenhere:
            if cursor is not None:
                conn.commit()
                cursor.close()
            if conn is not None and not conn.closed:
                conn.close()

# Handle database operations (insert if not exists)
def custom_query_execute(query, isCommittRequire=False):
    try:
        with pyodbc.connect(connect_db()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            if(isCommittRequire): cursor.connection.commit()
    except pyodbc.Error as e:
        logging.error(f"Error while executing custom query {query}, error: {e}")

# Handle database operations (insert if not exists)
def insert_or_get_id(cursor, table, lookup_field, lookup_value, insert_data, index):
    newCursorOpenhere = False
    try:
        
        search_query = f"SELECT * FROM {table} WHERE {lookup_field} = ?"
        if cursor is None and isinstance(cursor, cursor):
            conn = pyodbc.connect(connect_db())
            cursor = conn.cursor()
            newCursorOpenhere = True

        cursor.execute(search_query, lookup_value)
        result = cursor.fetchone()
        if not result:
            columns = ', '.join(insert_data.keys())
            placeholders = ', '.join(['?'] * len(insert_data))
            insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(insert_data.values()))
            #cursor.connection.commit()

            logging.debug(f"New entry created for {table} with {tuple(insert_data.values())}")
            cursor.execute(search_query, lookup_value)
            result = cursor.fetchone()

        logging.debug(f"Entry already exists in {table} for {lookup_field} as {lookup_value}")
        return result[0]
    except pyodbc.Error as e:
        logging.error(f"Error while checking or inserting for {table}: {e}")
        #log_error_to_db(cursor, f"Error while checking or inserting for {table}: {e}", DATA_START_ROW+ (index-1))
    finally:
        if newCursorOpenhere:
            if cursor is not None:
                conn.commit()
                cursor.close()
            if conn is not None and not conn.closed:
                conn.close()