import os
import pandas as pd
import logging
from dotenv import load_dotenv
import pyodbc
from logging_config import setup_logging
from datetime import datetime


# Load environment variables from .env file
load_dotenv()

logging = setup_logging()

HEADER_ROW = int(os.getenv('HEADER_ROW')) - 1  # Convert to 0-based index
DATA_START_ROW = int(os.getenv('DATA_START_ROW')) - 1  # Convert to 0-based index

def column_letter_to_index_111(column_letter):
    """Convert column letter(s) to a zero-based index."""
    logging.info(f'column_letter:{column_letter}')
    column_letter = column_letter.upper()
    index = 0
    for char in column_letter:
        logging.info(f'Index:{index}, Char:{ord(char)}, A : {ord('A')}')
        index = index + (ord(char) - ord('A'))
        logging.info(f'Calculated Index:{index}')
    index = ((len(column_letter)) * 26)+ index
    logging.info(f'Final  Index:{index}')
    return index

def column_letter_to_index(s):
    # This process is similar to binary-to- 
    # decimal conversion 
    result = 0; 
    for B in range(len(s)): 
        result *= 26; 
        result += ord(s[B]) - ord('A') + 1; 
 
    return result; 

expected_columns = {'Code', 'Particulars', 'Nature', 'MIS Head'}
start_branchcolumn_idex =(int(os.getenv("BRANCH_COLUMN_START"))) if os.getenv("BRANCH_COLUMN_START", None).isdigit() else (column_letter_to_index(os.getenv("BRANCH_COLUMN_START", "E"))) 
end_branchcolumn_index = (int(os.getenv("BARNCH_COLUMN_END"))) if os.getenv("BARNCH_COLUMN_END", None).isdigit() else (column_letter_to_index(os.getenv("BARNCH_COLUMN_END", "AB")))

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
        
        connection = pyodbc.connect(connection_string)
        return connection
    except pyodbc.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def log_error_to_db(conn, error_message, row_number, col_Number=None):
    """Log error message to the database."""
    try:
        if(isinstance(conn, cursor)):
            cursor = conn
        else:
            cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO error_log (error_process, errorMessage, rowNumber, colNumber, error_time) VALUES (?, ?, ?, ?, ?)",
            ('TB Data Upload',error_message, row_number, col_Number, datetime.now())
        )
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")


# Handle database operations (insert if not exists)
def insert_or_get_id(cursor, table, lookup_field, lookup_value, insert_data, index):
    try:
        search_query = f"SELECT * FROM {table} WHERE {lookup_field} = ?"
        cursor.execute(search_query, lookup_value)
        result = cursor.fetchone()
        if not result:
            columns = ', '.join(insert_data.keys())
            placeholders = ', '.join(['?'] * len(insert_data))
            insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(insert_data.values()))
            #cursor.connection.commit()

            logging.info(f"New entry created for {table} with {tuple(insert_data.values())}")
            cursor.execute(search_query, lookup_value)
            result = cursor.fetchone()

        logging.info(f"Entry already exists in {table} for {lookup_field} as {lookup_value}")
        return result[0]
    except pyodbc.Error as e:
        logging.error(f"Error while checking or inserting for {table}: {e}")
        log_error_to_db(cursor, f"Error while checking or inserting for {table}: {e}", DATA_START_ROW+ (index-1))

#validate mandatory fields available/missing in excel
def Validate_MandatoryFields(columns, conn):
    # Validate column names
    
    actual_columns = set(columns)
    
    # Check for missing or extra columns
    if not expected_columns.issubset(actual_columns):
        missing_cols = expected_columns - actual_columns
        #extra_cols = actual_columns - expected_columns
        if missing_cols:
            logging.error(f"Missing columns: {', '.join(missing_cols)}")
            log_error_to_db(conn, f"Missing columns: {', '.join(missing_cols)}", HEADER_ROW)
        return

def Validate_Data(code, particulars, nature, mis_head, index, row, columns, conn):
    try:
        logging.info("started new row reading")
        # Check if 'Code' is valid
        if not isinstance(code, str):
            raise ValueError(f"Invalid Code at row {index+1}")
        
        # Check if 'Particulars' is valid
        if not isinstance(particulars, str) or not particulars.strip():
            raise ValueError(f"Invalid Particulars at row {index+1}")

        # Check if 'MIS HEAD' is valid
        if not isinstance(mis_head, str) or not mis_head.strip():
            raise ValueError(f"Invalid MIS head at row {index+1}")

        #Check if numeric columns contain valid numbers
        numeric_cols = [col for col in columns if col not in expected_columns]
        for col in numeric_cols:
            try:
                # Convert to float to validate
                float(row[col])
            except ValueError:
                raise ValueError(f"Invalid value in column '{col}' at row {index+1}")
    except ValueError as ve:
        logging.error(str(ve))
        log_error_to_db(conn, str(ve), DATA_START_ROW+ (index-1))

# Process the Excel file and insert data into the database
def process_file(file_path):
    cursor = None
    try:
        # Read Excel file starting from the configured header row and actual data row
        df = pd.read_excel(file_path, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW+1), usecols=range(end_branchcolumn_index))
        
        connection = connect_db()
        if connection is None:
            logging.error("Database connection failed, skipping file.")
            return
        cursor = connection.cursor()

        code_col_index = (int(os.getenv("col_code", 1)) - 1) if os.getenv("col_code", None).isdigit() else 0
        particulars_col_index = (int(os.getenv("col_particulars", 2)) - 1) if os.getenv("col_particulars", None).isdigit() else 1
        nature_col_index = (int(os.getenv("col_nature", 3)) - 1) if os.getenv("col_nature", None).isdigit() else 2
        misheads_col_index = (int(os.getenv("col_mis_heads", 4)) - 1) if os.getenv("col_mis_heads", None).isdigit() else 3

        Validate_MandatoryFields(df.columns, connection)
        for index, row in df.iterrows():
            try:
                logging.info("started new row reading")
                code = row[code_col_index]  # Code data from column A
                particulars = row[particulars_col_index]  # Particulars data from column B
                nature = row[nature_col_index]  # Nature data from column D
                mis_head = row[misheads_col_index]  # MIS head data from column D

                logging.info(f"Data at row {index} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

                #validate basis data expected for fields
                Validate_Data(code, particulars, nature, mis_head, index, row, df.columns, connection)
                logging.info("Data Validation at row index {index} completed")
                
                # Insert or get MIS_head ID
                mis_head_id = insert_or_get_id(cursor, "TB_MISHead", "head_name", mis_head, {'head_name': mis_head, 'nature': nature}, index)
                logging.info(f"Received MIS Head ID {mis_head}")

                # Insert or get Particulars ID
                particulars_id = insert_or_get_id(cursor, "TB_Particulars", "particulars", particulars, {'code': code, 'particulars': particulars}, index)
                logging.info(f"Received Particulars ID {particulars_id}")

                if mis_head_id and particulars_id:
                    # For each branch column start position
                    for branch_col in df.columns[start_branchcolumn_idex:]:
                        
                        branch_id = branch_col  # Assuming branch name or id is in the column header
                        tb_amount = row[branch_col]
                        tb_date = pd.Timestamp.today().date()  # Or adjust as per file
                        
                        # Insert into TB_TrialBalance
                        try:
                            cursor.execute("""
                                INSERT INTO TB_TrialBalance (branch_id, head_id, particulars_id, tb_date, tb_amount)
                                VALUES (?, ?, ?, ?, ?)
                            """, (branch_id, mis_head_id, particulars_id, tb_date, tb_amount))
                            logging.info(f"Data inserted for Branch {branch_id}, MIS Head {mis_head_id}, Particulars {particulars_id}.")
                        except pyodbc.Error as e:
                            logging.error(f"Error inserting trial balance for row index {index}: {e}")
                            log_error_to_db(cursor, f"Error inserting trial balance for row index {index}: {e}", DATA_START_ROW+ (index-1), start_branchcolumn_idex + col_index)

                    logging.info(f"Commiting single row Data into DB for MIS Head {mis_head_id}, Particulars {particulars_id}.")
                    connection.commit()
            except ValueError as v:
                logging.error(f"Value error: {v}")
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")        
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

    finally:
        if cursor is not None:
            connection.commit()
            cursor.close()
        if connection is not None and not connection.closed:
            connection.close()


