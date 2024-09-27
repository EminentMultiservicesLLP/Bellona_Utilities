import os
import pandas as pd
import logging
from dotenv import load_dotenv
import pyodbc
from logging_config import setup_logging
from datetime import datetime
import helper, dbOperations


# Load environment variables from .env file
load_dotenv()

logging = setup_logging()

HEADER_ROW = int(os.getenv('HEADER_ROW')) - 1  # Convert to 0-based index
DATA_START_ROW = int(os.getenv('DATA_START_ROW')) - 1  # Convert to 0-based index

expected_columns = {'Code', 'Particulars', 'Nature', 'MIS Head'}
start_branchcolumn_idex =(int(os.getenv("BRANCH_COLUMN_START"))) if os.getenv("BRANCH_COLUMN_START", None).isdigit() else (helper.column_letter_to_index(os.getenv("BRANCH_COLUMN_START", "E"))) 
end_branchcolumn_index = (int(os.getenv("BARNCH_COLUMN_END"))) if os.getenv("BARNCH_COLUMN_END", None).isdigit() else (helper.column_letter_to_index(os.getenv("BARNCH_COLUMN_END", "AB")))


def log_error_to_db(error_message, row_number, col_Number=None):
    """Log error message to the database."""
    try:
        error_details={'error_process':'TB Data Upload',
                       'errorMessage':error_message, 'rowNumber':row_number, 'colNumber':col_Number, 'error_time':datetime.now()}
        
        dbOperations.insert_entries('error_log', error_details)
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")

#validate mandatory fields available/missing in excel
def Validate_MandatoryFields(columns):
    # Validate column names
    actual_columns = set(columns)
    
    # Check for missing or extra columns
    if not expected_columns.issubset(actual_columns):
        missing_cols = expected_columns - actual_columns
        #extra_cols = actual_columns - expected_columns
        if missing_cols:
            logging.error(f"Missing columns: {', '.join(missing_cols)}")
            log_error_to_db(f"Missing columns: {', '.join(missing_cols)}", HEADER_ROW)
        return

def Validate_Data(code, particulars, nature, mis_head, index, row, columns):
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
            
        return True
    except ValueError as ve:
        logging.error(str(ve))
        log_error_to_db( str(ve), DATA_START_ROW+ (index-1))
        return False
    except Exception as e:
        logging.error(str(e))
        log_error_to_db( str(e), DATA_START_ROW+ (index-1))
        return False

# Process the Excel file and insert data into the database
def process_file(file_path):
    cursor = None
    try:
        # Read Excel file starting from the configured header row and actual data row
        df = pd.read_excel(file_path, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW+1), usecols=range(end_branchcolumn_index))
        
        code_col_index = (int(os.getenv("col_code", 1)) - 1) if os.getenv("col_code", None).isdigit() else 0
        particulars_col_index = (int(os.getenv("col_particulars", 2)) - 1) if os.getenv("col_particulars", None).isdigit() else 1
        nature_col_index = (int(os.getenv("col_nature", 3)) - 1) if os.getenv("col_nature", None).isdigit() else 2
        misheads_col_index = (int(os.getenv("col_mis_heads", 4)) - 1) if os.getenv("col_mis_heads", None).isdigit() else 3

        with pyodbc.connect(dbOperations.connect_db()) as conn:
            # Create a cursor
            cursor = conn.cursor()

            Validate_MandatoryFields(df.columns)
            for index, row in df.iterrows():
                try:
                    logging.info("started new row reading")
                    code = row[code_col_index]  # Code data from column A
                    particulars = row[particulars_col_index]  # Particulars data from column B
                    nature = row[nature_col_index]  # Nature data from column D
                    mis_head = row[misheads_col_index]  # MIS head data from column D

                    logging.info(f"Data at row {index} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

                    #break the loop as soon as we hit the total row
                    if (particulars.strip().lower() == "total"): 
                        logging.debug("Reached at Total row at index {index}")
                        conn.commit()
                        break

                    #validate basis data expected for fields
                    if(Validate_Data(code, particulars, nature, mis_head, index, row, df.columns)):
                        logging.debug("Data Validation at row index {index} completed")
                        
                        # Insert or get MIS_head ID
                        mis_head_id = dbOperations.insert_or_get_id(cursor, "TB_MISHead", "head_name", mis_head, {'head_name': mis_head, 'nature': nature}, index)
                        logging.debug(f"Received MIS Head ID {mis_head}")

                        # Insert or get Particulars ID
                        particulars_id =  dbOperations.insert_or_get_id(cursor, "TB_Particulars", "particulars", particulars, {'code': code, 'particulars': particulars}, index)
                        logging.debug(f"Received Particulars ID {particulars_id}")

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
                                    log_error_to_db(f"Error inserting trial balance for row index {index}: {e}", DATA_START_ROW+ (index-1), start_branchcolumn_idex + col_index)

                            logging.info(f"Commiting single row Data into DB for MIS Head {mis_head_id}, Particulars {particulars_id}.")
                            conn.commit()
                    
                except ValueError as v:
                    logging.error(f"Value error: {v}")
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")        
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")



