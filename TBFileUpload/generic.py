import logging
from logging_config import setup_logging
from datetime import datetime
import dbOperations

logging = setup_logging()

def get_month_number(month_str):
    # Convert to lowercase to handle case-insensitive matches
    month_str = month_str.lower()

    # Month mapping (only first three characters needed)
    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
        "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    month_short = month_str[:3].lower()  # Take the first three characters and make it lowercase
    return month_mapping.get(month_short, 0)  # Return 0 if no match is found


def log_error_to_db(error_message, row_number, col_Number=None, col_Name='', fileId=0):
    """Log error message to the database."""
    try:
        error_details={'error_process':'TB Data Upload','fileId':fileId,
                       'errorMessage':error_message, 'rowNumber':row_number, 'colNumber':col_Number, 'colName':col_Name, 'error_time':datetime.now()}
        
        dbOperations.insert_entry_single('TB_error_log', error_details)
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")

def log_error_to_db_many(error_list):
    """Log error message to the database."""
    try:
        columns=['error_process', 'errorMessage', 'rowNumber', 'colNumber', 'colName','fileId','error_time']
        #sql_ = "INSERT INTO TB_error_log (error_process, errorMessage,rowNumber, colNumber, colName, fileId, error_time) VALUES (?,?,?,?,?,?)"

        dbOperations.insert_entries_many('TB_error_log', columns, error_list)
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")

