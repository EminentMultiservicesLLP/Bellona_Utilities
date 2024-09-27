import logging
from dotenv import load_dotenv
from logging_config import setup_logging
from datetime import datetime
import dbOperations

# Load environment variables from .env file
load_dotenv()

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


def log_error_to_db(error_message, row_number, col_Number=None, fileId=0):
    """Log error message to the database."""
    try:
        error_details={'error_process':'TB Data Upload','fileId':fileId,
                       'errorMessage':error_message, 'rowNumber':row_number, 'colNumber':col_Number, 'error_time':datetime.now()}
        
        dbOperations.insert_entries('TB_error_log', error_details)
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")

