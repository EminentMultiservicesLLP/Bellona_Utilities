import os
import time
import logging
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fileprocess as fileprocess
from logging_config import setup_logging
import pandas as pd
import re
import generic
import dbOperations


# Load environment variables from .env file
load_dotenv()

logging = setup_logging()

HEADER_ROW = int(os.getenv('HEADER_ROW')) - 1  # Convert to 0-based index
WATCH_DIRECTORY = os.getenv('WATCH_DIRECTORY')

# File Watcher Event Handler
class WatcherHandler(FileSystemEventHandler):


    def on_created(self, event):
        if event.is_directory:
            return None
        elif event.src_path.endswith(".xlsx"):
            logging.info(f"New file detected: {event.src_path}")
            dbOperations.execute_stored_procedure('dbsp_TBArchieveErrors', None)
            
            def read_excel_and_extract_data(file_path):
                fileId = 0
                with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                    df = pd.read_excel(xls, usecols=[0, 1, 2, 3, 4], nrows=HEADER_ROW)
                    #df = pd.read_excel(file_path, engine='openpyxl', usecols=[0, 1, 2,3,4], nrows=HEADER_ROW)  # Limit to 3 columns and 7 rows
                    #for cell in df.stack():  # Iterate over all cells
                    for (row_idx, col), cell in df.stack().items():
                        col_idx = df.columns.get_loc(col)
                        if isinstance(cell, str) and "Period =" in cell:
                            match = re.search(r"Period = (\w+ \d{4})", cell)
                            if match:
                                month, year = match.group(1).split()

                                if not isinstance(year, int):
                                    logging.error(f"Error, period field have wrong/missing year: {year} informathion")
                                    generic.log_error_to_db(f"Error, period field have wrong/missing year informathion, excel contains value:{cell}", row_idx, col_idx, 0)

                                month_numeric = generic.get_month_number(month)
                                if month_numeric:
                                    params= f'''@TBFileName ='{file_path.split('/')[-1]}', @TBMonth ={month_numeric}, @TBYear ={int(year)}, @FileId = @out output'''
                                    fileId = dbOperations.execute_stored_procedure('dbsp_InsertTBFileMonthYearLink', params, True, False)
                                    logging.debug(f"Inserted: {file_path}, Numeric Date: {(int(year), month_numeric)}")
                                else:
                                    logging.error(f"Error, period field have wrong/missing month: {month} informathion")
                                    generic.log_error_to_db(f"Error, period field missing Month information, excel contains value:{cell}", row_idx, col_idx, 0)
                                break;
                            else:
                                logging.error(f"Unable to find Perid data in excel")
                                generic.log_error_to_db(f"Error, period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)

                    del df
                    return fileId
            
            fileId = read_excel_and_extract_data(event.src_path)
            fileprocess.process_file(event.src_path, fileId)



# Watch directory for new files
def watch_directory():
    event_handler = WatcherHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    observer.start()
    logging.info(f"Watching directory: {WATCH_DIRECTORY}")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    watch_directory()

