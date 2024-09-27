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
            
            def read_excel_and_extract_data(file_path):
                df = pd.read_excel(file_path, engine='openpyxl', usecols=[0, 1, 2,3,4], nrows=HEADER_ROW)  # Limit to 3 columns and 7 rows
                for cell in df.stack():  # Iterate over all cells
                    if isinstance(cell, str) and "Period =" in cell:
                        match = re.search(r"Period = (\w+ \d{4})", cell)
                        if match:
                            month, year = match.group(1).split()
                            month_numeric = generic.get_month_number(month)
                            if month_numeric:
                                params=[file_path.split('/')[-1], month_numeric, int(year)]
                                OUTPUT = dbOperations.execute_stored_procedure('dbsp_InsertTBFileMonthYearLink', params, False)
                                logging.debug(f"Inserted: {file_path}, Numeric Date: {(int(year), month_numeric)}")

                            break;

            read_excel_and_extract_data(event.src_path)

            fileprocess.process_file(event.src_path)



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

