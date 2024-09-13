import os
import time
import logging
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fileprocess as fileprocess
from logging_config import setup_logging

# Load environment variables from .env file
load_dotenv()

logging = setup_logging()

WATCH_DIRECTORY = os.getenv('WATCH_DIRECTORY')

# File Watcher Event Handler
class WatcherHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return None
        elif event.src_path.endswith(".xlsx"):
            logging.info(f"New file detected: {event.src_path}")
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
