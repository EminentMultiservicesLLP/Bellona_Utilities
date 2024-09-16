import pyodbc
import os
from dotenv import load_dotenv, dotenv_values 
from Logging_config import setup_logging
from RistaCalls.GenericClass import executeType as executeType

load_dotenv() 
# Initialize the logger
logger = setup_logging()

class DatabaseSingleton:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        # Check if an instance already exists
        if not cls._instance:
            cls._instance = super(DatabaseSingleton, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialize_connection()
        else:
            # Check if the existing connection is closed
            if not cls._instance.connection or cls._instance.connection.closed:
                logger.info("Existing connection is closed. Creating a new instance.")
                cls._instance = super(DatabaseSingleton, cls).__new__(cls, *args, **kwargs)
                cls._instance._initialize_connection()
        return cls._instance
    
    def _initialize_connection(self):
        if os.getenv("db_environment", "").lower() in ["prod", "production", "live"]:
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("db_server")};;DATABASE={os.getenv("database")};;UID={os.getenv("UID")};;PWD={os.getenv("PWD")};;'
        else:
            connection_string=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("db_server")};;DATABASE={os.getenv("database")};;trusted_connection=yes'
        self.connection=pyodbc.connect(connection_string)
        logger.warning(f"Connection established and object created")
        self.cursor = self.connection.cursor()
        logger.warning(f"Cursor object created")

    def get_connection(self):
        return self.connection
    
    def get_cursor(self):
        if not self.connection or self.connection.closed:
            logger.warning("Database Connection was found to be closed, initializing new")
            self._initialize_connection()

        return self.cursor
    
    def close_connection(self):
         if self.connection:
            self.cursor.close()
            self.connection.close()
            logger.info("Database Connection closed")
            self.__class__._instance = None  # Reset instance to allow creating a new one if needed.
