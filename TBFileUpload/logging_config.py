import logging
import logging.config
import traceback
import os
from dotenv import load_dotenv

load_dotenv() 

class CustomLogger(logging.Logger):

    def error(self, msg, *args, **kwargs):
        # Capture the stack trace
        stack_trace = traceback.format_exc()
        
        # Extract the method name from the stack trace
        method_name = None
        if stack_trace != 'NoneType: None\n':
            method_name = traceback.extract_stack()[-3].name
        
        # If stack trace is available, append it to the message
        if stack_trace and method_name:
            msg = f"{msg}\nOccurred in method '{method_name}'\nStack trace:\n{stack_trace}"
        
        # Call the original error method with the modified message
        super().error(msg, *args, **kwargs)

def setup_logging():
    log_level = os.getenv("logLevel", "DEBUG").upper()
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.FileHandler',
                'filename': 'app.log',
                'formatter': 'default',
                'level': log_level,
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'level': log_level,
            },
        },
        'loggers': {
            'my_logger': {
                'handlers': ['file', 'console'],
                'level': log_level,
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['console'],  # Ensure root only logs to console
            'level': log_level,  # Root logger level set via environment variable
        },
    }

    # Register the custom logger
    logging.setLoggerClass(CustomLogger)

    logging.config.dictConfig(logging_config)
    logger = logging.getLogger('my_logger')
    return logger

