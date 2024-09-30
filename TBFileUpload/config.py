from dotenv import load_dotenv
import os

# Load the .env file once
load_dotenv("D:/Projects/Bellona/BellonaUtilities/Bellona/TBFileUpload/.env")  # Replace with the actual path to your .env file

# Now you can define functions to get environment variables
def get_env_variable(name, default=None):
    """Get an environment variable or return a default value."""
    return os.getenv(name, default)

def get_env_variable_inDigit(name, default=None):
    """Get an environment variable or return a default value."""
    return os.getenv(name, default if isinstance(default, int) else 0)