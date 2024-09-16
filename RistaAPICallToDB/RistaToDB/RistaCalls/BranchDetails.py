import os
import RistaCalls.RistaAPICalls as API
from Logging_config import setup_logging

# Initialize the logger
logger = setup_logging()

def GetBranchDetails():
    try:
        logger.info("       Call received for GetBranchDetails ")
        output = API.GetData_RistaAPI(os.getenv("branch_list_url"))
    except Exception as e:
        logger.error(f"      Failed in GetBranchDetails method, {e}")
    
    return output

