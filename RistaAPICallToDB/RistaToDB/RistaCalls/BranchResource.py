import os
from dotenv import load_dotenv
import RistaCalls.RistaAPICalls as API
from RistaToDB import DBOperations as DBCall
from RistaCalls.GenericClass import QueryType as QueryType
from RistaCalls.GenericClass import executeType as executeType
from Logging_config import setup_logging

load_dotenv()
# Initialize the logger
logger = setup_logging()

def GetBranchResource(params=None):
    try:
        logger.debug(f"call received for GetBranchResource with params : {params}")
        data_resources = API.GetData_RistaAPI(os.getenv("branch_resource_url"), params)
        logger.info(f"Sales Page data for day received url:{os.getenv("branch_resource_url")}, params : {params}, total {len(data_resources)} entries found.")

        if data_resources != None and len(data_resources) > 0:
            logger.debug(f"Started call to insert_SaleSourceinfo method")
            branchresource_data = {
                'branchcode':'',
                'resourceid':'',
                'name':'',
                'capacity':0,
                'resourcegroupname':''
            }

            final_list=[]
            for resource in data_resources:
                for attr in resource:
                    attr_lower = attr.lower()
                    if attr_lower in branchresource_data.keys():
                        branchresource_data[attr_lower] = resource[attr]

                final_list.extend(branchresource_data)
            
            table_name = os.getenv('rista_branchresource_tbl')
            DBCall.executeNonQuery(f"DELETE from {table_name}") #delete all existing entries first

            columns = ', '.join(branchresource_data.keys())
            placeholders = ', '.join('?' * len(branchresource_data.keys()))  # Use ? for each value to prevent SQL injection
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            DBCall.executeMany(query, final_list)

            logger.debug(f"Completed call to GetBranchResource method")
    except Exception as e:
        logger.error(f"Failed for processing Branch Resource data, error {e}")
