import requests
import os
from dotenv import load_dotenv
import RistaCalls.JWTToken as JWTToken
from Logging_config import setup_logging

# Initialize the logger
logger = setup_logging()

load_dotenv() 
def fetch_data(api_url, params=None):
    logger.info("Call receive for fetch_data")
    max_retries = 3
    for attempt in range(max_retries):
        logger.info(f" Attempt {attempt} to call fetch_data")
        try:
            token = JWTToken.GetToken()
            logger.info(f" Token generated - {token} ")
            api_key = os.getenv("api_key")

            headers = {
                'x-api-key': api_key,
                'x-api-token': token
            }
            logger.info(" Making requests.get call to API")
            if params == None :
                response = requests.get(api_url, headers=headers)
            else:
                response = requests.get(api_url, params=params, headers=headers)
            logger.info(" requests.get Call to API completed")

            if response.status_code == 401:
                logger.error("Unauthorized error: Invalid API token or credentials.")
                raise requests.exceptions.HTTPError("Unauthorized error", response=response)
            
            if response.status_code == 403:
                logger.error("Forbidden error: Token might be expired.")
                raise requests.exceptions.HTTPError("Forbidden error", response=response)
            
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            if response.status_code in [401, 403]:
                logger.error(f"Authentication error encountered: {e}. Attempting to refresh the token.")
                if attempt < max_retries - 1:
                    logger.error("Retrying with a new token...")
                    JWTToken.existing_token = None #this will force to generate new token
                else:
                    logger.error("Max retries exceeded for authentication errors.")
                    raise
            else:
                logger.error(f"HTTPError on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logger.error("Retrying...")
                else:
                    logger.error("Max retries exceeded.")
                    raise
        
        except Exception as e:
            logger.error(f"Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                logger.error("Retrying...")
            else:
                logger.error("Max retries exceeded.")
                raise
    logger.info("Call completed for fetch_data")
    return response.json()

def GetData_RistaAPI(apiUrl,params=None):
    logger.info("Call receive for GetData_RistaAPI")
    all_records = []  # Initialize apiData as a list to accumulate all fetched data
    try:
        logger.info(f"Calling fetch_data method for api url : {apiUrl}, params:{params}")
        response = fetch_data(apiUrl, params)
        logger.info(f"Successfull call to fetch_data method for api url : {apiUrl}, params:{params}")
        if 'data' in response:
            all_records.extend(response.get('data', []))
        else:
            all_records.extend(response)
    except Exception as e:
        logger.error(f"failed for apiurl : {apiUrl}, params:{params} ; Error: {e}")

    
    # Check if there's more data to fetch (indicated by 'lastKey') and 'lastKey' in response, fetch more data; otherwise, return the accumulated data.
    if len(all_records) > 0 and 'lastKey' in response:
        params['lastKey'] = response['lastKey']
        logger.info(f"Making call again because last key found in response from api url : {apiUrl}, params:{params}, lastKey: {response['lastKey']}")
        next_data = GetData_RistaAPI(apiUrl, params)
        logger.info(f"Data received from iterative API Call");
        all_records.extend(next_data)  # Append next_data to apiData
        logger.info(f"Data received from iterative API Call, appended to final data object");

    logger.info("Call completed for GetData_RistaAPI")
    return all_records