import jwt
import time
import uuid
import os
from dotenv import load_dotenv, dotenv_values 
from datetime import datetime
from Logging_config import setup_logging

# Initialize the logger
logger = setup_logging()

load_dotenv() 

def GenerateToken():
    # Define your secret key and API key
    secret_key = os.getenv("secret_key")
    api_key = os.getenv("api_key")

    # Create the key and credentials
    key = secret_key.encode('utf-8')
    creds = jwt.algorithms.HMACAlgorithm(jwt.algorithms.get_default_algorithms()['HS256']).prepare_key(key)

    current_ticks = int(time.time() +(2 * 3600))
    expiration = current_ticks + (3 * 3600)

    # Define claims
    claims = {
        'iss': api_key,
        'iat': (int(time.time())),  # Current time as a Unix timestamp
        'exp': int(time.time()) + (2*3600),
        'jti': str(uuid.uuid4())  # Generate a unique jti using UUID
    }

    # Create the JWT token
    token = jwt.encode(claims, creds, algorithm='HS256')

    # The token is now a string
    token_str = token.decode('utf-8') if isinstance(token, bytes) else token

    # Return the token
    return token_str

def is_token_valid(token):
    try:
        logger.info("Started is_Token_valid function")
        secret_key = os.getenv("secret_key")
        # Decode the token with the same secret key and algorithm
        decoded_token = jwt.decode(token, secret_key, algorithms=['HS256'])
        return True
    except jwt.ExpiredSignatureError:
        logger.error("Token has expired")
        return False
    except jwt.InvalidTokenError:
        logger.error("Token is invalid for other reasons (e.g., tampering)")
        return False

existing_token = None
def GetToken():
    global existing_token
    if existing_token and is_token_valid(existing_token):
        logger.info(f"Token is not expired yet: {existing_token}")
    else:
        new_token = GenerateToken()
        logger.info(f"Generated new token: {new_token}")
        existing_token = new_token

    return existing_token

