from flask import Flask, request, jsonify
import os, json
import fileprocess, config
from logging_config import setup_logging

logging = setup_logging()
app = Flask(__name__)
WATCH_DIRECTORY = config.get_env_variable('WATCH_DIRECTORY', "")

@app.route('/api/home/processTBFile', methods=['POST'])
def start_FileProcessing():
    try:
        file_path = ReadRequestDetails(request)    

        # Check if the file exists
        if not os.path.exists(file_path):
            logging.debug("Error: File not found, error send with 404 status code")
            return jsonify({"ResponsePhrase": "Failed", "value":"File not found"}), 404

        status, uploadedMonth, uploadedYear = fileprocess.StartFileProcessing(file_path)
        if status:
            logging.debug(f"File processed successfully for Month {uploadedMonth} and Year {uploadedYear}")
            return jsonify({"ResponsePhrase": "Success", "value":f"File processed successfully for Month {uploadedMonth} and Year {uploadedYear}"}), 200
        
        logging.debug("Error: File processing failed, please see the error(s)")
        return jsonify({"ResponsePhrase": "Failed", "value":"File processing failed, please see the error(s)"}), 422
    except Exception as e:
        logging.debug(f"Error: File processing failed, Internal server error. Ask Administrator to look into issue. Error:{e}")
        return jsonify({"ResponsePhrase": "Failed", "value":"File processing failed, Internal server error. Ask Administrator to look into issue"}), 500
		
@app.route('/api/home/checkTBAlreadyUploaded', methods=['GET'])
def CheckTBAlreadyExist():
    try:
        file_path = ReadRequestDetails(request)
        # Check if the file exists
        if not os.path.exists(file_path):
            logging.debug("Error: File not found, error send with 404 status code")
            return jsonify({"ResponsePhrase": "Failed", "value":"File not found"}), 404

        status = fileprocess.checkIfTBAlreadyExists(file_path)
 
        if status == 0:
            logging.debug("No record exist and good for processing")
            return jsonify({"ResponsePhrase": "Success", "value":"No record exist and good for processing", "recordCount":"0"}), 200
        elif status == -1:
            logging.debug("Error - period details not available in uploaded file, looks like wrong file or someone removed that data.")
            return jsonify({"ResponsePhrase": "Failed", "value":"period details not available in uploaded file, looks like wrong file or someone removed that data. Please upload correct file."}), 422
        elif status >0:
            return jsonify({"ResponsePhrase": "Success", "value":"Record already exist for uploaded month year combination.", "recordCount":{status}}), 200
    except Exception as e:
        logging.debug(f"Error: File processing failed, Internal server error. Ask Administrator to look into issue. Error:{e}")
        return jsonify({"ResponsePhrase": "Failed", "value":"File processing failed, Internal server error. Ask Administrator to look into issue"}), 500


def ReadRequestDetails(request):
    logging.debug("Request received")
    # Try to get data from JSON body (POST requests typically)
    request_data = request.get_json(silent=True)
    logging.debug(f"json_data: {request_data}")

    # Try to get data from query string (GET requests)
    query_params = request.args
    logging.debug(f"query_params: {query_params}")

    json_data = json.loads(request_data.replace("\\", "\\\\"))
    # If no data is passed in JSON, fall back to query params
    file_path = json_data.get('file_path') if json_data else query_params.get('file_path')
    logging.debug(f"File location shared:{file_path}")

    return file_path

if __name__ == '__main__':
    app.run(port=8090)
