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
        fileId, file_path, uploadedMonth,uploadedYear = ReadRequestDetails(request)
        if not fileId is None and fileId > 0:
            file_path,uploadedMonth,uploadedYear = fileprocess.GetFileDetailsByFileId(fileId)
            if not os.path.exists(file_path):
                logging.debug("Error: File not found, error send with 404 status code")
                return jsonify({"ResponsePhrase": "Failed", "value":"File not found"}), 404

        else:
             # Check if the file exists
            if not os.path.exists(file_path):
                logging.debug("Error: File not found, error send with 404 status code")
                return jsonify({"ResponsePhrase": "Failed", "value":"File not found"}), 404
            
            status = fileprocess.StartFileProcessing(file_path, uploadedMonth, uploadedYear)
            
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
        file_path, tbMonth,tbYear = ReadRequestDetails(request)
        # Check if the file exists
        if not os.path.exists(file_path):
            logging.debug("Error: File not found, error send with 404 status code")
            return jsonify({"ResponsePhrase": "Failed", "value":"File not found"}), 404

        status = fileprocess.checkIfTBAlreadyExists(file_path, tbMonth, tbYear)
 
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
    fileId = 0
    file_path=''
    tbMonth=0
    tbYear=0
    logging.debug("Request received")
    # Try to get data from JSON body (POST requests typically)
    json_data = request.get_json(silent=True)
    logging.debug(f"json_data: {json_data}")

    # Try to get data from query string (GET requests)
    query_params = request.args
    logging.debug(f"query_params: {query_params}")

    lowercase_JSON_data = {key.lower(): value for key, value in json_data.items()}
    logging.debug(f"lowercase_JSON_data: {lowercase_JSON_data}")

    if any(key == "fileid" for key in lowercase_JSON_data):
        fileId = lowercase_JSON_data.get("fileid")
        logging.debug(f"fileId: {fileId}")
    if any(key == "file_path" for key in lowercase_JSON_data):
        file_path = lowercase_JSON_data.get("file_path")
        logging.debug(f"file_path: {file_path}")
    
    if any(key == "tbmonth" for key in lowercase_JSON_data):
        tbMonth = lowercase_JSON_data.get("tbmonth")
        logging.debug(f"tbmonth: {tbMonth}")
    
    if any(key.lower() == "tbyear" for key in lowercase_JSON_data):
        tbYear = lowercase_JSON_data.get("tbyear")
        logging.debug(f"tbyear: {tbYear}")

    return fileId, file_path, tbMonth, tbYear

if __name__ == '__main__':
    app.run(port=8090)
