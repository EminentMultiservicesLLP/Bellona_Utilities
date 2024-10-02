from flask import Flask, request, jsonify
import os
import fileprocess, config
from logging_config import setup_logging

logging = setup_logging()
app = Flask(__name__)
WATCH_DIRECTORY = config.get_env_variable('WATCH_DIRECTORY', "")

@app.route('/api/home/processTBFile', methods=['POST'])
def start_FileProcessing():
    try:
        # Try to get data from JSON body (POST requests typically)
        json_data = request.get_json(silent=True)
        # Try to get data from query string (GET requests)
        query_params = request.args

        # If no data is passed in JSON, fall back to query params
        file_path = json_data.get('file_path') if json_data else query_params.get('file_path')
        
        # Check if the file exists
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        if fileprocess.StartFileProcessing(file_path):
            return jsonify({"status": "File processed successfully"}), 200
        return jsonify({"status": "File processing failed, please see the error(s)"}), 422
    except Exception as e:
        return jsonify({"status": "File processing failed, Insternal server error. Ask Administrator to look into issue"}), 423
@app.route('/api/home/getErrorList', methods=['GET'])
def GetErrorList():
    return jsonify({"status": "watcher stopped"}), 200

if __name__ == '__main__':
    app.run(port=8090)
