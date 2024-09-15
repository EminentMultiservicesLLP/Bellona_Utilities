from flask import Flask, request, jsonify
import os
from datetime import datetime
import dBCalls
from ExtractContent_OCR import extract_text_from_pdf, extract_text_based_on_rules 
from logging_config import setup_logging

logging = setup_logging()
app = Flask(__name__)

# Directory where the files will be saved
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Create the folder if it doesn't exist

@app.route('/api/home/ProcessScannedDocument', methods=['POST'])
def upload_file():
    logging.info("API call received ")
    try:
        #capture client details
        client_ip = request.remote_addr
        user_agent = request.headers.get('User-Agent')
        client_id = request.headers.get('X-Client-ID')
        if client_id:
            print(f"Client ID: {client_id}")
        logging.info(f"Client request from IP:{client_ip}, user agent:{user_agent} and Client Id:{client_id if client_id else ''}")

        if 'pdffile' not in request.files:
            logging.error("error:No file part, Unknown call to API, required attribute missing from client call")
            return jsonify({"error": "No file part"}), 400

        file = request.files['pdffile']
        if file.filename == '':
            logging.error("error:No selected file, no file data shared")
            return jsonify({"error": "No selected file"}), 400

        # if file and file.filename.lower().endswith('.pdf'):
        #     pdf_file = io.BytesIO(file.read())
        #     pdf_text = extract_text_from_pdf(pdf_file)
        #     result = extract_text_based_on_rules(pdf_text)
        #     return jsonify({"extracted_text": result})

        if file and file.filename.lower().endswith('.pdf'):
            # Create a timestamp in the format ddMMMyyyyHHMMSS
            timestamp = datetime.now().strftime('%d%b%Y%H%M%S') + f"{datetime.now().microsecond // 1000:03d}"
            
            # Get the original file name without the extension
            original_filename = os.path.splitext(file.filename)[0]
            
            # Create a new filename with the timestamp
            new_filename = f"{original_filename}_{timestamp}.pdf"
            
            logging.info(f"File will be saved at with new name: {new_filename} ")
            # Save the file to the specified directory with the new filename
            save_path = os.path.join(UPLOAD_FOLDER, new_filename)
            file.save(save_path)
            logging.info(f"File saved at {save_path} ")

            #insert document details into database
            params = [0, new_filename, save_path, 0,'',0]
            documentId = dBCalls.InsertDocumentdetails_DB(params)

            # Open the saved file for processing
            with open(save_path, 'rb') as pdf_file:
                pdf_text = extract_text_from_pdf(pdf_file)

                #Insert scanned text into database
                params=[documentId, pdf_text, 0]
                dBCalls.execute_stored_procedure(os.getenv("sql_insertDocumentContent"),params, False)

                result = extract_text_based_on_rules(pdf_text, documentId)

            return jsonify({"extracted_text": result})

        logging.error("Invalid file type received")
        return jsonify({"error": "Invalid file type"}), 400
    except Exception as e:
        logging.error("error: something went wrong {e}")



if __name__ == '__main__':
    app.run()