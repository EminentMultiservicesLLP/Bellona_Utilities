import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import numpy as np
import io, os
import dBCalls
from logging_config import setup_logging
logging = setup_logging()



# Path to your Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = os.getenv("Tesseract_OCR_Executable")

def extract_text_from_pdf(pdf_path):
    logging.info("Method extract_text_from_pdf started")
    try:
        extracted_text = ""
        with fitz.open(stream=pdf_path.read(), filetype='pdf') as document:
            logging.info(f"File open successfully in memory, total pages in file are {len(document)}")
            for page_number in range(len(document)):
                logging.info(f"Started processing page:{page_number}")
                page = document.load_page(page_number)
                
                # Render page to an image
                pix = page.get_pixmap()
                logging.info("Render page to an image")

                # Convert image to bytes and then open with PIL
                img_data = pix.pil_tobytes(format='png')  # Ensure the format is specified
                logging.info("Converted image to bytes and then open with PIL")

                try:
                    img = Image.open(io.BytesIO(img_data))
                    img = img.convert('RGB')  # Ensure image is in RGB format for OCR
                except Exception as e:
                    logging.error(f"Error processing page {page_number + 1}: {e}")
                    continue

                # Perform OCR on the image
                try:
                    text = pytesseract.image_to_string(img)
                    extracted_text += f"Page {page_number + 1}:\n{text}\n"
                except Exception as e:
                    logging.error(f"Error during OCR on page {page_number + 1}: {e}")

            
            # output = extract_text_based_on_rules(extracted_text);
            # logging.info(f"Text extracted from PDF file as: {output}")
        return extracted_text;
    except Exception as e:
        logging.error(f"Error while extracting text from PDF file :{pdf_path}, error:{e}")
    finally:
        logging.info("Method extract_text_from_pdf completed")

# This function performs different levels of cleaning based on the provided level parameter. 
# It removes spaces before colons, colons themselves, or all spaces.
def clean_search_text(text, level=1):
    """
    Cleans the search text based on the specified level.
    - Level 1: Remove space before ':'
    - Level 2: Remove ':'
    - Level 3: Remove all spaces
    """
    if level == 1:
        return text.replace(" :", ":")
    elif level == 2:
        return text.replace(":", "")
    elif level == 3:
        return text.replace(" ", "")
    return text

#This recursive function attempts to find the start text in the given text, applying different levels of cleaning if necessary.
def find_text_recursive(text, search_text, searchStartFromPos,textSearchStartFromTop=False, level=1):
    try:
        """
        Recursively attempts to find the search_text in the given text,
        applying different cleaning levels if necessary.
        """
        cleaned_search_text = search_text
        start_index = text.find(cleaned_search_text, (searchStartFromPos if textSearchStartFromTop == False else 0))
        if start_index == -1:
            # If the search text is not found and we haven't exhausted all levels
            if level < 3:
                cleaned_search_text = clean_search_text(search_text, level)
                # Recursive call to try the next cleaning level
                return find_text_recursive(text, cleaned_search_text, searchStartFromPos, textSearchStartFromTop, level + 1)
            else:
                # If no level finds the text, return -1
                return -1
        else:
            # Return the start index adjusted for the length of the search text
            return start_index + len(cleaned_search_text)
            #return last_position
    except Exception as e:
        logging.error(f"Error occured in recursive function attempts, error:{e}")

def extract_between(text, start, end, line_limit, start_search_from_top, read_data_on_same_line, last_position):
    #global last_position
    try:
        # Find start and end indices
        start_index = last_position if start.strip() == "" else find_text_recursive(text, start, last_position, start_search_from_top)
        if start_index == -1:
            result = ''
        else:
            end_index =  -1 if end.strip() == "" else find_text_recursive(text, end, start_index, start_search_from_top)
            if end_index == -1:
                end_index = len(text)  # Set to end of text if end text is not found

            # Extract the text between start and end indices
            extracted = text[start_index:end_index-len(end)].strip()

            if read_data_on_same_line:
                lines = extracted.split('\n')
                result = lines[0].strip()
            else:
                #lines = text.split('\n')
                #line_start = text[:text.find(extracted)].count('\n') + 1

                lines = extracted.split('\n')
                result_lines = lines[:line_limit]
                result = '\n'.join(result_lines).strip()

            last_position = start_index
        return result, last_position
    except Exception as e:
        logging.error(f"Error occured while extracting text between {start} and {end}, error:{e}")

def extract_text_based_on_rules(pdf_text, documentId):
    try:
        # Parameters expected by the stored procedure
        stored_procedure = os.getenv("sql_property")
        columns, extraction_rules = dBCalls.execute_stored_procedure(stored_procedure, None)

        sorted_rules = sorted(extraction_rules, key=lambda r: r['SearchSequence'])

        last_position = 0
        extracted_text = []
        logging.info(f"Extracted text from PDF :{pdf_text}")
        for rule in sorted_rules:
            logging.info(f"Processing for propertyName : {rule['PropertyName']}, last_position:{last_position}")
            extracted, last_position = extract_between(
                pdf_text,
                rule['PropertyStart'],
                rule.get("PropertyEnd",""),
                rule.get("LineLimit","1"),
                rule.get("SearchFromStart","0") == 1,
                rule.get("ReadDataOnSameLine", "1"),
                last_position
            )
            last_position = last_position + len(extracted)
            extracted_text.append(f"{rule['PropertyName']}: {extracted}")

            #insert value in DB against property
            params =[documentId, rule['PropertyId'], extracted]
            dBCalls.execute_stored_procedure(os.getenv('sql_insertPropertyContent'), params, False)

            logging.info(f"Extracted value for propertyName {rule['PropertyName']}: {extracted} , last_position:{last_position}")
    except Exception as e:
        logging.error("Error occured while extracting text as per rules provided, error:{error}")

    return "\n".join(extracted_text)