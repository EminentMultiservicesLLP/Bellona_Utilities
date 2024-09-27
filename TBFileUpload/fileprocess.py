import os
import pandas as pd
import logging
from dotenv import load_dotenv
import pyodbc
from logging_config import setup_logging
from datetime import datetime
import helper, dbOperations, generic

# Load environment variables from .env file
load_dotenv()

logging = setup_logging()

HEADER_ROW = int(os.getenv('HEADER_ROW')) - 1  # Convert to 0-based index
DATA_START_ROW = int(os.getenv('DATA_START_ROW')) - 1  # Convert to 0-based index

expected_columns = {'Code', 'Particulars', 'Nature', 'MIS Head'}
start_branchcolumn_idex =(int(os.getenv("BRANCH_COLUMN_START"))) if os.getenv("BRANCH_COLUMN_START", None).isdigit() else (helper.column_letter_to_index(os.getenv("BRANCH_COLUMN_START", "E"))) 
end_branchcolumn_index = (int(os.getenv("BARNCH_COLUMN_END"))) if os.getenv("BARNCH_COLUMN_END", None).isdigit() else (helper.column_letter_to_index(os.getenv("BARNCH_COLUMN_END", "AB")))



#validate mandatory fields available/missing in excel
def Validate_MandatoryFields(columns,fileId):
    # Validate column names
    actual_columns = set(columns)
    
    # Check for missing or extra columns
    if not expected_columns.issubset(actual_columns):
        missing_cols = expected_columns - actual_columns
        #extra_cols = actual_columns - expected_columns
        if missing_cols:
            logging.error(f"Missing columns: {', '.join(missing_cols)}")
            generic.log_error_to_db(f"Missing columns: {', '.join(missing_cols)}", HEADER_ROW, 0, fileId)
        return

def Validate_Row(code, particulars, nature, mis_head, index, row, columns, fileId):
    try:
        output = True
        logging.info("started new row reading")
        # Check if 'Code' is valid
        if not isinstance(code, str):
            output = False
            logging.error(f"Invalid Code at row {index+1} and column 0")
            generic.log_error_to_db(f"Invalid Code at row {index+1} and column 0", index+1, 0, fileId)
        
        # Check if 'Particulars' is valid
        if not isinstance(particulars, str) or not particulars.strip():
            output = False
            logging.error(f"Invalid Particulars at row {index+1} and column 0")
            generic.log_error_to_db(f"Invalid Particulars at row {index+1} and column 0", index+1, 0,fileId)

        # Check if 'MIS HEAD' is valid
        if not isinstance(mis_head, str) or not mis_head.strip():
            output = False
            logging.error(f"Invalid MIS head at row {index+1} and column 0")
            generic.log_error_to_db(f"Invalid MIS head at row {index+1} and column 0", index+1, 0,fileId)

        #Check if numeric columns contain valid numbers
        numeric_cols = [col for col in columns if col not in expected_columns]
        for col in numeric_cols:
            try:
                col_idx = columns.get_loc(col) 
                # Convert to float to validate
                float(row[col])
            except ValueError:
                output = False
                generic.log_error_to_db(f"Invalid value (expected Numeric value) in column '{col}' at row {index+1} and column {col_idx}", DATA_START_ROW+ (index-1), col_idx,fileId)
                logging.error(f"Invalid value (expected Numeric value) in column '{col}' at row {index+1} and column {col_idx}")
            
        return output
    except ValueError as ve:
        logging.error(str(ve))
        generic.log_error_to_db( str(ve), DATA_START_ROW+ (index-1), None,fileId)
        return False
    except Exception as e:
        logging.error(str(e))
        generic.log_error_to_db( str(e), DATA_START_ROW+ (index-1), None,fileId)
        return False

def ExcelDataChecks(df, code_col_index, particulars_col_index, nature_col_index, misheads_col_index,fileId):
    validation_Succeed = True
    logging.info("Validation of Data Started")
    for index, row in df.iterrows():
        logging.info("started new row reading")

        code = row[code_col_index]  # Code data from column A
        particulars = row[particulars_col_index]  # Particulars data from column B
        nature = row[nature_col_index]  # Nature data from column D
        mis_head = row[misheads_col_index]  # MIS head data from column D

        logging.info(f"Data at row {index+HEADER_ROW} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

        #break the loop as soon as we hit the total row
        if isinstance(particulars, str) and (particulars.strip().lower() == "total"): 
            break

        # as per finance team they need to use first letter of each word of partuvlar columns, if code is blank
        if code is None or not isinstance(code, str):
            code = ''.join([word[0] for word in particulars.split()])

        #validate basis data expected for fields
        if validation_Succeed: 
            validation_Succeed = Validate_Row(code, particulars, nature, mis_head, index+HEADER_ROW, row, df.columns, fileId)
        else:
            Validate_Row(code, particulars, nature, mis_head, index+HEADER_ROW, row, df.columns, fileId)
        logging.debug(f"Data Validation at row index {index+HEADER_ROW} completed")

    logging.info("Validation of Data Completed")
    return validation_Succeed



# Process the Excel file and insert data into the database
def process_file(file_path, fileId):
    cursor = None
    try:
        data_failed_anytime = False
        # Read Excel file starting from the configured header row and actual data row
        with pd.ExcelFile(file_path, engine='openpyxl') as xls:
            df = pd.read_excel(xls, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))
            #df = pd.read_excel(file_path, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))
        
            code_col_index = (int(os.getenv("col_code", 1)) - 1) if os.getenv("col_code", None).isdigit() else 0
            particulars_col_index = (int(os.getenv("col_particulars", 2)) - 1) if os.getenv("col_particulars", None).isdigit() else 1
            nature_col_index = (int(os.getenv("col_nature", 3)) - 1) if os.getenv("col_nature", None).isdigit() else 2
            misheads_col_index = (int(os.getenv("col_mis_heads", 4)) - 1) if os.getenv("col_mis_heads", None).isdigit() else 3

            with pyodbc.connect(dbOperations.connect_db()) as conn:
                # Create a cursor
                cursor = conn.cursor()

                Validate_MandatoryFields(df.columns, fileId)

                if(ExcelDataChecks(df, code_col_index, particulars_col_index, nature_col_index, misheads_col_index, fileId)):
                    for index, row in df.iterrows():
                        try:
                            logging.info("started new row reading")

                            code = row[code_col_index]  # Code data from column A
                            particulars = row[particulars_col_index]  # Particulars data from column B
                            nature = row[nature_col_index]  # Nature data from column D
                            mis_head = row[misheads_col_index]  # MIS head data from column D

                            logging.info(f"Data at row {index+HEADER_ROW} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

                            #break the loop as soon as we hit the total row
                            if (particulars.strip().lower() == "total"): 
                                logging.debug(f"Reached at Total row at index {index+HEADER_ROW}, ending file processing.")
                                conn.commit()
                                break

                            # as per finance team they need to use first letter of each word of partuvlar columns, if code is blank
                            if code is None or not isinstance(code, str):
                                code = ''.join([word[0] for word in particulars.split()])

                            # Insert or get MIS_head ID
                            #mis_head_id = dbOperations.insert_or_get_id(cursor, "TB_MISHead", "head_name", mis_head, {'head_name': mis_head, 'nature': nature}, index)
                            mis_head_id = dbOperations.getData_scalar(cursor, "TB_MISHead", "head_name", mis_head)
                            logging.debug(f"Received MIS Head ID {mis_head}")

                            # Insert or get Particulars ID
                            #particulars_id =  dbOperations.insert_or_get_id(cursor, "TB_Particulars", "code", code, {'code': code, 'particulars': particulars}, index)
                            particulars_id =  dbOperations.getData_scalar(cursor, "TB_Particulars", "code", code)
                            logging.debug(f"Received Particulars ID {particulars_id}")

                            if mis_head_id and particulars_id:
                                # For each branch column start position
                                for branch_col in df.columns[start_branchcolumn_idex:]:
                                    
                                    col_idx = df.columns.get_loc(branch_col) 
                                    branch_id = branch_col  # Assuming branch name or id is in the column header
                                    tb_amount = row[branch_col]
                                    #tb_date = pd.Timestamp.today().date()  # Or adjust as per file
                                    
                                    # Insert into TB_TrialBalance
                                    try:
                                        cursor.execute("""
                                            INSERT INTO TB_TrialBalance (fileId, branch_id, head_id, particulars_id, tb_amount)
                                            VALUES (?, ?, ?, ?, ?)
                                        """, ( fileId, branch_id, mis_head_id, particulars_id, tb_amount))
                                        logging.info(f"Data inserted for Branch {branch_id}, MIS Head {mis_head_id}, TB Particulars {particulars_id}.")
                                    except pyodbc.Error as e:
                                        logging.error(f"Error inserting trial balance for row index {index}, column index {col_idx}: {e}")
                                        generic.log_error_to_db(f"Error inserting trial balance at row ({index}) and column ({col_idx}): {e}", DATA_START_ROW+ (index-1), start_branchcolumn_idex + col_idx,fileId)

                                logging.info(f"Commiting single row Data into DB for MIS Head {mis_head_id}, TB Particulars {particulars_id}.")
                                conn.commit()
                                                
                        except ValueError as v:
                            logging.error(f"Value error: {v}")
                        except Exception as e:
                            logging.error(f"Error processing file {file_path}: {e}")        
        
        del df
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")






