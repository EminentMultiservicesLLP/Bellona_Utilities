import pandas as pd
import logging
from datetime import datetime
import time
import os, re
import pyodbc
from logging_config import setup_logging
import helper, dbOperations, generic, config

logging = setup_logging()

HEADER_ROW = int(config.get_env_variable('HEADER_ROW', 1)) - 1  # Convert to 0-based index
DATA_START_ROW = int(config.get_env_variable('DATA_START_ROW', 1)) - 1  # Convert to 0-based index
ALLOWED_EXTENSIONS = ["." + ext if not ext.startswith(".") else ext for ext in config.get_env_variable('file_extentions_allowed',"").split(",")]
FILE_ARCHIVALPATH = config.get_env_variable('file_archivalPath', "")
expected_columns = {'Code', 'Particulars', 'Nature', 'MIS Head'}

start_branchcolumn_idex =(int(config.get_env_variable_inDigit("BRANCH_COLUMN_START"))) if helper.try_parse(config.get_env_variable("BRANCH_COLUMN_START"), int) else (helper.column_letter_to_index(config.get_env_variable("BRANCH_COLUMN_START", "E"))) 
end_branchcolumn_index = (int(config.get_env_variable_inDigit("BARNCH_COLUMN_END"))) if helper.try_parse(config.get_env_variable("BARNCH_COLUMN_END"), int) else (helper.column_letter_to_index(config.get_env_variable("BARNCH_COLUMN_END", "AB")))

code_col_index = (int(config.get_env_variable_inDigit("col_code", None)) - 1)
particulars_col_index = (int(config.get_env_variable_inDigit("col_particulars", 2)) - 1)
nature_col_index = (int(config.get_env_variable_inDigit("col_nature", 3)) - 1)
misheads_col_index = (int(config.get_env_variable_inDigit("col_mis_heads", 4)) - 1)

#validate mandatory fields available/missing in excel
def Validate_MandatoryFields(columns,fileId):
    missing_cols=[]
    # Validate column names
    actual_columns = set(columns)
    
    # Check for missing or extra columns
    if not expected_columns.issubset(actual_columns):
        missing_cols = expected_columns - actual_columns
        #extra_cols = actual_columns - expected_columns
    
    if missing_cols:
        logging.error(f"Missing columns: {', '.join(missing_cols)}")
        generic.log_error_to_db(f"Required fields ({', '.join(missing_cols)}) missing in uploaded file.", HEADER_ROW, 0, fileId)
        return False
    return True

def Validate_Row(code, particulars, nature, mis_head, index, row, columns, fileId):
    error_list =[]
    try:
        logging.debug("started new row reading")
        # Check if 'Code' is valid
        if not isinstance(code, str):
            logging.error(f"Blank/Invalid data for Code field at row {index+1} and column {helper.index_to_column_letter(code_col_index)}")
            #generic.log_error_to_db(f"Blank/Invalid data for Code field at row {index+1} and column {helper.index_to_column_letter(code_col_index)}", index+1, code_col_index, helper.index_to_column_letter(code_col_index), fileId)
            error_list.append((f"Blank/Invalid data for Code field at row {index+1} and column {helper.index_to_column_letter(code_col_index)}", index+1, code_col_index, helper.index_to_column_letter(code_col_index), fileId))

        # Check if 'Particulars' is valid
        if not isinstance(particulars, str) or not particulars.strip():
            logging.error(f"Blank/Invalid data (expected text and can not be blank) for Particulars field at row {index+1} and column {helper.index_to_column_letter(particulars_col_index)}")
            #generic.log_error_to_db(f"Blank/Invalid data (expected text and can not be blank) for Particulars field at row {index+1} and column {helper.index_to_column_letter(particulars_col_index)}", index+1, particulars_col_index, helper.index_to_column_letter(particulars_col_index),fileId)
            error_list.append((f"Blank/Invalid data (expected text and can not be blank) for Particulars field at row {index+1} and column {helper.index_to_column_letter(particulars_col_index)}", index+1, particulars_col_index, helper.index_to_column_letter(particulars_col_index),fileId))

        # Check if 'MIS HEAD' is valid
        if not isinstance(mis_head, str) or not mis_head.strip():
            logging.error(f"Blank/Invalid data (expected text and can not be blank) for MIS head field at row {index+1} and column {helper.index_to_column_letter(misheads_col_index)}")
            #generic.log_error_to_db(f"Blank/Invalid data (expected text and can not be blank) for MIS head field at row {index+1} and column {helper.index_to_column_letter(misheads_col_index)}", index+1,  misheads_col_index, helper.index_to_column_letter(misheads_col_index),fileId)
            error_list.append((f"Blank/Invalid data (expected text and can not be blank) for MIS head field at row {index+1} and column {helper.index_to_column_letter(misheads_col_index)}", index+1,  misheads_col_index, helper.index_to_column_letter(misheads_col_index),fileId))

        #Check if numeric columns contain valid numbers
        numeric_cols = [col for col in columns if col not in expected_columns]
        for col in numeric_cols:
            try:
                col_idx = columns.get_loc(col) 
                # Convert to float to validate
                float(row[col])
            except ValueError:
                logging.error(f"Invalid value (expected Numeric value) in column '{helper.index_to_column_letter(col_idx)}' at row {index+1}.")
                #generic.log_error_to_db(f"Blank/Invalid (expected Numeric value) in column '{helper.index_to_column_letter(col_idx)}' at row {index+1}.", DATA_START_ROW+ (index-1), col_idx, helper.index_to_column_letter(col_idx),fileId)
                error_list.append((f"Blank/Invalid (expected Numeric value) in column '{helper.index_to_column_letter(col_idx)}' at row {index+1}.", DATA_START_ROW+ (index-1), col_idx, helper.index_to_column_letter(col_idx),fileId))
            
        return error_list
    except ValueError as ve:
        logging.error(str(ve))
        #generic.log_error_to_db( str(ve), DATA_START_ROW+ (index-1), 0, '',fileId)
        error_list.append((f"Blank/Invalid data for Code field at row {index+1} and column {helper.index_to_column_letter(code_col_index)}", index+1, code_col_index, helper.index_to_column_letter(code_col_index), fileId))
        return error_list
    except Exception as e:
        logging.error(str(e))
        #generic.log_error_to_db( str(e), DATA_START_ROW+ (index-1), 0, '',fileId)
        error_list.append(( str(e), DATA_START_ROW+ (index-1), 0, '',fileId))
        return error_list

def ExcelDataChecks(df,fileId):
    validation_Succeed = False
    error_list=[]
    logging.debug("Validation of Data Started")

    _, MISHead_Collection = dbOperations.getData_withoutFilter("TB_MISHead",True)
    _, Particulars_Collection = dbOperations.getData_withoutFilter("TB_Particulars",True)

    '''  follow below steps  -- Check if any of value is non numeric for expected Branch numeric values 
    1.  # get subset field which falls for branch numeric details
    2.  # Clean the data: strip any leading/trailing spaces
    3.  # Convert to numeric, forcing non-numeric values to NaN
    4.  # Check for any NaN values, which indicate non-numeric entries
    5.  # If you want to see the rows and columns that contain non-numeric values
    '''
    subset_df = df.iloc[:, start_branchcolumn_idex-1:end_branchcolumn_index]
    subset_df = subset_df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)
    numeric_df = subset_df.apply(pd.to_numeric, errors='coerce')
    non_numeric_mask = numeric_df.isna()
    non_numeric_cells = non_numeric_mask.stack()[non_numeric_mask.stack()].index.tolist()
    for row_idx, col_name in non_numeric_cells:
        col_idx = df.columns.get_loc(col_name) +1
        col_letter = helper.index_to_column_letter(col_idx)
        error_list.append((f"Blank/Invalid (expected Numeric value) in column '{col_letter}' at row {row_idx+DATA_START_ROW +1}.", DATA_START_ROW+ row_idx+1, col_idx, col_letter ,fileId))

    subset_df = df.iloc[:, 0:start_branchcolumn_idex-2]
    blank_or_empty_mask = subset_df.applymap(lambda val: pd.isna(val) or str(val).strip() == '')
    blank_or_empty_cells = blank_or_empty_mask.stack()[blank_or_empty_mask.stack()].index.tolist()
    for row_idx, col_name in blank_or_empty_cells:
        col_idx = df.columns.get_loc(col_name) + 1
        col_letter = helper.index_to_column_letter(col_idx)
        error_list.append((f"Blank/Invalid (expected text value) in column '{col_letter}' at row {row_idx+DATA_START_ROW +1}.", DATA_START_ROW+ row_idx+1, col_idx, col_letter ,fileId))

    for index, row in df.iterrows():
        logging.debug("started new row reading-ExcelDataChecks")

        code = row[code_col_index]  # Code data from column A
        particulars = row[particulars_col_index]  # Particulars data from column B
        nature = row[nature_col_index]  # Nature data from column D
        mis_head = row[misheads_col_index]  # MIS head data from column D

        logging.debug(f"Data at row {index+HEADER_ROW} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

        # as per finance team they need to use first letter of each word of partuvlar columns, if code is blank
        code = ''.join(word[0] for word in particulars.split()) if not isinstance(code, str) or not code else code

        #break the loop as soon as we hit the total row
        if isinstance(particulars, str) and (particulars.strip().lower() == "total" or code.strip().lower() == "total"): break
       
        #validate basis data expected for fields
        # errors = Validate_Row(code, particulars, nature, mis_head, index+HEADER_ROW, row, df.columns, fileId)
        # if errors : error_list.append(errors)

        #below three if clause to check if excel value exists in database or not and raise error accordingly
        if not(any(d.get("head_name") == mis_head for d in MISHead_Collection)):
            logging.debug(f"MIS Head {mis_head} not exist in system")
            error_list.append((f"MIS Head {mis_head} in uploade excel at row ({index+DATA_START_ROW-1}) not exist system", DATA_START_ROW+ (index-1), 4, helper.index_to_column_letter(4),fileId))

        if not(any(d.get("particulars") == particulars for d in Particulars_Collection)):
            logging.debug(f"Particular {particulars} not exist in system")
            error_list.append((f"Particulars {particulars} in uploade excel at row ({index+DATA_START_ROW-1}) not exist system", DATA_START_ROW+ (index-1), 2, helper.index_to_column_letter(2),fileId))

        if not(any(d.get("code") == code for d in Particulars_Collection)):
            logging.debug(f"Code {code} not exist in system")
            error_list.append((f"Code {code} in uploade excel at row ({index+DATA_START_ROW-1}) not exist system", DATA_START_ROW+ (index-1), 1, helper.index_to_column_letter(1),fileId))

        logging.debug(f"Data Validation at row index {index+HEADER_ROW} completed")

    if error_list:
        validation_Succeed = False
        updated_error_list = [("TB_Upload", t[0], t[1], t[2], t[3], t[4], datetime.now()) for t in error_list]
        generic.log_error_to_db_many(updated_error_list)
    else: validation_Succeed = True    
         
    logging.debug("Validation of Data Completed")
    return validation_Succeed

# Process the Excel file and insert data into the database
def process_file(file_path, fileId):
    cursor = None
    start_time = time.time()
    try:
        # Read Excel file starting from the configured header row and actual data row
        with pd.ExcelFile(file_path, engine='openpyxl') as xls:
            df = pd.read_excel(xls, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))
            #df = pd.read_excel(file_path, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))

            if Validate_MandatoryFields(df.columns, fileId):
                end_time = time.time()
                elapsed_time = end_time - start_time
                logging.warning(f"Validation took -{elapsed_time:.4f} seconds")

                start_time = time.time();
                if(ExcelDataChecks(df, fileId)) and fileId > 0:
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    logging.warning(f"ExcelDataChecks took -{elapsed_time:.4f} seconds")

                    start_time = time.time();
                    with pyodbc.connect(dbOperations.connect_db()) as conn:
                        # Create a cursor
                        cursor = conn.cursor()

                        for index, row in df.iterrows():
                            try:
                                logging.debug("started new row reading after exceldatachecks")

                                code = row[code_col_index]  # Code data from column A
                                particulars = row[particulars_col_index]  # Particulars data from column B
                                nature = row[nature_col_index]  # Nature data from column D
                                mis_head = row[misheads_col_index]  # MIS head data from column D

                                logging.debug(f"Data at row {index+HEADER_ROW} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

                                #break the loop as soon as we hit the total row
                                if (particulars.strip().lower() == "total"): 
                                    logging.debug(f"Reached at Total row at index {index+DATA_START_ROW-1}, ending file processing.")
                                    conn.commit()
                                    break

                                # as per finance team they need to use first letter of each word of partuvlar columns, if code is blank
                                if code is None or not isinstance(code, str):
                                    code = ''.join([word[0] for word in particulars.split()])

                                # Insert or get MIS_head ID
                                #mis_head_id = dbOperations.insert_or_get_id(cursor, "TB_MISHead", "head_name", mis_head, {'head_name': mis_head, 'nature': nature}, index)
                                mis_head_id = dbOperations.getData_scalar(cursor, "TB_MISHead", "head_id", "head_name", mis_head)
                                logging.debug(f"Received MIS Head ID {mis_head}")

                                # Insert or get Particulars ID
                                #particulars_id =  dbOperations.insert_or_get_id(cursor, "TB_Particulars", "code", code, {'code': code, 'particulars': particulars}, index)
                                particulars_id =  dbOperations.getData_scalar(cursor, "TB_Particulars", "id", "code", code)
                                logging.debug(f"Received Particulars ID {particulars_id}")

                                if mis_head_id and particulars_id:
                                    # For each branch column start position
                                    single_row_multiplebranch_data = []
                                    insert_sql = """
                                                INSERT INTO dbo.TB_TrialBalance (fileId, branch_id, head_id, particulars_id, tb_amount)
                                                VALUES (?, ?, ?, ?, ?)
                                                """
                                    for branch_col in df.columns[start_branchcolumn_idex-1:]:
                                        #col_idx = df.columns.get_loc(branch_col) 
                                        branch_id = branch_col  # Assuming branch name or id is in the column header
                                        tb_amount = row[branch_col]
                                        #tb_date = pd.Timestamp.today().date()  # Or adjust as per file
                                        
                                        single_row_multiplebranch_data.append(( fileId, branch_id, mis_head_id, particulars_id, tb_amount))
                                        # # Insert into TB_TrialBalance
                                        # try:
                                        #     cursor.execute("""
                                        #         INSERT INTO dbo.TB_TrialBalance (fileId, branch_id, head_id, particulars_id, tb_amount)
                                        #         VALUES (?, ?, ?, ?, ?)
                                        #     """, ( fileId, branch_id, mis_head_id, particulars_id, tb_amount))
                                        #     logging.debug(f"Data inserted for Branch {branch_id}, MIS Head {mis_head_id}, TB Particulars {particulars_id}.")
                                        # except pyodbc.Error as e:
                                        #     logging.error(f"Error inserting trial balance value of row ({index+DATA_START_ROW-1}) and column ({col_idx}) into system  (fileId:{fileId}, branch_id:{branch_id}, mis_head_id:{mis_head_id}, particulars_id:{particulars_id}, tb_amount:{tb_amount}): {e}")
                                        #     generic.log_error_to_db(f"Error inserting trial balance value of row ({index+DATA_START_ROW-1}) and column ({col_idx}) into system: {e}", DATA_START_ROW+ (index-1), col_idx, helper.index_to_column_letter(col_idx),fileId)

                                    logging.debug(f"Commiting All branch details for row Data for MIS Head {mis_head_id}, TB Particulars {particulars_id}.")
            
                                    cursor.executemany(insert_sql, single_row_multiplebranch_data)
                                    conn.commit()
                                                    
                            except ValueError as v:
                                logging.error(f"Value error: {v}")
                            except Exception as e:
                                logging.error(f"Error processing file {file_path}: {e}")        
                    elapsed_time = (time.time()) - start_time
                    logging.warning(f"insert took -{elapsed_time:.4f} seconds")

        del df
        logging.debug("Processing completed, dataframe delete")
        xls.close()
        logging.debug("Processing completed, excel instance closed")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

    # End time
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.warning(f"Time taken: {elapsed_time:.4f} seconds")

def StartFileProcessing(fileSourcePath):
    try:
        
        if fileSourcePath.endswith(tuple(ALLOWED_EXTENSIONS)):
            logging.debug(f"New file detected: {fileSourcePath}")
            dbOperations.execute_stored_procedure('dbsp_TBArchieveErrors', None)
            
            def read_excel_and_extract_data(file_path):
                fileId = 0
                with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                    df = pd.read_excel(xls, usecols=[0, 1, 2, 3, 4], nrows=HEADER_ROW)
                    #df = pd.read_excel(file_path, engine='openpyxl', usecols=[0, 1, 2,3,4], nrows=HEADER_ROW)  # Limit to 3 columns and 7 rows
                    #for cell in df.stack():  # Iterate over all cells
                    for (row_idx, col), cell in df.stack().items():
                        col_idx = df.columns.get_loc(col)
                        if isinstance(cell, str) and "Period =" in cell:
                            #match = re.search(r"Period = (\w+ \d{4})", cell)
                            match = re.search(r"Period\s*=\s*([A-Za-z]+)\s+(\d{4})", cell)
                            if match:
                                month = match.group(1)
                                year = match.group(2)

                                if not helper.try_parse(year, int):
                                    logging.error(f"Error period field have wrong/missing year: {year} informathion")
                                    generic.log_error_to_db(f"Error, period field have wrong/missing year informathion, excel contains value:{cell}", row_idx, col_idx, 0)

                                month_numeric = generic.get_month_number(month)
                                if month_numeric:
                                    params= f'''@TBFileName ='{file_path.split('/')[-1]}', @TBMonth ={month_numeric}, @TBYear ={int(year)}, @FileId = @out output'''
                                    fileId = dbOperations.execute_stored_procedure('dbsp_InsertTBFileMonthYearLink', params, True, False)
                                    logging.debug(f"Inserted: {file_path}, Numeric Date: {(int(year), month_numeric)}")
                                else:
                                    logging.error(f"Error, period field have wrong/missing month: {month} informathion")
                                    generic.log_error_to_db(f"Error, period field missing Month information, excel contains value:{cell}", row_idx, col_idx, 0)
                                break;
                            else:
                                logging.error(f"Unable to find Perid data in excel")
                                generic.log_error_to_db(f"Error period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
                    if fileId == 0:
                        logging.error(f"Unable to find Perid data in uploaded excel file")
                        generic.log_error_to_db(f"Error - period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
                    del df
                    xls.close()
                    return fileId

            fileId = read_excel_and_extract_data(fileSourcePath)
            process_file(fileSourcePath, fileId)
            error_count = dbOperations.getData_scalar(None, config.get_env_variable("error_table"), "count(1)", "","")
            if error_count > 0:
                return False
            return True
        else:
            logging.error(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.")
            generic.log_error_to_db(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.", 0, 0, 0)
            return False
    except Exception as e:
        logging.error(f"Error in Initial checks for uploaded files: {e}", exc_info=True)
        return False
    finally:
         if FILE_ARCHIVALPATH:
            file_name = file_name = os.path.basename(fileSourcePath)
            archive_completePath = os.path.join(FILE_ARCHIVALPATH, file_name)
            os.rename(fileSourcePath, archive_completePath)