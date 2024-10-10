import pandas as pd
import logging
from datetime import datetime
import time
import os, re
from logging_config import setup_logging
import helper, dbOperations, generic, config
from sqlalchemy import create_engine

logging = setup_logging()

HEADER_ROW = int(config.get_env_variable('HEADER_ROW', 1)) - 1  # Convert to 0-based index
DATA_START_ROW = int(config.get_env_variable('DATA_START_ROW', 1)) - 1  # Convert to 0-based index
ALLOWED_EXTENSIONS = ["." + ext if not ext.startswith(".") else ext for ext in config.get_env_variable('file_extentions_allowed',"").split(",")]
FILE_ARCHIVALPATH = config.get_env_variable('file_archivalPath', "")
expected_columns = {'Code', 'Particulars', 'Nature', 'MIS Head'}
end_row_text_tofind = config.get_env_variable("TEXT_AT_END_ROW", "Total")

start_branchcolumn_idex =(int(config.get_env_variable_inDigit("BRANCH_COLUMN_START"))) if helper.try_parse(config.get_env_variable("BRANCH_COLUMN_START"), int) else (helper.column_letter_to_index(config.get_env_variable("BRANCH_COLUMN_START", "E"))) 
end_branchcolumn_index = (int(config.get_env_variable_inDigit("BARNCH_COLUMN_END"))) if helper.try_parse(config.get_env_variable("BARNCH_COLUMN_END"), int) else (helper.column_letter_to_index(config.get_env_variable("BARNCH_COLUMN_END", "AB")))

code_col_index = (int(config.get_env_variable_inDigit("col_code", None)) - 1)
particulars_col_index = (int(config.get_env_variable_inDigit("col_particulars", 2)) - 1)
nature_col_index = (int(config.get_env_variable_inDigit("col_nature", 3)) - 1)
misheads_col_index = (int(config.get_env_variable_inDigit("col_mis_heads", 4)) - 1)
MISHead_Collection =[]
Particulars_Collection =[]

#load master details for MIS head and Partivulars + code in session
def load_MIShead_Particulars():
    global MISHead_Collection, Particulars_Collection
    _, MISHead_Collection = dbOperations.getData_withoutFilter("TB_MISHead",True)
    _, Particulars_Collection = dbOperations.getData_withoutFilter("TB_Particulars",True)


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
        generic.log_error_to_db(f"Required fields ({', '.join(missing_cols)}) missing from header in uploaded file.", HEADER_ROW, 0, '')
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
    
    '''  follow below steps  -- Check if any of value is non numeric for expected Branch numeric values 
    1.  # get subset field which falls for branch numeric details
    2.  # Clean the data: strip any leading/trailing spaces
    3.  # Convert to numeric, forcing non-numeric values to NaN
    4.  # Check for any NaN values, which indicate non-numeric entries
    5.  # If you want to see the rows and columns that contain non-numeric values
    '''
    # total_row_index = df.index[df.iloc[:, particulars_col_index].str.lower() == end_row_text_tofind.lower()].tolist()
    # if not total_row_index :
    #     total_row_index = df.index[df.iloc[:, code_col_index].str.lower() == end_row_text_tofind.lower()].tolist()

    total_row_index = df.index[(df.iloc[:, particulars_col_index].str.lower() == end_row_text_tofind.lower()) | (df.iloc[:, code_col_index].str.lower() == end_row_text_tofind.lower())].tolist()

    subset_df = df.iloc[:total_row_index[0], start_branchcolumn_idex-1:end_branchcolumn_index] if total_row_index else df.iloc[:, start_branchcolumn_idex-1:end_branchcolumn_index]
    subset_df = subset_df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)
    numeric_df = subset_df.apply(pd.to_numeric, errors='coerce')
    non_numeric_mask = numeric_df.isna()
    non_numeric_cells = non_numeric_mask.stack()[non_numeric_mask.stack()].index.tolist()
    for row_idx, col_name in non_numeric_cells:
        col_idx = df.columns.get_loc(col_name) +1
        col_letter = helper.index_to_column_letter(col_idx)
        error_list.append((f"Blank/Invalid (expected Numeric value) in column '{col_letter}' at row {row_idx+DATA_START_ROW +1}.", DATA_START_ROW+ row_idx+1, col_idx, col_letter ,fileId))

    #check for empty field in first 4 columns which are (code, particulars, nature & MIS_Head)
    subset_df = df.iloc[:total_row_index[0], 0:start_branchcolumn_idex-2] if total_row_index else df.iloc[:, 0:start_branchcolumn_idex-2]
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

        # Create a new DataFrame from final_data
        error_df = pd.DataFrame(updated_error_list, columns=['error_process', 'errorMessage', 'rowNumber', 'colNumber', 'colName','fileId','error_time'])
        engine = create_engine(dbOperations.connect_db_sqlalchemy())
        try:
            error_df.to_sql(config.get_env_variable("error_table", "TB_error_log"), con=engine, if_exists='append', index=False)
            logging.debug("Error Data inserted successfully!")
        except Exception as e:
            logging.error(f"Error inserting Error entries: {e}")
            generic.log_error_to_db_many(updated_error_list)

    else: validation_Succeed = True    
         
    logging.debug("Validation of Data Completed")
    return validation_Succeed

# Process the Excel file and insert data into the database
def process_file(file_path, fileId):
    global MISHead_Collection, Particulars_Collection
    start_time = time.time()
    try:
        load_MIShead_Particulars()

        # Read Excel file starting from the configured header row and actual data row
        with pd.ExcelFile(file_path, engine='openpyxl') as xls:
            try:
                df = pd.read_excel(xls, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))
                #df = pd.read_excel(file_path, header=HEADER_ROW, skiprows=range(HEADER_ROW+1, DATA_START_ROW), usecols=range(end_branchcolumn_index))

                total_row_index = df.index[(df.iloc[:, particulars_col_index].str.lower() == end_row_text_tofind.lower()) | (df.iloc[:, code_col_index].str.lower() == end_row_text_tofind.lower())].tolist()

                code_col = df.columns[code_col_index]
                particulars_col = df.columns[particulars_col_index]
                if total_row_index:
                    #limit df till "total" row index
                    total_rows = total_row_index[0]-1
                    df.loc[:total_rows, code_col] = df.loc[:total_rows].apply(lambda row: ''.join(word[0] for word in row[particulars_col].split()) if pd.isna(row[code_col]) or str(row[code_col]).strip() == '' else row[code_col], axis=1)
                    df = df[:total_rows]
                else:
                    df[code_col] = df.apply(lambda row: ''.join(word[0] for word in row[particulars_col].split()) if pd.isna(row[code_col]) or str(row[code_col]).strip() == '' else row[code_col], axis=1)

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
                        df_columns =['fileId', 'branch_id', 'head_id', 'particulars_id', 'tb_amount'] 
                        final_data=[]
                        for index, row in df.iterrows():
                            try:
                                logging.debug("stared new row reading after exceldatachecks")

                                code = row[code_col_index]  # Code data from column A
                                particulars = row[particulars_col_index]  # Particulars data from column B
                                nature = row[nature_col_index]  # Nature data from column D
                                mis_head = row[misheads_col_index]  # MIS head data from column D

                                logging.debug(f"Data at row {index+HEADER_ROW} are Code:{code}, particulars:{particulars}, nature:{nature}, mis head:{mis_head}")

                                #break the loop as soon as we hit the total row
                                if (particulars.strip().lower() == "total"): 
                                    logging.debug(f"Reached at Total row at index {index+DATA_START_ROW-1}, ending file processing.")
                                    break

                                # as per finance team they need to use first letter of each word of partuvlar columns, if code is blank
                                if code is None or not isinstance(code, str):
                                    code = ''.join([word[0] for word in particulars.split()])

                                mis_head_id = next((d["head_id"] for d in MISHead_Collection if d.get("head_name") == mis_head), None)
                                if mis_head_id: logging.debug(f"Received MIS Head ID {mis_head_id}")
                                
                                particulars_id =  next((d["Id"] for d in Particulars_Collection if d.get("code") == code), None)
                                if particulars_id: logging.debug(f"Received Particulars ID {mis_head_id}")

                                # Prepare data for each branch column
                                for branch_col in df.columns[start_branchcolumn_idex - 1:]:
                                    branch_id = branch_col  # Assuming branch name or id is in the column header
                                    tb_amount = row[branch_col]
                                    final_data.append((fileId, branch_id, mis_head_id, particulars_id, tb_amount))

                                # if mis_head_id and particulars_id:
                                #     final_data.append(
                                #         (fileId, branch_col, mis_head_id, particulars_id, row[branch_col])
                                #         for branch_col in df.columns[start_branchcolumn_idex - 1:]
                                #     )
                                
                                
                                # fileId= 5
                                # if mis_head_id and particulars_id:
                                #     single_row_multiplebranch_data = []
                                #     for branch_col in df.columns[start_branchcolumn_idex-1:]:
                                #         branch_id = branch_col  # Assuming branch name or id is in the column header
                                #         tb_amount = row[branch_col]
                                        
                                #         single_row_multiplebranch_data.append(( fileId, branch_id, mis_head_id, particulars_id, tb_amount))
                                    
                            except ValueError as v:
                                logging.error(f"Value error: {v}")
                            except Exception as e:
                                logging.error(f"Error processing file {file_path}: {e}")        
                        
                        
                        # Create a new DataFrame from final_data
                        final_df = pd.DataFrame(final_data, columns=df_columns)

                        # Create a SQLAlchemy engine
                        engine = create_engine(dbOperations.connect_db_sqlalchemy())

                        # Insert the DataFrame into the database
                        # Replace 'your_table_name' with the actual table name
                        try:
                            final_df.to_sql(config.get_env_variable("tb_balance", "TB_TrialBalance"), con=engine, if_exists='append', index=False)
                            logging.debug("Data inserted successfully!")
                        except Exception as e:
                            logging.error(f"Error inserting TB Data: {e}")

                        elapsed_time = (time.time()) - start_time
                        logging.warning(f"insert took -{elapsed_time:.4f} seconds")
            
                if 'df' in locals() and not df.empty: del df
            except Exception as e:
                logging.error(f"Error ocurred while processing file: {e}", exc_info=True)
            finally:
                xls.close()
                logging.debug("Processing completed, excel instance closed")    
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

    # End time
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.warning(f"Time taken: {elapsed_time:.4f} seconds")

def read_excel_and_extract_data(file_path):
    uploadedMonth = 0
    uploadedYear=0
    isSuccess = False
    with pd.ExcelFile(file_path, engine='openpyxl') as xls:
        try:
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
                            uploadedMonth = month_numeric
                            uploadedYear = int(year)

                            isSuccess = True
                            logging.debug(f"Inserted: {file_path}, Numeric Date: {(int(year), month_numeric)}")
                        else:
                            logging.error(f"Error, period field have wrong/missing month: {month} informathion")
                            generic.log_error_to_db(f"Error, period field missing Month information, excel contains value:{cell}", row_idx, col_idx, 0)
                        break;
                    else:
                        logging.error(f"Unable to find Perid data in excel")
                        generic.log_error_to_db(f"Error period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
                else:
                    logging.error(f"Unable to find Perid data in excel")
                    generic.log_error_to_db(f"Error period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
            
            if 'df' in locals() and not df.empty: del df
        except Exception as e:
            logging.error(f"Error ocurred while start file processing: {e}", exc_info=True)
        finally:
            xls.close()
            return isSuccess, uploadedMonth, uploadedYear

def StartFileProcessing(fileSourcePath,uploadedMonth,uploadedYear):
    is_success= True
    try:
        if fileSourcePath.endswith(tuple(ALLOWED_EXTENSIONS)):
            logging.debug(f"New file detected: {fileSourcePath}")
            dbOperations.execute_stored_procedure('dbsp_TBArchieveErrors', None)
            
            params= f'''@TBFileName ='{fileSourcePath.split('/')[-1]}', @TBMonth ={uploadedMonth}, @TBYear ={uploadedYear}, @FileId = @out output'''
            fileId = dbOperations.execute_stored_procedure('dbsp_InsertTBFileMonthYearLink', params, True, False)
            logging.debug(f"Inserted: {fileSourcePath}, Numeric Date: {(uploadedYear, uploadedMonth)}")
            
            process_file(fileSourcePath, fileId)
        else:
            logging.error(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.")
            generic.log_error_to_db(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.", 0, 0, 0)
            is_success= False
    except Exception as e:
        logging.error(f"Error in Initial checks for uploaded files: {e}", exc_info=True)
        is_success= False
    finally:
        error_count = dbOperations.getData_scalar(None, config.get_env_variable("error_table"), "count(1)", "","")
        if error_count > 0 or fileId ==0:
            is_success= False
        fileArchive(fileSourcePath, error_count)      
        return is_success;

def GetFileDetailsByFileId(fileId):
    is_success= True
    try:
        columns, values= dbOperations.getData("TB_FILE_MONTH_LINK", "ID", fileId, True)
        if values:
            fileSourcePath = values[0]["TBFileName"]
            uploadedMonth = values[0]["TBMonth"]
            uploadedYear = values[0]["TBYear"]

        return fileSourcePath,uploadedMonth,uploadedYear
    except Exception as e:
        logging.error(f"Error in Initial checks for uploaded files: {e}", exc_info=True)
        is_success= False
    finally:
        error_count = dbOperations.getData_scalar(None, config.get_env_variable("error_table"), "count(1)", "","")
        if error_count > 0 or fileId ==0:
            is_success= False
        fileArchive(fileSourcePath, error_count)      
        return is_success;


def fileArchive(fileSourcePath, error_count):
    try:
        if FILE_ARCHIVALPATH:
                logging.debug(f"File archieval started")
                file_name = file_name = os.path.basename(fileSourcePath)
                file_name = f"{datetime.now().strftime('%d%m%y%H%M%S')}_{file_name}"
                archive_folder = os.path.join(FILE_ARCHIVALPATH, "Success" if error_count == 0 else "Failed")
                archive_completePath = os.path.join(archive_folder, file_name)
                os.makedirs(archive_folder, exist_ok=True)
                os.rename(fileSourcePath, archive_completePath)
                logging.debug(f"File archieved")
    except Exception as ex:
        logging.error("Failed to archive file")

def checkIfTBAlreadyExists(fileSourcePath):
    is_check_failed = 0
    try:
        if fileSourcePath.endswith(tuple(ALLOWED_EXTENSIONS)):
            logging.debug(f"New file detected: {fileSourcePath}")
            dbOperations.execute_stored_procedure('dbsp_TBArchieveErrors', None)
            
            def read_excel_and_check_data(file_path):
                fileId = 0
                with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                    try:
                        df = pd.read_excel(xls, usecols=[0, 1, 2, 3, 4], nrows=HEADER_ROW)
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
                                        fileId = dbOperations.getData_scalar(None, f'TB_FILE_MONTH_LINK WHERE TBMonth = {month_numeric} AND TBYear = {int(year)} AND DEACTIVATE = 0', 'COUNT(1)', None, None) 
                                        if fileId >0 :
                                            logging.debug(f"Data already exists for Year: {int(year)} and Month :{month_numeric}")
                                        else:
                                            logging.debug(f"No data exists for Year: {int(year)} and Month :{month_numeric}")
                                    else:
                                        logging.error(f"Error, period field have wrong/missing month: {month} informathion")
                                        generic.log_error_to_db(f"Error, period field missing Month information, excel contains value:{cell}", row_idx, col_idx, 0)
                                        fileId =-1
                                    break;
                                else:
                                    logging.error(f"Unable to find Perid data in excel")
                                    generic.log_error_to_db(f"Error period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
                                    fileId=-1
                        if fileId == -1:
                            logging.error(f"Unable to find Perid data in uploaded excel file")
                            generic.log_error_to_db(f"Error - period details not available in uploaded file, looks like wrong file or someone removed that data", 4, 1, 0)
                        
                        if 'df' in locals() and not df.empty: del df
                    except Exception as e:
                        logging.error(f"Error ocurred while start file processing: {e}", exc_info=True)
                    finally:
                        xls.close()
                        return fileId
                    
            is_check_failed = read_excel_and_check_data(fileSourcePath)
        else:
            logging.error(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.")
            generic.log_error_to_db(f"Error, wrong file uploaded, only files with {','.join(ALLOWED_EXTENSIONS)} extentions allowed.", 0, 0, 0)
            is_check_failed =-1
    except Exception as e:
        logging.error(f"Error in Initial checks for uploaded files: {e}", exc_info=True)
        is_check_failed =-1
    finally:
        return is_check_failed