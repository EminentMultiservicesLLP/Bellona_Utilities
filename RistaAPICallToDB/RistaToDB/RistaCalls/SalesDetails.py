import os
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict
import RistaCalls.RistaAPICalls as API
from RistaToDB import DBOperations as DBCall
from RistaCalls.GenericClass import QueryType as QueryType
from RistaCalls.GenericClass import executeType as executeType
from Logging_config import setup_logging
import json
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()
# Initialize the logger
logger = setup_logging()

df_salesSourceInfo =[]
df_salesDelivery =[]
df_salesDeliveryBy =[]
df_salesCustomers =[]
df_salesDiscount =[]
df_salesTaxes =[]
df_salesCharges =[]
df_salesPayments =[]
df_salesRefunds = []
df_salesloyalties = []            
df_salesResourceInfo =[]
df_salesEventLog =[]
df_salesBatches =[]
df_salesItems =[]
df_saleItemOptions=[]
df_saleItemTaxes=[]
df_saleItemDiscount=[]

# Function to clear all DataFrames or lists
def clear_all_dataframes():
    global df_salesSourceInfo, df_salesDelivery, df_salesDeliveryBy, df_salesCustomers, df_salesDiscount
    global df_salesTaxes, df_salesCharges, df_salesPayments, df_salesRefunds, df_salesloyalties
    global df_salesResourceInfo, df_salesEventLog, df_salesBatches, df_salesItems, df_saleItemOptions
    global df_saleItemTaxes, df_saleItemDiscount

    # Reinitialize all DataFrames to empty DataFrames
    df_salesSourceInfo = []
    df_salesDelivery = []
    df_salesDeliveryBy = []
    df_salesCustomers = []
    df_salesDiscount = []
    df_salesTaxes = []
    df_salesCharges = []
    df_salesPayments = []
    df_salesRefunds = []
    df_salesloyalties = []
    df_salesResourceInfo = []
    df_salesEventLog = []
    df_salesBatches = []
    df_salesItems = []
    df_saleItemOptions = []
    df_saleItemTaxes = []
    df_saleItemDiscount = []


def createBasicQuery(table_name, data, queryType=QueryType, WhereClause=None):
    columns = ', '.join(data.keys())
    placeholders = ', '.join('?' * len(data))  # Use ? for each value to prevent SQL injection
    if queryType == QueryType.Insert:
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    elif queryType == QueryType.Update:
        query = f"Update {table_name} set  {', '.join([f"{key}=?" for key in data.keys()])}  Where {WhereClause}"
    elif queryType == QueryType.Select:
        query = f"Select columns from {table_name} Where {WhereClause}"
    elif queryType == QueryType.Delete:
        query = f"Delete from {table_name} Where {WhereClause}"

    return query

def Map_APIData_With_DictionaryObject(apiDataObject, dictionaryObject):
    for attr in apiDataObject:
        attr_lower = attr.lower()
        if attr_lower in dictionaryObject.keys():
            dictionaryObject[attr_lower] = apiDataObject[attr]

    return dictionaryObject

def Insert_DF_Data(final_data, table_name):
    df_columns = [key.lower() for key in final_data[0].keys()] 
    data = [tuple(d.values()) for d in final_data]

    final_df = pd.DataFrame(data, columns=df_columns)

    # Create a SQLAlchemy engine
    engine = create_engine(DBCall.connect_db_sqlalchemy())

    # Insert the DataFrame into the database
    # Replace 'your_table_name' with the actual table name
    try:
        final_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.debug("Data inserted successfully!")

        if 'final_df' in locals() and not final_df.empty: del final_df
    except Exception as e:
        logger.error(f"Error inserting TB Data: {e}")

    

def GetSalesPageforDay(params=None):
    try:
        global df_salesSourceInfo
        global df_salesDelivery
        global df_salesDeliveryBy
        global df_salesCustomers
        global df_salesDiscount
        global df_salesTaxes
        global df_salesCharges
        global df_salesPayments
        global df_salesRefunds
        global df_salesloyalties            
        global df_salesResourceInfo
        global df_salesEventLog
        global df_salesBatches
        global df_salesItems
        global df_saleItemOptions
        global df_saleItemTaxes
        global df_saleItemDiscount

        #logger.debug(f"call received for GetSalesPageforDay with params : {params}")
        data_sales_summary = API.GetData_RistaAPI(os.getenv("todays_salesData_url"), params)
        #logger.info(f"Sales Page data for day received url:{os.getenv("todays_salesData_url")}, params : {params}, total {len(data_sales_summary)} entries found.")
        #json.dumps(data_sales_summary, indent=4)

        #logger.info("Start looping through all list of data received for inserting into database.")
        
        for entry in data_sales_summary:
            #logger.debug(f"Started call to InsertInvoice_and_prepareOtherRelatedData method")
            InsertInvoice_and_prepareOtherRelatedData(entry)
            #logger.debug(f"Completed InsertInvoice_and_prepareOtherRelatedData method call")
        
        if df_salesSourceInfo: Insert_DF_Data(df_salesSourceInfo, 'Rista_SaleSourceInfo')
        if df_salesPayments: Insert_DF_Data(df_salesPayments,'Rista_SalePayments')
        
        if df_saleItemOptions: Insert_DF_Data(df_saleItemOptions, 'Rista_SaleItemOptions') 
        if df_saleItemTaxes: Insert_DF_Data(df_saleItemTaxes,'Rista_SaleItemTaxes')
        if df_saleItemDiscount: Insert_DF_Data(df_saleItemDiscount,'Rista_SaleItemDiscounts') 
        
        if df_salesTaxes: Insert_DF_Data(df_salesTaxes,'Rista_SaleTaxes')
        if df_salesCharges: Insert_DF_Data(df_salesCharges,'Rista_SaleCharges')
        if df_salesCustomers: Insert_DF_Data(df_salesCustomers,'Rista_SaleCustomers')
        if df_salesDelivery: Insert_DF_Data(df_salesDelivery,'Rista_SaleDelivery')
        if df_salesDeliveryBy: Insert_DF_Data(df_salesDeliveryBy,'Rista_SaleDeliveryBy')
        if df_salesDiscount: Insert_DF_Data(df_salesDiscount,'Rista_SaleDiscounts')
        if df_salesloyalties: Insert_DF_Data(df_salesloyalties,'Rista_SaleLoyalties')
        if df_salesRefunds: Insert_DF_Data(df_salesRefunds,'Rista_SaleRefunds')
        if df_salesResourceInfo: Insert_DF_Data(df_salesResourceInfo,'Rista_SaleResource')
        if df_salesBatches: Insert_DF_Data(df_salesBatches,'Rista_SaleBatches')
        if df_salesEventLog: Insert_DF_Data(df_salesEventLog,'Rista_SaleEventLog')
        
        # Call the function to clear all data
        clear_all_dataframes()
        
    except Exception as e:
        logger.error(f"Failed for processing Sale page data, error {e}")

def InsertInvoice_and_prepareOtherRelatedData(entry):
    try:
        #logger.debug(f"Started call to InsertInvoice_and_prepareOtherRelatedData method")
        #insert Invoice data into database as is from API
        invoiceId = InsertInvoiceDetails(entry)

        if invoiceId > 0 :
            #delete all entries for invoice related tables
            params= f'''@InvoiceId ={invoiceId}'''
            deleteQuery = f"EXEC dbsp_deleteExistingInviceDetails_ById {params}";
            DBCall.executeNonQuery(deleteQuery)
            logger.warning(f"All child tables data deleted for invoice id: {invoiceId}")

            #Prepare information
            prepare_SaleSourceinfo(entry["sourceInfo"], invoiceId) if (entry.get("sourceInfo")) else logger.warning(f"sourceinfo data not available in API data for for InvoiceId : {invoiceId} ")
            prepare_SaleDelivery(entry["delivery"], invoiceId)if (entry.get("delivery")) else logger.warning(f"delivery data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleDeliveryBy(entry["deliveryBy"], invoiceId) if (entry.get("deliveryBy")) else logger.warning(f"deliveryBy data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleCustomers(entry["customer"], invoiceId) if (entry.get("customer")) else logger.warning(f"customer data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleDiscount(entry["discounts"], invoiceId) if (entry.get("discounts")) else logger.warning(f"discounts data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleTaxes(entry["taxes"], invoiceId) if (entry.get("taxes")) else logger.warning(f"taxes data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleCharges(entry["charges"], invoiceId) if (entry.get("charges")) else logger.warning(f"charges data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SalePayments(entry["payments"], invoiceId) if (entry.get("payments")) else logger.warning(f"payments data not available in API data for InvoiceId : {invoiceId} ")
            prepare_SaleRefunds(entry["overallRefunds"], invoiceId) if (entry.get("overallRefunds")) else logger.warning(f"overallRefunds data not available in API data for InvoiceId : {invoiceId} ")
            prepare_Saleloyalties(entry["loyalties"], invoiceId) if (entry.get("loyalties")) else logger.warning(f"loyalties data not available in API data for for InvoiceId : {invoiceId} ")
            prepare_SaleResourceInfo(entry["resourceInfo"], invoiceId) if (entry.get("resourceInfo")) else logger.warning(f"resourceInfo data not available in API data for for InvoiceId : {invoiceId} ")
            prepare_SaleEventLog(entry["eventLog"], invoiceId) if (entry.get("eventLog")) else logger.warning(f"eventLog data not available in API data for for InvoiceId : {invoiceId} ")
            prepare_SaleBatches(entry["batches"], invoiceId) if (entry.get("batches")) else logger.warning(f"batches data not available in API data for for InvoiceId : {invoiceId} ")
            prepare_SaleItems(entry["items"], invoiceId) if (entry.get("items")) else  logger.warning(f"items data not available in API data for for InvoiceId : {invoiceId} ")
          
        #logger.info(f"Completed call to InsertInvoice_and_prepareOtherRelatedData method")
    except Exception as e:
        logger.error(f"Error occured while inserting data into database for Sale page function (InsertInvoice_and_prepareOtherRelatedData), error {e}")


#insert Invoice data into database as is from API
def InsertInvoiceDetails(entry):
    #logger.debug(f"Started call to InsertInvoiceDetails method")
    invoiceId = 0
    try:
        invoice_data = {
                    'branchcode':'',
                    'invoicenumber':'',
                    'ordernumber':'',
                    'statementnumber':'',
                    'invoicedate':'',
                    'invoiceday':'',
                    'createddate':'',
                    'modifieddate':'',
                    'devicelabel':'',
                    'invoicetype':'',
                    'status':'',
                    'fulfillmentstatus':'',
                    'sourceoutletid':0,
                    'originalsaleinvoicenumber':'',
                    'deliverymode':'',
                    'label':'',
                    'labelgroup':'',
                    'sessionlabel':'',
                    'personcount':0,
                    'salebyuserid':'',
                    'channel':'',
                    'currency':'',
                    'itemcount':0,
                    'itemtotalamount':0,
                    'directchargeamout':0,
                    'chargeamount':0,
                    'discountamount':0,
                    'totaldiscountamount':0,
                    'taxamountincluded':0,
                    'taxamountexcluded':0,
                    'grossamount':0,
                    'netdiscountamount':0,
                    'netdirectchargeamount':0,
                    'netamount':0,
                    'netchargeamount':0,
                    'billamount':0,
                    'roundoffamount':0,
                    'billroundedamount':0,
                    'tipamount':0,
                    'totalamount':0,
                    'totalcost':0,
                    'note':'',
                    'url':'',
                    'StatusInfoReason':'',
                    'accountingroundoff':0,
                    'lastkey':''
                }

        #logger.debug(f"Calling Map_APIData_With_DictionaryObject for Invoice details")
        data = Map_APIData_With_DictionaryObject(entry, invoice_data)
        table_name = os.getenv("rista_saleInvoice_tbl")
        columns  = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))  # Use ? for each value to prevent SQL injection

        #logger.debug(f"Creating Where clause statement for Invoice details")
        WhereClause =  f" invoicenumber = '{entry["invoiceNumber"]}' and branchCode = '{entry["branchCode"]}' and invoiceday = '{entry["invoiceDay"]}' "

        #logger.debug(f"Check if Invoice Date, Invoice number & Branch code combination entry already exists in system")
        checkOutput = DBCall.executeQuery(f"SELECT DISTINCT invoiceId, CAST(modifieddate AS datetime) modifieddate FROM {table_name} WHERE {WhereClause}", None, executeType.Reader)
        #logger.debug(f"Check completed")

        modified_Date = datetime.fromisoformat(entry["modifiedDate"])
        formatted_date = modified_Date.strftime('%Y-%m-%d %H:%M:%S')

        #UPDATE INVOICE ID IF RECORD FOUND
        invoiceId = checkOutput[0][0] if checkOutput != None and len(checkOutput) > 0 and checkOutput[0][0] > 0 else 0 

        if checkOutput != None and len(checkOutput) > 0 and str(checkOutput[0][1]) == formatted_date :
            invoiceId = invoiceId #skip updating or inserting entry because record already exist and we have latest data in system
            logger.warning(f"skip updating or inserting entry for sale invoice because record already exist and we have latest data in system, Modifieddate:{checkOutput[0][1]}, InvoiceId:{checkOutput[0][0]}")
        else:
            if invoiceId != None and invoiceId > 0:
                logger.warning(f"Invoice ID {invoiceId} already in system, forming Update query")
                query = f"""SET NOCOUNT ON;
                            Update {table_name} set {', '.join([f"{key}=?" for key in data.keys()])}  WHERE {WhereClause};
                            SELECT InvoiceID FROM {table_name} WHERE {WhereClause};
                            """
            else:
                logger.warning(f"No entry found for InvoiceNumber, BranchCode and Invoice day combination, forming Insert query with API data")
                query = f"""SET NOCOUNT ON;
                        DECLARE @table_identity TABLE(InvoiceID int);
                        INSERT INTO {table_name} ({columns})
                        OUTPUT inserted.InvoiceID INTO @table_identity(InvoiceID) 
                        VALUES({placeholders});
                        SELECT InvoiceID FROM @table_identity;
                        """
            #logger.debug("Database query execute step initiated")
            invoiceId = DBCall.executeQuery(query, list(data.values()), executeType.Scalar)
            #logger.debug("Database query execute step completed")
            logger.warning(f"Sales invoice entrye inserted/updated successfully and Invoice Id is {invoiceId}")

    except Exception as e:
        invoiceId = 0
        logger.error(f"Error while Inserting/Updating InvoiceDetails, error {e}")

    #logger.debug(f"Completed call to InsertInvoiceDetails method")
    return invoiceId

def InsertMultiple_OtherData_AfterInvoiceInsertUpdate(apiData, newDataObject, table_name, invoiceId, whereClause=None):
    #logger.debug(f"Started call to InsertUpdate_OtherData_AfterInvoiceInsert method")
    try:
        columns = ', '.join(newDataObject.keys())
        placeholders = ', '.join('?' * len(newDataObject.keys()))  # Use ? for each value to prevent SQL injection
        insertQuery = f"INSERT INTO {table_name} WITH (TABLOCK) ({columns}) VALUES ({placeholders})"
        
        #logger.debug("Start Data map for query execution")
        final_list=[]
        for resource in apiData:
            for attr in resource:
                attr_lower = attr.lower()
                if attr_lower in newDataObject.keys():
                    newDataObject[attr_lower] = resource[attr]

            final_list.append(tuple(newDataObject.values()))
        
        #logger.debug("Starting query execution with the mapped data values as NonQuery execution")
        # Execute the query with the mapped data values
        DBCall.executeMany(insertQuery, final_list)
        #logger.debug("Completed query execution with the mapped data values as NonQuery execution")
    except Exception as e:
        logger.error(f"Error in menthod InsertUpdate_OtherData_AfterInvoiceInsert, error {e}")

    #logger.debug(f"Completed call to InsertUpdate_OtherData_AfterInvoiceInsert method")

def InsertUpdate_OtherData_AfterInvoiceInsert(apiData, newDataObject, table_name, invoiceId, whereClause=None):
    #logger.debug(f"Started call to InsertUpdate_OtherData_AfterInvoiceInsert method")
    try:
        insertQuery = createBasicQuery(table_name, newDataObject, QueryType.Insert )
        
        ''' no need to use where clause because we are deleting all entries from child table for invoice id and hence we can simple insert new
        WhereClause = whereClause if not whereClause is None else f" InvoiceID = {invoiceId} "

        #logger.debug("Check if the record exists for invoice ID in system")
        # Check if the record exists and determine the query in one line
        if DBCall.executeQuery(f"SELECT 1 FROM {table_name} (nolock) WHERE {WhereClause}", None, executeType.Scalar) != None:
            return
        '''

        #logger.debug("Start Data map for query execution")
        # Map the data
        data = Map_APIData_With_DictionaryObject(apiData, newDataObject)
        
        #logger.debug("Starting query execution with the mapped data values as NonQuery execution")
        # Execute the query with the mapped data values
        DBCall.executeQuery(insertQuery, list(data.values()), executeType.NonQuery)
        #logger.debug("Completed query execution with the mapped data values as NonQuery execution")
    except Exception as e:
        logger.error(f"Error in menthod InsertUpdate_OtherData_AfterInvoiceInsert, error {e}")

    #logger.debug(f"Completed call to InsertUpdate_OtherData_AfterInvoiceInsert method")

def prepare_SaleSourceinfo(entry, invoiceId):
    global df_salesSourceInfo
    #logger.debug(f"Started call to insert_SaleSourceinfo method")
    saleSourceinfo_data = {
        'invoiceid':invoiceId,
        'companyname':'',
        'invoicenumber':'',
        'invoicedate':'',
        'callbackurl':'',
        'source':'',
        'sourceoutletid':'',
        'outletid':'',
        'iseditable':0,
        'verifycoupons':0,
        'isecomorder':0
    }

    df_salesSourceInfo.append(Map_APIData_With_DictionaryObject(entry, saleSourceinfo_data))

def prepare_SaleDelivery(entry, invoiceId):
    global df_salesDelivery
    #logger.debug(f"Started call to insert_SaleDelivery method")
    saleDelivery_data ={
        'invoiceid':invoiceId,
        'title':'',
        'advanceorder':0,
        'name':'',
        'email':'',
        'phonenumber':'',
        'mode':'',
        'addresslabel':'',
        'addressline':'',
        'city':'',
        'state':'',
        'country':'',
        'zip':'',
        'landmark':'',
        'latitude':0,
        'longitude':0,
        'deliverydate':''
    }
    df_salesDelivery.append(Map_APIData_With_DictionaryObject(entry, saleDelivery_data))

def prepare_SaleCustomers(entry, invoiceId):
    global df_salesCustomers
    #logger.debug(f"Started call to insert_SaleCustomers method")
    saleCustomers_data ={
        'invoiceid':invoiceId,
        'id':'',
        'title':'',
        'name':'',
        'email':'',
        'phonenumber':'',
        'taxid':'',
    }
    df_salesCustomers.append(Map_APIData_With_DictionaryObject(entry, saleCustomers_data))

def prepare_SaleDeliveryBy(entry, invoiceId):
    global df_salesDeliveryBy
    #logger.debug(f"Started call to insert_SaleDeliveryBy method")
    saleDeliveryBy_data ={
        'invoiceid':invoiceId,
        "name": "",
        "email": "",
        "phoneNumber": ""
    }
    df_salesDeliveryBy.append(Map_APIData_With_DictionaryObject(entry, saleDeliveryBy_data))

def prepare_SaleItems(entry, invoiceId):
    #logger.debug(f"Started call to insert_SaleItems method")
    itemID = 0
    try:
        saleItems_data ={
            'invoiceid':invoiceId,
            'shortname':'',
            'longname':'',
            'variants':'',
            'skucode':'',
            'barcode':'',
            'categoryname':'',
            'subcategoryname':'',
            'brandname':'',
            'accountname':'',
            'itemnature':'',
            'quantity':0,
            'unitprice':0,
            'overridden':'',
            'measuringunit':'',
            'itemamount':0,
            'optionamount':0,
            'discountamount':0,
            'factoreddiscountamount':0,
            'itemtotalamount':0,
            'grossamount':0,
            'basegrossamount':0,
            'netdiscountamount':0,
            'basenetdiscountamount':0,
            'netamount':0,
            'basenetamount':0,
            'taxamountincluded':0,
            'taxamountexcluded':0,
            'taxamount':0,
            'basetaxamount':0,
            'itemcost':0,
            'createdby':'',
            'createdtime':'',
            'note':'',
            'kotnumber': '',
            'kotgroup': '',
            'kottimestamp': '',
            'kotstatus': ''
        }
        InsertMultiple_OtherData_AfterInvoiceInsertUpdate(entry, saleItems_data, os.getenv("rista_saleItems_tbl"),invoiceId)

        table_name = os.getenv("rista_saleItems_tbl")
        for item in entry:
            #logger.debug(f"Calling Map_APIData_With_DictionaryObject for Invoice Items details")
            #logger.debug(f"Statement for getting Item Id")
            WhereClause =  f" invoiceid = {invoiceId} and skuCode = '{item["skuCode"]}'"

            #logger.debug(f"Check if Item already exist for Invoice id and skuCode combination entry already exists in system")
            itemID = DBCall.executeQuery(f"SELECT ItemID FROM {table_name} (nolock) WHERE {WhereClause}", None, executeType.Scalar)
            #logger.debug(f"Check completed")

            if(itemID != None and itemID > 0):
                if(item.get("options")): prepare_SaleItemOptions(item["options"], itemID)  
                else: logger.warning(f"options data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")

                if(item.get("taxes")): prepare_SaleItemTaxes(item["taxes"], itemID) 
                else: logger.warning(f"taxes data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")
               
                if(item.get("discounts")): prepare_SaleItemDiscount(item["discounts"], itemID) 
                else: logger.warning(f"discounts data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")

    except Exception as e:
        itemID = 0
        logger.error(f"Error while Inserting/Updating Item details, error {e}")

    #logger.debug(f"Completed call to insert_SaleItems method")
    return itemID

def prepare_SaleItemOptions(entry, itemId):
    global df_saleItemOptions
    #logger.debug(f"Started call to insert_SaleItemOptions method")
    saleItemOption_data ={
        'itemid':itemId,
        'type':'',
        'name':'',
        'variants':'',
        'skucode':'',
        'quantity':0,
        'unitprice':0,
        'amount':0,
        'grossamount':0,
        'netdiscountamount':0,
        'netamount':0,
        'taxamount':0,
        'categoryname':'',
        'subcategoryname':'',
        'brandname':'',
        'accountname':''
    }
    for optionEntry in entry:
        df_saleItemOptions.append(Map_APIData_With_DictionaryObject(optionEntry, saleItemOption_data))

def prepare_SaleItemTaxes(entry, itemId):
    global df_saleItemTaxes
    #logger.debug(f"Started call to insert_SaleItemTaxes method")
    saleItemTaxes_data ={
        'itemid':itemId,
        'name':'',
        'percentage':0,
        'saleamount':0,
        'amountincluded':0,
        'amountexcluded':0,
        'amount':0
    }
    for taxEntry in entry:
        df_saleItemTaxes.append(Map_APIData_With_DictionaryObject(taxEntry, saleItemTaxes_data))

def prepare_SaleItemDiscount(entry, itemId):
    global df_saleItemDiscount
    #logger.debug(f"Started call to insert_SaleItemDiscount method")
    saleItemDiscount_data ={
        'itemid':itemId,
        'name':'',
        'type':'',
        'rate':0,
        'saleamount':0,
        'amount':0,
        'loyaltypoints':0,
        'coupon':'',
        'couponprovider':'',
        'campaignname':'',
        'reason':'',
        'appliedby':''
    }
    for discountEntry in entry:
        df_saleItemDiscount.append(Map_APIData_With_DictionaryObject(discountEntry, saleItemDiscount_data))

# region Sale discount section
def prepare_SaleDiscount(entry, invoiceId):
    global df_salesDiscount
    #logger.debug(f"Started call to insert_SaleDiscount method")
    saleDiscount_data ={
        'invoiceid':invoiceId,
        'name':'',
        'type':'',
        'rate':0,
        'saleamount':0,
        'amount':0,
        'loyaltypoints':0,
        'coupon':'',
        'couponprovider':'',
        'campaignname':'',
        'reason':'',
        'appliedby':''
    }
    for discountEntry in entry:
        df_salesDiscount.append(Map_APIData_With_DictionaryObject(discountEntry, saleDiscount_data))

# region Sale Tax section
def prepare_SaleTaxes(entry, invoiceId):
    global df_salesTaxes
    #logger.debug(f"Started call to insert_SaleTaxes method")
    saleTaxes_data ={
        'invoiceid':invoiceId,
        'name':'',
        'percentage':0,
        'saleamount':0,
        'itemtaxincluded':0,
        'itemtaxexcluded':0,
        'chargetaxincluded':0,
        'chargetaxexcluded':0,
        'amountincluded':0,
        'amountexcluded':0,
        'amount':0
    }
    for taxEntry in entry:
        df_salesTaxes.append(Map_APIData_With_DictionaryObject(taxEntry, saleTaxes_data))

# region Sale Payment section
def prepare_SalePayments(entry, invoiceId):
    global df_salesPayments
    #logger.debug(f"Started call to insert_SalePayments method")
    salePayment_data ={
        'invoiceid':invoiceId,
        'mode':'',
        'submode':'',
        'amount':0,
        'reference':'',
        'note':'',
        'posteddate':'',
    }
    for paymentEntry in entry:
        df_salesPayments.append(Map_APIData_With_DictionaryObject(paymentEntry, salePayment_data))    

# region Sale Charges section
def prepare_SaleCharges(entry, invoiceId):
    global df_salesCharges
    #logger.debug(f"Started call to insert_SaleCharges method")
    saleCharge_data ={
        'invoiceid':invoiceId,
        'name':'',
        'type':'',
        'rate':0,
        'saleamount':0,
        'amount':0,
        'taxamountincluded':0,
        'taxamountexcluded':0,
        'taxamount':0,
        'isdirectcharge':''
    }
    for chargeEntry in entry:
        df_salesCharges.append(Map_APIData_With_DictionaryObject(chargeEntry, saleCharge_data))    

# region Sale Refund section
def prepare_SaleRefunds(entry, invoiceId):
    global df_salesRefunds
    #logger.debug(f"Started call to insert_SaleRefunds method")
    saleRefund_data ={
        'invoiceid':invoiceId,
        'mode':'',
        'refundamount':0,
        'paymentid':0,
        'returntocustomer':0,
        'credittocustomerprepaidbalance':0
    }
    for RefundEntry in entry:
        df_salesRefunds.append(Map_APIData_With_DictionaryObject(RefundEntry, saleRefund_data))    

# region Sale Loyalties section
def prepare_Saleloyalties(entry, invoiceId):
    global df_salesloyalties
    #logger.debug(f"Started call to insert_Saleloyalties method")
    saleloyalties_data ={
        'invoiceid':invoiceId,
        'type':'',
        'points':0,
        'remarks':''
    }
    for loyaltyEntry in entry:
        df_salesloyalties.append(Map_APIData_With_DictionaryObject(loyaltyEntry, saleloyalties_data))    

# region Sale discount section
def prepare_SaleResourceInfo(entry, invoiceId):
    global df_salesResourceInfo
    #logger.debug(f"Started call to insert_SaleResourceInfo method")
    _data ={
        'invoiceid':invoiceId,
    	'branchcode':'',
	    'resourceid':'',
        'resourcename':'',
        'groupsize':0,
	    'resourcegroupname':''
    }
    df_salesResourceInfo.append(Map_APIData_With_DictionaryObject(entry, _data))    

# region Sale EventLog section
def prepare_SaleEventLog(entries, invoiceId):
    global df_salesEventLog
    #logger.debug(f"Started call to insert_SaleEventLog method")
    _data ={
        'invoiceid':invoiceId,
	    'status':'',
	    'note':'',
        'eventbyusername':'',
        'eventdate':'',
    }
    for entry in entries:
        df_salesEventLog.append(Map_APIData_With_DictionaryObject(entry, _data))   

# region Sale Batches section
def prepare_SaleBatches(entries, invoiceId):
    global df_salesBatches
    #logger.debug(f"Started call to insert_SaleBatches method")
    _data ={
        'invoiceid':invoiceId,
        'batchnumber':'',
        'expirydate':'',
	    'quantity':0,
        'supplierbatchnumber':''
    }
    for entry in entries:
        df_salesBatches.append(Map_APIData_With_DictionaryObject(entry, _data))   

#*************************************************
