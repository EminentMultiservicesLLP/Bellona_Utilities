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

load_dotenv()
# Initialize the logger
logger = setup_logging()

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

def GetTestAPIData(params=None):
    try:
        logger.debug(f"call received for GetTestAPIData with params : {params}")
        data_sales_summary = API.GetData_RistaAPI(os.getenv("test_api_url"), params)
        logger.info(f"Sales Page data for day received url:{os.getenv("todays_salesData_url")}, params : {params}, total {len(data_sales_summary)} entries found.")
        print(json.dumps(data_sales_summary, indent=4))

        logger.info("Start looping through all list of data received for inserting into database.")
        # for entry in data_sales_summary:
        #     logger.debug(f"Started call to InsertIntoDB method")
        #     InsertIntoDB(entry)
        #     logger.debug(f"Completed InsertIntoDB method call")
    except Exception as e:
        logger.error(f"Failed for processing Sale page data, error {e}")


    """ commented code
    #Get breakfast, lunch, evening & dinner details
    time_periods = {
        "breakfast": (datetime.strptime(os.getenv("morning_timeslot_start"), "%H:%M:%S").time(), datetime.strptime(os.getenv("lunch_timeslot_start"), "%H:%M:%S").time()),
        "lunch": (datetime.strptime(os.getenv("lunch_timeslot_start"), "%H:%M:%S").time(), datetime.strptime(os.getenv("evening_timeslot_start"), "%H:%M:%S").time()),
        "eveningsnacks": (datetime.strptime(os.getenv("evening_timeslot_start"), "%H:%M:%S").time(), datetime.strptime(os.getenv("dinner_timeslot_start"), "%H:%M:%S").time()),
        "dinner": (datetime.strptime(os.getenv("dinner_timeslot_start"), "%H:%M:%S").time(), datetime.strptime(os.getenv("morning_timeslot_start"), "%H:%M:%S").time())
    }

    # Initialize data structures to hold results
    guest_counts = defaultdict(int)
    bill_amounts = defaultdict(float)
    account_totals = defaultdict(lambda: defaultdict(float))

    # Process each invoice
    for entry in data_sales_summary:
        invoice_time = datetime.fromisoformat(entry["invoiceDate"]).time()
        for period, (start, end) in time_periods.items():
            if (start <= invoice_time < end) or (end < start and (invoice_time >= start or invoice_time < end)):
                guest_counts[period] += entry["personCount"]
                bill_amounts[period] += entry["billRoundedAmount"]
                for item in entry["items"]:
                    account_totals[period][item["accountName"]] += item["baseNetAmount"]
                break

            # Insert into Invoices
            # invoice_data = {
            #     'InvoiceID': entry['invoiceNumber'],  # Simplified, map other fields similarly
            #     # Add other fields from record to invoice_data dictionary
            # }
            #insert_into_table(conn, 'Invoices', invoice_data)
            
        

    # Print results
    for period in time_periods:
        print(f"Time period: {period.capitalize()}")
        print(f"  Guest count: {guest_counts[period]}")
        print(f"  Bill amount: {bill_amounts[period]}")
        print(f"  Account totals:")
        for account_name, total in account_totals[period].items():
            print(f"    {account_name}: {total}")
        print()


    sale_types = ['Food Sale', 'Beverage Sale', 'Wine Sale', 'Beer Sale', 'Liquor Sale', 'Tobacco Sale']
    salesDineIn = {sale: sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] if account['account'] == sale for channel in account['channels']  if channel['name'].startswith('Dinein_')) for sale in sale_types}
    #salesTakeaway = sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] for channel in account['channels'] if not channel['name'].startswith('Dinein_'))

    Salefood, SaleBeverage, SaleWine, saleBeer, SaleLiquor, SaleTobacco = salesDineIn['Food Sale'], salesDineIn['Beverage Sale'], salesDineIn['Wine Sale'], salesDineIn['Beer Sale'], salesDineIn['Liquor Sale'], salesDineIn['Tobacco Sale']
    total_gross_amount = sum(item['itemTotalgrossAmount'] for item in data_sales_summary['items'])
    
    salesTakeaway = sum(channel['netSaleAmount'] for channel in data_sales_summary['channelSummary'] if not channel['name'].startswith('Dinein_'))
    SaleDinnerDinein = sum(channel['netSaleAmount'] for channel in data_sales_summary['channelSummary'] if channel['name'].startswith('Dinein_'))
    TotalNoOfBills  =  sum(channel['noOfSales'] for channel in data_sales_summary['channelSummary'])
    GuestCountDinner  =  sum(channel['paxCount'] for channel in data_sales_summary['channelSummary'] if channel['name'].startswith('Dinein_'))

    """


def InsertIntoDB(entry):
    try:
        logger.debug(f"Started call to InsertIntoDB method")
        #insert Invoice data into database as is from API
        invoiceId = InsertInvoiceDetails(entry)
        if invoiceId > 0 :
            #insert source information
            insert_SaleSourceinfo(entry["sourceinfo"], invoiceId) if "sourceinfo" in entry.keys() else logger.warn(f"sourceinfo data not available in API data for for InvoiceId : {invoiceId} ")
            insert_SaleDelivery(entry["delivery"], invoiceId)if "delivery" in entry.keys() else logger.warn(f"delivery data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleDeliveryBy(entry["deliveryBy"], invoiceId) if "deliveryBy" in entry.keys() else logger.warn(f"deliveryBy data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleCustomers(entry["customer"], invoiceId) if "customer" in entry.keys() else logger.warn(f"customer data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleDiscount(entry["discounts"], invoiceId) if "discounts" in entry.keys() else logger.warn(f"discounts data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleTaxes(entry["taxes"], invoiceId) if "taxes" in entry.keys() else logger.warn(f"taxes data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleCharges(entry["charges"], invoiceId) if "charges" in entry.keys() else logger.warn(f"charges data not available in API data for InvoiceId : {invoiceId} ")
            insert_SalePayments(entry["payments"], invoiceId) if "payments" in entry.keys() else logger.warn(f"payments data not available in API data for InvoiceId : {invoiceId} ")
            insert_SaleRefunds(entry["overallRefunds"], invoiceId) if "overallRefunds" in entry.keys() else logger.warn(f"overallRefunds data not available in API data for InvoiceId : {invoiceId} ")
            insert_Saleloyalties(entry["loyalties"], invoiceId) if "loyalties" in entry.keys() else logger.warn(f"loyalties data not available in API data for for InvoiceId : {invoiceId} ")
            
            insert_SaleItems(entry["items"], invoiceId) if "items" in entry.keys() else  logger.warn(f"items data not available in API data for for InvoiceId : {invoiceId} ")

        logger.info(f"Completed call to InsertIntoDB method")
    except Exception as e:
        logger.error(f"Error occured while inserting data into database for Sale page, error {e}")


#insert Invoice data into database as is from API
def InsertInvoiceDetails(entry):
    logger.debug(f"Started call to InsertInvoiceDetails method")
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

        logger.debug(f"Calling Map_APIData_With_DictionaryObject for Invoice details")
        data = Map_APIData_With_DictionaryObject(entry, invoice_data)
        table_name = os.getenv("rista_saleInvoice_tbl")
        columns  = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))  # Use ? for each value to prevent SQL injection

        logger.debug(f"Creating Where clause statement for Invoice details")
        WhereClause =  f" invoicenumber = '{entry["invoiceNumber"]}' and branchCode = '{entry["branchCode"]}' and invoiceday = '{entry["invoiceDay"]}' "

        logger.debug(f"Check if Invoice Date, Invoice number & Branch code combination entry already exists in system")
        checkOutput = DBCall.executeQuery(f"SELECT DISTINCT invoiceId, CAST(modifieddate AS datetime) modifieddate FROM {table_name} WHERE {WhereClause}", None, executeType.Reader)
        logger.debug(f"Check completed")

        modified_Date = datetime.fromisoformat(entry["modifiedDate"])
        formatted_date = modified_Date.strftime('%Y-%m-%d %H:%M:%S')

        #UPDATE INVOICE ID IF RECORD FOUND
        invoiceId = checkOutput[0][0] if checkOutput != None and len(checkOutput) > 0 and checkOutput[0][0] > 0 else 0 

        if checkOutput != None and len(checkOutput) > 0 and str(checkOutput[0][1]) == formatted_date :
            invoiceId = 0 #skip updating or inserting entry because record already exist and we have latest data in system
            logger.warn(f"skip updating or inserting entry for sale invoice because record already exist and we have latest data in system, Modifieddate:{checkOutput[0][1]}, InvoiceId:{checkOutput[0][0]}")
        else:
            if invoiceId != None and invoiceId > 0:
                logger.warn(f"Invoice ID {invoiceId} already in system, forming Update query")
                query = f"""SET NOCOUNT ON;
                            Update {table_name} set {', '.join([f"{key}=?" for key in data.keys()])}  WHERE {WhereClause};
                            SELECT InvoiceID FROM {table_name} WHERE {WhereClause};
                            """
            else:
                logger.warn(f"No entry found for InvoiceNumber, BranchCode and Invoice day combination, forming Insert query with API data")
                query = f"""SET NOCOUNT ON;
                        DECLARE @table_identity TABLE(InvoiceID int);
                        INSERT INTO {table_name} ({columns})
                        OUTPUT inserted.InvoiceID INTO @table_identity(InvoiceID) 
                        VALUES({placeholders});
                        SELECT InvoiceID FROM @table_identity;
                        """
            logger.debug("Database query execute step initiated")
            invoiceId = DBCall.executeQuery(query, list(data.values()), executeType.Scalar)
            logger.debug("Database query execute step completed")
            logger.warn(f"Sales invoice entrye inserted/updated successfully and Invoice Id is {invoiceId}")

    except Exception as e:
        invoiceId = 0
        logger.error(f"Error while Inserting/Updating InvoiceDetails, error {e}")

    logger.debug(f"Completed call to InsertInvoiceDetails method")
    return invoiceId

def InsertUpdate_OtherData_AfterInvoiceInsert(apiData, newDataObject, table_name, invoiceId, whereClause=None):
    logger.debug(f"Started call to InsertUpdate_OtherData_AfterInvoiceInsert method")
    try:

        WhereClause = whereClause if not whereClause is None else f" InvoiceID = {invoiceId} "
        insertQuery = createBasicQuery(table_name, newDataObject, QueryType.Insert )
        updateQuery = createBasicQuery(table_name, newDataObject, QueryType.Update, WhereClause )

        logger.debug("Check if the record exists for invoice ID in system")
        # Check if the record exists and determine the query in one line
        query = updateQuery if DBCall.executeQuery(f"SELECT 1 FROM {table_name} WHERE {WhereClause}", None, executeType.Scalar) != None else insertQuery

        logger.debug("Start Data map for query execution")
        # Map the data
        data = Map_APIData_With_DictionaryObject(apiData, newDataObject)

        logger.debug("Starting query execution with the mapped data values as NonQuery execution")
        # Execute the query with the mapped data values
        DBCall.executeQuery(query, list(data.values()), executeType.NonQuery)
        logger.debug("Completed query execution with the mapped data values as NonQuery execution")
    except Exception as e:
        logger.error(f"Error in menthod InsertUpdate_OtherData_AfterInvoiceInsert, error {e}")

    logger.debug(f"Completed call to InsertUpdate_OtherData_AfterInvoiceInsert method")

def insert_SaleSourceinfo(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleSourceinfo method")
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
    
    InsertUpdate_OtherData_AfterInvoiceInsert(entry, saleSourceinfo_data, os.getenv("rista_saleSourceinfo_tbl"), invoiceId)
    logger.debug(f"Completed call to insert_SaleSourceinfo method")

def insert_SaleDelivery(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleDelivery method")
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
    
    InsertUpdate_OtherData_AfterInvoiceInsert(entry, saleDelivery_data,os.getenv("rista_saleDelivery_tbl"), invoiceId)
    logger.debug(f"Completed call to insert_SaleDelivery method")

def insert_SaleCustomers(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleCustomers method")
    saleCustomers_data ={
        'invoiceid':invoiceId,
        'id':'',
        'title':'',
        'name':'',
        'email':'',
        'phonenumber':'',
        'taxid':'',
    }
    
    InsertUpdate_OtherData_AfterInvoiceInsert(entry, saleCustomers_data, os.getenv("rista_saleCustomers_tbl"), invoiceId)
    
def insert_SaleDeliveryBy(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleDeliveryBy method")
    saleDeliveryBy_data ={
        'invoiceid':invoiceId,
        "name": "",
        "email": "",
        "phoneNumber": ""
    }
    
    InsertUpdate_OtherData_AfterInvoiceInsert(entry, saleDeliveryBy_data, os.getenv("rista_saleDeliveryBy_tbl"), invoiceId)
    
    logger.debug(f"Completed call to insert_SaleDeliveryBy method")

def insert_SaleItems(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleItems method")
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

        logger.debug(f"Total {len(entry)} entries found in Sales Items")
        for item in entry:
            logger.debug(f"Calling Map_APIData_With_DictionaryObject for Invoice Items details")
            data = Map_APIData_With_DictionaryObject(item, saleItems_data)
            table_name = os.getenv("rista_saleItems_tbl")
            columns  = ', '.join(saleItems_data.keys())
            placeholders = ', '.join('?' * len(saleItems_data.keys()))  # Use ? for each value to prevent SQL injection

            logger.debug(f"Statement for getting Item Id")
            WhereClause =  f" invoiceid = {invoiceId} and skuCode = '{item["skuCode"]}'"

            logger.debug(f"Check if Item already exist for Invoice id and skuCode combination entry already exists in system")
            itemID = DBCall.executeQuery(f"SELECT ItemID FROM {table_name} WHERE {WhereClause}", None, executeType.Scalar)
            logger.debug(f"Check completed")

            if itemID != None and itemID > 0:
                logger.debug(f"itemID ID already in system: {itemID}, forming Update query")
                query = f"""SET NOCOUNT ON;
                            Update {table_name} set {', '.join([f"{key}=?" for key in saleItems_data.keys()])}  WHERE {WhereClause};
                            SELECT ItemID FROM {table_name} WHERE {WhereClause};
                            """
            else:
                logger.debug(f"Forming Insert query for Item with API data")
                query = f"""SET NOCOUNT ON;
                        DECLARE @table_identity TABLE(ItemID int);
                        INSERT INTO {table_name} ({columns})
                        OUTPUT inserted.ItemID INTO @table_identity(ItemID) 
                        VALUES({placeholders});
                        SELECT ItemID FROM @table_identity;
                        """
            logger.debug(f"Database query execute step initiated")
            itemID = DBCall.executeQuery(query, list(data.values()), executeType.Scalar)
            logger.debug(f"Database query execute step completed")

            insert_SaleItemOptions(item["options"], itemID) if "options" in item.keys() else logger.warn(f"options data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")
            insert_SaleItemTaxes(item["taxes"], itemID) if "taxes" in item.keys() else logger.warn(f"taxes data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")
            insert_SaleItemDiscount(item["discounts"], itemID) if "discounts" in item.keys() else logger.warn(f"discounts data in items not available in API data for for InvoiceId : {invoiceId}, itemId:{itemID}")

    except Exception as e:
        itemID = 0
        logger.error(f"Error while Inserting/Updating Item details, error {e}")

    logger.debug(f"Completed call to insert_SaleItems method")
    return itemID

def insert_SaleItemOptions(entry, itemId):
    logger.debug(f"Started call to insert_SaleItemOptions method")
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
    
    logger.debug(f"Looping all item options entry for inserting")
    for optionEntry in entry:
        logger.debug(f"Started inserting item options data")
        InsertUpdate_OtherData_AfterInvoiceInsert(optionEntry, saleItemOption_data, os.getenv("rista_saleItemOptions_tbl"), itemId, f" itemId = {itemId}")
        logger.debug(f"completed inserting item options data")
    
    logger.debug(f"Completed call to insert_SaleItemOptions method")

def insert_SaleItemTaxes(entry, itemId):
    logger.debug(f"Started call to insert_SaleItemTaxes method")
    saleItemTaxes_data ={
        'itemid':itemId,
        'name':'',
        'percentage':0,
        'saleamount':0,
        'amountincluded':0,
        'amountexcluded':0,
        'amount':0
    }
    
    logger.debug(f"Looping all item Taxes entry for inserting")
    for taxEntry in entry:
        logger.debug(f"Started inserting item options data")
        InsertUpdate_OtherData_AfterInvoiceInsert(taxEntry, saleItemTaxes_data, os.getenv("rista_saleItemTaxes_tbl"), itemId, f" itemId = {itemId}")
        logger.debug(f"completed inserting item Tax data")
    
    logger.debug(f"Completed call to insert_SaleItemTaxes method")

def insert_SaleItemDiscount(entry, itemId):
    logger.debug(f"Started call to insert_SaleItemDiscount method")
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
    
    logger.debug(f"Looping all item Taxes entry for inserting")
    for discountEntry in entry:
        logger.debug(f"Started inserting item discount data")
        InsertUpdate_OtherData_AfterInvoiceInsert(discountEntry, saleItemDiscount_data, os.getenv("rista_saleItemDiscounts_tbl"), itemId, f" itemId = {itemId}")
        logger.debug(f"completed inserting item discount data")
    
    logger.debug(f"Completed call to insert_SaleItemDiscount method")

# region Sale discount section
def insert_SaleDiscount(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleDiscount method")
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
    
    logger.debug(f"Looping all invoice discount entry for inserting")
    for discountEntry in entry:
        logger.debug(f"Started inserting invoice discount data")
        InsertUpdate_OtherData_AfterInvoiceInsert(discountEntry, saleDiscount_data, os.getenv("rista_saleDiscounts_tbl"), invoiceId)
        logger.debug(f"completed inserting invoice discount data")
    
    logger.debug(f"Completed call to insert_SaleDiscount method")

# region Sale Tax section
def insert_SaleTaxes(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleTaxes method")
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
    
    logger.debug(f"Looping all sale Taxes entry for inserting")
    for taxEntry in entry:
        logger.debug(f"Started inserting item options data")
        InsertUpdate_OtherData_AfterInvoiceInsert(taxEntry, saleTaxes_data, os.getenv("rista_saleTaxes_tbl"), invoiceId)
        logger.debug(f"completed inserting sale Tax data")
    
    logger.debug(f"Completed call to insert_SaleTaxes method")

# region Sale Payment section
def insert_SalePayments(entry, invoiceId):
    logger.debug(f"Started call to insert_SalePayments method")
    salePayment_data ={
        'invoiceid':invoiceId,
        'mode':'',
        'submode':'',
        'amount':0,
        'reference':'',
        'note':'',
        'posteddate':'',
    }
    
    logger.debug(f"Looping all sale Payment entry for inserting")
    for paymentEntry in entry:
        logger.debug(f"Started inserting Payment data")
        InsertUpdate_OtherData_AfterInvoiceInsert(paymentEntry, salePayment_data, os.getenv("rista_salePayment_tbl"), invoiceId)
        logger.debug(f"completed inserting sale Payment data")
    
    logger.debug(f"Completed call to insert_SalePayments method")

# region Sale Charges section
def insert_SaleCharges(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleCharges method")
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
    
    logger.debug(f"Looping all sale Charges entry for inserting")
    for chargeEntry in entry:
        logger.debug(f"Started inserting sale charge data")
        InsertUpdate_OtherData_AfterInvoiceInsert(chargeEntry, saleCharge_data, os.getenv("rista_saleCharges_tbl"), invoiceId)
        logger.debug(f"completed inserting sale charge data")
    
    logger.debug(f"Completed call to insert_SaleCharges method")

# region Sale Refund section
def insert_SaleRefunds(entry, invoiceId):
    logger.debug(f"Started call to insert_SaleRefunds method")
    saleRefund_data ={
        'invoiceid':invoiceId,
        'mode':'',
        'refundamount':0,
        'paymentid':0,
        'returntocustomer':0,
        'credittocustomerprepaidbalance':0
    }
    
    logger.debug(f"Looping all sale Refund entry for inserting")
    for RefundEntry in entry:
        logger.debug(f"Started inserting sale Refund data")
        InsertUpdate_OtherData_AfterInvoiceInsert(RefundEntry, saleRefund_data, os.getenv("rista_saleRefunds_tbl"), invoiceId)
        logger.debug(f"completed inserting sale Refund data")
    
    logger.debug(f"Completed call to insert_SaleRefunds method")

# region Sale Loyalties section
def insert_Saleloyalties(entry, invoiceId):
    logger.debug(f"Started call to insert_Saleloyalties method")
    saleloyalties_data ={
        'invoiceid':invoiceId,
        'type':'',
        'points':0,
        'remarks':''
    }
    
    logger.debug(f"Looping all sale loyalties entry for inserting")
    for loyaltyEntry in entry:
        logger.debug(f"Started inserting sale loyalties data")
        InsertUpdate_OtherData_AfterInvoiceInsert(loyaltyEntry, saleloyalties_data, os.getenv("rista_saleLoyalties_tbl"), invoiceId)
        logger.debug(f"completed inserting sale loyalties data")
    
    logger.debug(f"Completed call to insert_Saleloyalties method")


