import os
from datetime import datetime, timedelta
from collections import defaultdict
import RistaCalls.RistaAPICalls as API
import RistaToDB.
import json

def GetSalesPageforDay(params=None):
    data_sales_summary = API.GetData_RistaAPI(os.getenv("todays_salesData_url"), params)
    json.dumps(data_sales_summary, indent=4)

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
            invoice_data = {
                'InvoiceID': entry['invoiceNumber'],  # Simplified, map other fields similarly
                # Add other fields from record to invoice_data dictionary
            }
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



def InsertInvoiceDetails(entry):
    invoice_data = {
                'branchCode':entry['branchCode'],
                'InvoiceNumber':entry['InvoiceNumber'],
                'OrderNumber':entry['OrderNumber'],
                'StatementNumber':entry['StatementNumber'],
                'InvoiceDate':entry['InvoiceDate'],
                'InvoiceDay':entry['InvoiceDay'],
                'CreatedDate':entry['CreatedDate'],
                'ModifiedDate':entry['ModifiedDate'],
                'DeviceLabel':entry['DeviceLabel'],
                'InvoiceType':entry['InvoiceType'],
                'Status':entry['Status'],
                'FulfillmentStatus':entry['FulfillmentStatus'],
                'SourceOutletID':entry['SourceOutletID'],
                'OriginalSaleInvoiceNumber':entry['OriginalSaleInvoiceNumber'],
                'DeliveryMode':entry['DeliveryMode'],
                'Label':entry['Label'],
                'LabelGroup':entry['LabelGroup'],
                'SessionLabel':entry['SessionLabel'],
                'PersonCount':entry['PersonCount'],
                'SaleByUserID':entry['SaleByUserID'],
                'Channel':entry['Channel'],
                'Currency':entry['Currency'],
                'ItemCount':entry['ItemCount'],
                'ItemTotalAmount':entry['ItemTotalAmount'],
                'DirectChargeAmount':entry['DirectChargeAmount'],
                'ChargeAmount':entry['ChargeAmount'],
                'DiscountAmount':entry['DiscountAmount'],
                'TotalDiscountAmount':entry['TotalDiscountAmount'],
                'TaxAmountIncluded':entry['TaxAmountIncluded'],
                'TaxAmountExcluded':entry['TaxAmountExcluded'],
                'GrossAmount':entry['GrossAmount'],
                'NetDiscountAmount':entry['NetDiscountAmount'],
                'NetDirectChargeAmount':entry['NetDirectChargeAmount'],
                'NetAmount':entry['NetAmount'],
                'NetChargeAmount':entry['NetChargeAmount'],
                'BillAmount':entry['BillAmount'],
                'RoundOffAmount':entry['RoundOffAmount'],
                'BillRoundedAmount':entry['BillRoundedAmount'],
                'TipAmount':entry['TipAmount'],
                'TotalAmount':entry['TotalAmount'],
                'TotalCost':entry['TotalCost'],
                'Note':entry['Note'],
                'URL':entry['URL'],
                'StatusInfoReason':entry['StatusInfoReason'],
                'AccountingRoundOff':entry['AccountingRoundOff'],
                'LastKey':entry['LastKey']
            }
    