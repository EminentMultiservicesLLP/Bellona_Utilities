import os
from dotenv import load_dotenv 
import DBOperations
from datetime import datetime, timedelta
from RistaCalls import BranchResource
from RistaCalls.DatabaseManagerSingleton import DatabaseSingleton
import RistaCalls.SalesDetails as sales
import RistaCalls.BranchDetails as branch
from Logging_config import setup_logging

import RistaCalls.TestAPIData as test

load_dotenv() 
# Initialize the logger
logger = setup_logging()

# Main function to run the steps
def main():
    logger.debug("Main Function execution starts")

    try:
        if os.getenv('run_for_test') == '1':
            params = {
            'branch': 'bnkrisha',
                'day': '2024-08-18',
                'dayInclusivePayment': True
            }
            test.GetTestAPIData(params)

            return

        env_branches = os.getenv("branch_data_to_pull")
        if env_branches != None and len(env_branches) > 0 and len(env_branches.split(',')) > 0 :
            branches = env_branches.split(',')
        else:
            branches = branch.GetBranchDetails()
            branches = [branch["branchCode"] for branch in branches]
            
        #outlet_list = DBOperations.Get_OuletList()

        date_format="%Y-%m-%d"
        date_to_pull = os.getenv("date_to_pull")
        if date_to_pull != None and len(date_to_pull)>0 and datetime.strptime(date_to_pull, date_format):
            today= datetime.strptime(date_to_pull, date_format)
        else:
            today = datetime.today() # Get today's date

        try:
            no_of_days  = int(os.getenv("data_pull_number_of_days"))
        except:
            no_of_days =1

        # Getting the singleton instance
        db_singleton = DatabaseSingleton()
        cursor = db_singleton.get_cursor()
        DBOperations.set_cursor(cursor)

        for i in range(no_of_days):
            day = today - timedelta(days=i+1)
            formatted_date = day.strftime('%Y-%m-%d')
            branch_count = 1;
            for outlet in branches:

                try:
                    
                    # #Get Resource details
                    # params = {'branch': outlet }
                    # logger.warning(f"Branch Resource Data import starting for parameter {params}")
                    # BranchResource.GetBranchResource(params)
                    # logger.warning(f"Branch Resource Data imported for parameter {params} completed successfully")

                    if 1==1:
                    #if outlet.OutletCode in {branch["branchCode"] for branch in branches}:  #commented on 21-Aug
                        params = {
                            #'branch': outlet.branchCode, #commented on 21-Aug
                            'branch': outlet,
                            'day': formatted_date,
                            'dayInclusivePayment': True
                        }
                        #data_sales_summary = fetch_data(os.getenv("sales_summary_url"), params)
                        logger.warning(f"Data import started for {branch_count} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} for parameter {params}")
                        sales.GetSalesPageforDay(params)
                        logger.warning(f"Data importe completed at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} for parameter {params} successfully")
                        branch_count = branch_count+1
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}", exc_info=True)
                    raise  # Re-raise the exception after logging    
                        
    finally:    
        db_singleton.close_connection()
        logger.warning(f"All data imported for day {formatted_date} completed successfully")
# Ensure the script runs when executed
if __name__ == "__main__":
    logger.debug("Main call started")
    main()


# Salefood = sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] if account['account'] == 'Food Sale' for channel in account['channels'])
# SaleBeverage = sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] if account['account'] == 'Beverage Sale' for channel in account['channels'])
# SaleLiquor = sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] if account['account'] == 'Liquor Sale' for channel in account['channels'])
# SaleWine = sum(channel['amount'] for account in data_sales_summary['accountsWiseChannels'] if account['account'] == 'Wine Sale' for channel in account['channels'])


#upsert_data_to_mssql(mapped_data, table_name, conn_params, id_column)

# insert_sql = f"""INSERT INTO dbo.Transaction_Daily_Sales 
#             (OutletID, DSREntryDate, SaleLunchDinein, SaleEveningDinein, SaleDinnerDinein,/* TotalSaleDinein, */
#             SaleTakeAway, SaleFood, SaleBeverage, SaleWine, SaleBeer, SaleLiquor, SaleTobacco, SaleOther, 
#             ItemsPerBill, TotalNoOfBills, CTCSalary, ServiceCharge, GuestCountLunch, GuestCountEvening, GuestCountDinner, IsActive,CashCollected,CashStatus)
#             SELECT """
# value_sql = f"""{outlet.OutletID},'{formatted_date}',0,0,{SaleDinnerDinein},
#             {salesTakeaway}, {Salefood}, {SaleBeverage},{SaleWine}, {saleBeer}, {SaleLiquor}, {SaleTobacco}, 0,
#             0,{TotalNoOfBills},0,0,0,0,{GuestCountDinner},1,{total_gross_amount},0"""

