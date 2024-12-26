import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from fpdf import FPDF
import fitz  #PyMuPDF
from pdf2image import convert_from_path
import pywhatkit as kit
import schedule
import time
import os

# Database configuration
db_config = {
    "server": "103.93.17.106",
    "database": "BELLONA_LIVE",
    "username": "sa",
    "password": "Ja!sa!nath@12",
    "procedure": "dbsp_GetMISDailyUpdate"
}

# WhatsApp contact list
whatsapp_contacts = ["DMqEWkv9MhX8x3Qje7Vbmf"] # "LHdtyMvCqACEPrTyACUHnj"

# Output directory
output_dir = "pdf_reports"
os.makedirs(output_dir, exist_ok=True)

custom_colors = {
    'yellow': '#FFFF00',
    'red': '#FF0000',
    'blue': '#0000FF',
    'green': '#00FF00',
    'orange': '#FFA500',
    # Add more colors as needed
}

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        # Set no margin for header and footer
        self.set_auto_page_break(auto=True, margin=0)  # Disable footer margin
        self.set_margins(left=5, top=5, right=5)     # Adjust left, top, and right margins
    
    # def header(self):
    #     # Override header method to do nothing
    #     pass
    
    def header(self):
        self.set_font("Arial", "B", 8)
        yesterday = datetime.now() - timedelta(days=1)
        day_suffix = get_day_suffix(yesterday.day)
        formatted_date = yesterday.strftime(f"%d{day_suffix} %B %Y (%A)")
        
        self.cell(0, 5, f"Daily Sales Report - {formatted_date}", 0, 1, "C")
        self.ln()

    def footer(self):
        self.set_y(-5)
        self.set_font("Arial", "I", 8)
        self.cell(0, 2, f"Page {self.page_no()} of {{nb}}", 0, 0, "C")

    def add_table(self, data):
        self.set_font("Arial", size=6)

        # Table Headers
        headers = ['C_Cover','C_APC','C_Net (lacs)','C_Gross (lacs)','M_Cover','M_APC','M_Net (lacs)','M_Gross (lacs)','GrossBudget','% achieved']
        col_widths = [12, 12, 13, 15, 12, 12, 13, 15, 16.5, 15 ]

        #set all decimal value to 2 digit
        for inx, col in enumerate(headers[0:]):
            if 'cover' in str(col).lower():
                data[col] = data[col].astype(int).round(0)
            else:
                data[col] = data[col].astype(float).round(2)
        
        # Filter and format data
        filtered_data = data[(data["Outlet"].str.lower() == "cluster total") & (data["Cluster"].str.lower() == "total")]
        if not filtered_data.empty:
            grand_total_row = filtered_data.iloc[0]
        else:
            print("No matching rows found.")

        outletCol_widths=45
        font_size = 6
        row_height=4
        empty_row_height =2
        
        # Add first headers column
        self.cell(outletCol_widths,row_height,"Cluster/Outlet",1,0,'C')

        # Add Second headers column
        yesterday = datetime.now() - timedelta(days=1)
        day_suffix = get_day_suffix(yesterday.day)
        formatted_date = yesterday.strftime(f"%d{day_suffix} %B %Y (%A)")
        self.cell(sum(col_widths[:4]),row_height,formatted_date,1,0,'C')

        # Add Thirdcond headers column
        formatted_date = datetime.now().strftime(f"MTD %B")
        self.cell(sum(col_widths[4:8]),row_height,formatted_date,1,0,'C')

        # Add Last headers column
        self.cell(sum(col_widths[8:10]),row_height,"Budget for Month",1,0,'C')
        self.ln()

        # Group and sort data
        cluster_totals = data[(data["Outlet"].str.lower() == "cluster total") & (data["Cluster"].str.lower() != "total")]
        outlet_details = data[data["Outlet"].str.lower() != "cluster total"]

        # Add subtotal rows and corresponding outlet details
        for _, cluster in cluster_totals.iterrows():
            self.set_font("Arial", "B", font_size)

            #first column to show Cluster and Outlet Grouping
            self.set_fill_color(255, 255, 0) # Set the fill color to yellow            
            self.cell(outletCol_widths, row_height + row_height, str(cluster["Cluster"]), 1,0,'C', fill=True)
            
            #Next four fields to show Sales data for YEsterday
            self.set_fill_color(0, 0, 255) #setup header color as blue (2nd to 5th field)
            self.set_text_color(255, 255, 255)   #set text color white because filling with Blue background color
            for idx, col in enumerate(headers[0:4],0): #first 4 columns/fields in headers are for Sales
                self.cell(col_widths[idx], row_height, str(col).replace("C_", ""), 1,0,'C', fill=True)

            #Next four fields to show MTD data
            self.set_fill_color(191, 255, 0) #setup header color as blue (6th to 10th field)
            self.set_text_color(0, 0, 0)   #set text color white because filling with Blue background color
            for idx, col in enumerate(headers[4:8],4):  #field 5th to next 4 columns/fields in headers are for MTD
                self.cell(col_widths[idx], row_height, str(col).replace("M_", ""), 1,0,'C', fill=True)

            #Last 2 fields of Budget
            self.set_fill_color(0, 255, 239) #setup header color as tutquiose
            for idx, col in enumerate(headers[8:], 8):  # Last two fields belongs to Budget
                self.cell(col_widths[idx], row_height, str(col), 1,0,'C', fill=True)
            self.ln()

            self.set_text_color(0, 0, 0)  #set text black color
            # Subtotal Row for Yesterdays Sales
            self.cell(outletCol_widths, 0, "", 0)
            self.set_fill_color(166, 166, 166)
            for idx, col in enumerate(headers[0:]):  # Start from the 3rd column
                self.cell(col_widths[idx], row_height, str(cluster[col]), 1,0,'C',fill=True)
            self.ln()

            # Add outlet details for this cluster
            self.set_font("Arial", size=font_size)
            outlets = outlet_details[outlet_details["Cluster"] == cluster["Cluster"]]
            for _, outlet in outlets.iterrows():
                self.cell(outletCol_widths, row_height, str(outlet["Outlet"]), 1,0,'C')  # Second column
                for idx, col in enumerate(headers[0:]):  # Start from the 3rd column
                    self.cell(col_widths[idx], row_height, str(outlet[col]), 1,0,'C')
                self.ln()

            # Add empty line after each cluster group
            self.cell(outletCol_widths, empty_row_height, "", 1,0,'C')
            for idx, header in enumerate(headers[0:]):
                self.cell(col_widths[idx], empty_row_height, "", 1,0,'C')
            self.ln()

        # Add grand total
        self.ln(empty_row_height)  # Add a line space before grand total
        self.set_font("Arial", "B", font_size)
        self.cell(outletCol_widths, row_height, "Grand Total", 1,0,'C')
        #self.cell(col_widths[1], row_height, str(grand_total_row["Outlet"]), 1,0,'C')
        for idx, col in enumerate(headers[0:]):  # Start from the 3rd column
            self.cell(col_widths[idx], row_height, str(grand_total_row[col]), 1,0,'C')
        self.ln()

# Function to determine the day suffix
def get_day_suffix(day):
    if 11 <= day <= 13:
        return "th"
    elif day % 10 == 1:
        return "st"
    elif day % 10 == 2:
        return "nd"
    elif day % 10 == 3:
        return "rd"
    else:
        return "th"
    
def fetch_data():
    connection = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={db_config['server']};DATABASE={db_config['database']};"
        f"UID={db_config['username']};PWD={db_config['password']}"
    )
    cur = connection.cursor()
    cur.execute("EXEC dbo.dbsp_GetMISDailyUpdate;")
    rows = cur.fetchall()
    cols= [desc[0] for desc in cur.description]

    list_result =np.array(rows)
    df = pd.DataFrame(list_result, columns=cols)

    cur.close()
    connection.close()
    return df

def create_pdf(data):
    """Generate a PDF report in the required format."""
    file_name = os.path.join(output_dir, f"Daily_Sales_Report_{datetime.now().strftime('%Y%m%d')}.pdf")
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    # Generate the table
    pdf.add_table(data)
    pdf.output(file_name)
    print(f"PDF generated: {file_name}")
    return file_name

def send_pdf_to_whatsapp(file_name):
    output_folder="temp_images"
    doc = fitz.open(file_name)  # Open the PDF
    for page_number in range(len(doc)):
        page = doc[page_number]
        
        zoom = 2 # 250% zoom 
        mat = fitz.Matrix(zoom, zoom) 

        pix = page.get_pixmap(matrix=mat,dpi=300)  # Render page to an image
        image_path = f"{output_folder}/page_{page_number + 1}.png"
        pix.save(image_path)  # Save image
        print(f"Saved: {image_path}")

    yesterday = datetime.now() - timedelta(days=1)
    day_suffix = get_day_suffix(yesterday.day)
    formatted_date = yesterday.strftime(f"%d{day_suffix} %B %Y (%A)")
    """Send a PDF file via WhatsApp."""
    for contact in whatsapp_contacts:
        kit.sendwhats_image(contact, image_path, caption=f"Daily Sales Report for {formatted_date}",wait_time=10, tab_close=False, )
        print(f"Sent report to {contact}")

def send_daily_report():
    """Fetch data, create a PDF, and send it via WhatsApp."""
    try:
        print("Generating daily sales report...")
        data = fetch_data()
        file_name = create_pdf(data)
        send_pdf_to_whatsapp(file_name)
    except Exception as e:
        print(f"Error: {e}")

# # Schedule the task for 6:30 AM daily
# schedule.every().day.at("06:30").do(send_daily_report)

# print("Scheduler is running...")
# while True:
#     schedule.run_pending()
#     time.sleep(1)
send_daily_report()