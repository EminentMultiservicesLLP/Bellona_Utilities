import pandas as pd
from datetime import datetime, timedelta

# Generate data for the financial year 2023-24
def generate_financial_year_data(start_date, end_date):
    data = []
    current_date = start_date
    week_number = 1
    week_no_label_A = 1  # Week number for "WEEK 01" format
    week_no_label = 1  # Week number for "WEEK 01" format
    day_no = 1

    while current_date <= end_date:
        week_day = current_date.strftime('%A').upper()

        # Increment week numbers on Monday
        if week_day == 'MONDAY' and current_date != start_date:
            week_no_label += 1

        week_label = f'WEEK {week_no_label:02d}'
        week_no_label_A = f'{week_label}/2324'
        period = f'PERIOD 01/2324'
        dates = current_date.strftime('%d').lstrip('0') + current_date.strftime('%d').lstrip('0')[-1] + ' ' + current_date.strftime('%b').upper() + ' ' + current_date.strftime('%Y')
        date_str = current_date.strftime('%Y-%m-%d')
        financial_year = '2023-24'
        year = current_date.year
        month = current_date.month

        data.append({
            "DateRangeId": len(data) + 1,
            "Period": period,
            "Week": week_no_label_A,
            "Dates": dates,
            "Days": week_day,
            "Date": date_str,
            "FinancialYear": financial_year,
            "Year": year,
            "Month": month,
            "WeekNo": week_label,
            "DayNo": day_no,
            "IsCurrentYear": 1 if year == 2023 else 0,
            "Week_number": week_no_label
        })

        # Increment day number and move to the next day
        day_no += 1
        current_date += timedelta(days=1)

    return pd.DataFrame(data)

# Define start and end dates
start_date = datetime(2023, 4, 1)
end_date = datetime(2024, 3, 31)

# Generate data
data = generate_financial_year_data(start_date, end_date)

# Save to a CSV file
data.to_csv("financial_year_2023_24.csv", index=False)

print("Data for financial year 2023-24 has been generated and saved to 'financial_year_2023_24.csv'.")
