def get_month_number(month_str):
    # Convert to lowercase to handle case-insensitive matches
    month_str = month_str.lower()

    # Month mapping (only first three characters needed)
    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
        "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    month_short = month_str[:3].lower()  # Take the first three characters and make it lowercase
    return month_mapping.get(month_short, 0)  # Return 0 if no match is found
