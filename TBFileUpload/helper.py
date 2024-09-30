def column_letter_to_index(s):
    # This process is similar to binary-to- 
    # decimal conversion 
    result = 0; 
    for B in range(len(s)): 
        result *= 26; 
        result += ord(s[B]) - ord('A') + 1; 
 
    return result; 


def index_to_column_letter(index):
    column_name = ''
    while index > 0:
        index, remainder = divmod(index-1, 26)
        column_name = chr(65 + remainder) + column_name
    return column_name

def try_parse(value, data_type):
    try:
        # Try to convert the value to the given data type (int, float, etc.)
        data_type(value)
        return True
    except (ValueError, TypeError):
        # Return False if conversion fails
        return False
    