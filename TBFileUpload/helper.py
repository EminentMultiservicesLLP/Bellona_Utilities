def column_letter_to_index(s):
    # This process is similar to binary-to- 
    # decimal conversion 
    result = 0; 
    for B in range(len(s)): 
        result *= 26; 
        result += ord(s[B]) - ord('A') + 1; 
 
    return result; 