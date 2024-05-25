#!/usr/bin/env python3

import sys

# Reducer to return global top 10 diamonds by price

# Initialize a list to store the top N records as a collection of tuples (price, record)
myList = []
n = 10  # Number of top N records

for line in sys.stdin:
    # Remove leading and trailing whitespace
    line = line.strip()
    # Split data values into list
    data = line.split(",")  

    # Ensure the line has enough columns
    if len(data) < 7:  
        continue

    # Convert price (currently a string) to int
    try:
        price = int(data[6])  # 'price' is the 7th column (index 6)
    except ValueError:
        continue

    # Add (price, record) tuple to list
    myList.append((price, line))
    # Sort list in reverse order
    myList.sort(reverse=True)

    # Keep only first N records
    if len(myList) > n:
        myList = myList[:n]

# Print top N records
for (k, v) in myList:
    print(v)
