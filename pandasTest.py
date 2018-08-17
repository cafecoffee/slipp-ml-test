#!/usr/bin/python
# -*- coding: ascii -*-

import pandas as pd
from pandas import Series, DataFrame
import numpy as np

# column_name : data
raw_data = {'first_name': ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'],
            'last_name': ['Miller', 'Jacobson', 'Ali', 'Mine', 'Cooze'],
            'age': [42, 52, 36, 24, 73],
            'city': ['San Francisco', ' Baltimore', 'Miami', 'Douglas', 'Boston']}

# print all column
dataFrame = DataFrame(raw_data, columns=['first_name', 'last_name', 'age', 'city'])
print(dataFrame)
print('-----------------------------------')

# select column
selectDataFrame = DataFrame(raw_data, columns=["age", "city"])
print(selectDataFrame.age * 2)
print(selectDataFrame["age"] > 60)
print('-----------------------------------')

# add new column
addDataFrame = DataFrame(raw_data, columns=['first_name', 'last_name', 'age', 'city', 'debt'])
addDataFrame['debt'] = addDataFrame.age * 2
print(addDataFrame)
print('-----------------------------------')

# select column - extract series
print(dataFrame.first_name)
print('-----------------------------------')

# select column - extract series 2
print(dataFrame["city"])
print('-----------------------------------')

# select column - extract series - get value
print(dataFrame["city"].get(0))
print('-----------------------------------')