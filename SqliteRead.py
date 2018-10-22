import sys
import time
from pandas import DataFrame
import datetime
import sqlite3
import os
import pandas_datareader.data as web
import pandas as pd
import numpy as np
import requests

from io import BytesIO

class SqliteRead:
    def __init__(self):
        print("init")

    def run(self):
        print("run")
        con = sqlite3.connect(os.getcwd() + "/stock.db")
        df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", con)
        print(df)

        for i, code in enumerate(df['name']):
            df2 = pd.read_sql("SELECT * FROM '"+code+"'", con)
            print(df2)



if __name__ == "__main__":
    sqlRead = SqliteRead()
    sqlRead.run()

