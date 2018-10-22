import os
import sqlite3
from io import BytesIO

import numpy as np
import pandas as pd
import pandas_datareader.data as web
import requests
from pandas_datareader._utils import RemoteDataError


class SlippStockCrawler:
    def __init__(self):
        print("init")

    # 상장법인 목록 얻기
    @staticmethod
    def get_stock_code_list():
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do'
        # 파싱할 데이터, 소스에서 확인
        data = {
            'method': 'download',
            'orderMode': '1',
            'orderStat': 'D',
            'searchType': '13',
            'fiscalYearEnd': 'all',
            'location': 'all'
        }
        dataframe = pd.read_html(BytesIO(requests.post(url, data=data).content), header=0, parse_dates=['상장일'])[0]
        dataframe['종목코드'] = dataframe['종목코드'].astype(np.str)  # 타입 변환
        dataframe['종목코드'] = dataframe['종목코드'].str.zfill(6)  # 빈자리 0으로 채움
        dataframe = dataframe.loc[:, ['종목코드']]

        return dataframe['종목코드']

    @staticmethod
    def get_sqlite_connection():
        # sqlite 현재 폴더 stock.db 연결
        return sqlite3.connect(os.getcwd() + "/stock.db")

    def run(self):

        # codes = self.get_stock_code_list()
        codes = ['000400', '051500']
        num = len(codes)

        con = self.get_sqlite_connection()

        for i, code in enumerate(codes):
            try:
                print(i, '/', num, ':', code)
                df = self.get_available_stock(code)
                self.append_moving_average(df, 10)
                self.append_moving_average(df, 20)
                self.append_moving_average(df, 60)
                self.append_moving_average(df, 120)
                print(df)
                df.to_sql(code, con, if_exists='replace')
            except RemoteDataError as err:
                print(err)

    @staticmethod
    def get_available_stock(code):
        # 종목 선택
        gs = web.DataReader(code + ".KS", "yahoo")
        gs[["High", "Low", "Open", "Close", "Volume", "Adj Close"]] = gs[
            ["High", "Low", "Open", "Close", "Volume", "Adj Close"]].round()
        print(gs)
        # 주말에는 장이 열리지 않으므로 제거
        return gs[gs['Volume'] != 0]

    @staticmethod
    def append_moving_average(df, days):
        ma = df['Adj Close'].rolling(window=days).mean().round(0)
        df.insert(len(df.columns), "MA" + str(days), ma)

        return df


if __name__ == "__main__":
    crawler = SlippStockCrawler()
    crawler.run()
