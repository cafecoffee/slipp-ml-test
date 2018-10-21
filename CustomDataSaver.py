import pandas_datareader.data as web
import datetime
import pandas as pd

import sqlite3
from CorporationStockInfo import get_stock_listed_corporation


code_df = get_stock_listed_corporation()
#print(code_df.head())
print(code_df)

def get_corporation_name(dataframe, column):
    return dataframe[column][0]


def get_url(item_name, dataframe):
    code = dataframe.query("회사명=='{}'".format(item_name))['종목코드'].to_string(index=False)
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    return url


def crawl_stock():
    url = get_url(get_corporation_name(code_df, '회사명'), code_df)
    df = pd.DataFrame()

    for page in range(1, 21):
        pg_url = '{url}&page={page}'.format(url=url, page=page)
        df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    df = df.dropna()
    df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open','고가': 'high', '저가': 'low', '거래량': 'volume'})

    df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['date'], ascending=True)
    df.set_index('date', inplace=True)
    return df

es_excepted_weekend = crawl_stock()

moving_average_10 = es_excepted_weekend['close'].rolling(window=10).mean()
moving_average_20 = es_excepted_weekend['close'].rolling(window=20).mean()
moving_average_60 = es_excepted_weekend['close'].rolling(window=60).mean()
moving_average_120 = es_excepted_weekend['close'].rolling(window=120).mean()

es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA10", moving_average_10)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA20", moving_average_20)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA60", moving_average_60)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA120", moving_average_120)

conn = sqlite3.connect("C:/Users/DD/PycharmProjects/slipp-ml-test/test1.db")

cursor = conn.cursor()
#cursor.execute("CREATE TABLE IF NOT EXISTS test(Date text, High int, Low int, Open int, Close int, Volume int, Adj_Close int)")
es_excepted_weekend.to_sql('test', conn, if_exists='replace')
