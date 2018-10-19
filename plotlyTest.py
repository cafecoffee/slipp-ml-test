# -*- coding: utf-8 -*-

import pandas as pd

import plotly.offline as offline
import plotly.graph_objs as go
from crawlingTest import get_stock_listed_corporation


code_df = get_stock_listed_corporation()
print(code_df.head())

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
    df = df.sort_values(by=['date'], ascending=False)
    return df

df = crawl_stock()

def draw_chart():
    trace = go.Scatter(x=df.date, y=df.close, name=get_corporation_name(code_df, '회사명'))
    data = [trace]
    layout = dict(title='{}의 거래량'.format(get_corporation_name(code_df, '회사명')),
                  xaxis=dict(
                      rangeselector=dict(
                          buttons=list([
                              dict(count=1, label='1m', step='month', stepmode='backward'),
                              dict(count=3, label='3m', step='month', stepmode='backward'),
                              dict(count=6, label='6m', step='month', stepmode='backward'),
                              dict(step='all')
                          ])
                      ),
                      rangeslider=dict(),
                      type='date'))
    fig = go.Figure(data=data, layout=layout)
    offline.plot(fig, filename='test.html')

draw_chart()
