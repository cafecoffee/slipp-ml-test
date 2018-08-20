import pandas as pd
import numpy as np
import requests

from io import BytesIO

# 상장법인 목록 얻기
def get_stock_listed_corporation():
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do'
    #파싱할 데이터, 소스에서 확인
    data = {
        'method': 'download',
        'orderMode': '1',
        'orderStat': 'D',
        'searchType': '13',
        'fiscalYearEnd': 'all',
        'location': 'all'
    }

    dataframe = pd.read_html(BytesIO(requests.post(url, data=data).content), header=0, parse_dates=['상장일'])[0]

    dataframe['종목코드'] = dataframe['종목코드'].astype(np.str)  #타입 변환
    dataframe['종목코드'] = dataframe['종목코드'].str.zfill(6)    #빈자리 0으로 채움

    return dataframe

dataframe = get_stock_listed_corporation()
dataframe = dataframe.loc[:, ['회사명', '종목코드', '업종', '상장일', '결산월']]

print(dataframe.head())