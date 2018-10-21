import pandas_datareader.data as web
import datetime
import sqlite3

#수집 일수 설정
start = datetime.datetime(2016, 1, 1)
end = datetime.datetime(2018, 10, 20)
#종목 선택
gs = web.DataReader("078930.KS", "yahoo", start, end)
gs = gs.rename(columns={"High": "고가", "Low": "저가", "Open": "시가", "Close": "종가", "Volume": "거래량", "Adj Close": "수정 종가"})
#주말에는 장이 열리지 않으므로 제거
es_excepted_weekend = gs[gs['거래량'] != 0]

moving_average_10 = es_excepted_weekend['수정 종가'].rolling(window=10).mean()
moving_average_20 = es_excepted_weekend['수정 종가'].rolling(window=20).mean()
moving_average_60 = es_excepted_weekend['수정 종가'].rolling(window=60).mean()
moving_average_120 = es_excepted_weekend['수정 종가'].rolling(window=120).mean()

es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA10", moving_average_10)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA20", moving_average_20)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA60", moving_average_60)
es_excepted_weekend.insert(len(es_excepted_weekend.columns), "MA120", moving_average_120)

conn = sqlite3.connect("C:/Users/DD/PycharmProjects/slipp-ml-test/test.db")

cursor = conn.cursor()
#cursor.execute("CREATE TABLE IF NOT EXISTS test(Date text, High int, Low int, Open int, Close int, Volume int, Adj_Close int)")
es_excepted_weekend.to_sql('test', conn, if_exists='replace')
