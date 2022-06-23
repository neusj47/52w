import pandas as pd
import quantstats as qs
import warnings
from datetime import datetime
warnings.filterwarnings(action='ignore')
import numpy as np
from pykrx import stock
import requests
from bs4 import BeautifulSoup
from io import BytesIO


start_date = '20190204'


df_pf = pd.read_excel('C:/Users/ysj/Desktop/ss.xlsx',sheet_name= 'Sheet1').sort_values('StdDate')
tickers = df_pf.Code.str.split('A').str[1].unique().tolist()


def get_adj_price(gubun, start_date, tickers) :
    df_prc = pd.DataFrame()
    for s in range(0, len(tickers)):
        cnt = round((datetime.today() - datetime.strptime(start_date, "%Y%m%d")).days * 25/30, 0)
        response = requests.get('https://fchart.stock.naver.com/sise.nhn?symbol={}&timeframe=day&count={}&requestType=0'.format(tickers[s],cnt))
        bs = BeautifulSoup(response.content, "html.parser")
        df_item = bs.select('item')
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = pd.DataFrame([], columns = columns, index = range(len(df_item)))
        for t in range(len(df_item)):
            df.iloc[t] = str(df_item[t]['data']).split('|')
            df['Date'].iloc[t] = datetime.strptime(df['Date'].iloc[t], "%Y%m%d")
            df['Close'].iloc[t] = int(df['Close'].iloc[t])
        df['52w_high'] = df['Close'].rolling(250).max()
        df['52w_low'] = df['Close'].rolling(250).min()
        if gubun == 'prc' :
            df_price_temp = pd.DataFrame(df[['Date', 'Close']].set_index('Date'))
            df_price_temp.columns = [tickers[s]]
        elif gubun == 'high' :
            df_price_temp = pd.DataFrame(df[['Date', '52w_high']].set_index('Date'))
            df_price_temp.columns = [tickers[s]]
        elif gubun == 'low' :
            df_price_temp = pd.DataFrame(df[['Date', '52w_low']].set_index('Date'))
            df_price_temp.columns = [tickers[s]]
        df_prc = pd.concat([df_prc, df_price_temp], axis=1)
    df_prc = df_prc.fillna(0).sort_index()
    return df_prc

df_prc = get_adj_price('prc', start_date, tickers)
df_52high = get_adj_price('high', start_date, tickers)
df_52low = get_adj_price('low', start_date, tickers)
