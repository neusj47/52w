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
from dateutil.relativedelta import relativedelta


stddate = '20220624'
start_date = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=60),"%Y%m%d"))


def get_kospi_code(mkt, stddate):
    if mkt == '코스피' : ind = '001'
    elif mkt == '코스피50' : ind = '035'
    elif mkt == '코스피100' : ind = '034'
    elif mkt == '코스피200' : ind = '028'
    elif mkt == '코스피100200' : ind = '167'
    query_str_parms = {
    'locale': 'ko_KR',
    'tboxindIdx_finder_equidx0_2': '',
    'indIdx': '1',
    'indIdx2': ind,
    'codeNmindIdx_finder_equidx0_2': '',
    'param1indIdx_finder_equidx0_2': '',
    'trdDd': stddate,
    'money': 3,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
    }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
    }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = 'A'+str(df.종목코드[i]).zfill(6)
    return df

def get_kosdaq_code(mkt, stddate):
    if mkt == '코스닥' : ind = '001'
    elif mkt == '코스피150' : ind = '203'
    elif mkt == '코스닥대형주' : ind = '002'
    query_str_parms = {
    'locale': 'ko_KR',
    'tboxindIdx_finder_equidx0_2': '',
    'indIdx': '2',
    'indIdx2': ind,
    'codeNmindIdx_finder_equidx0_2': '',
    'param1indIdx_finder_equidx0_2': '',
    'trdDd': stddate,
    'money': 3,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
    }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
    }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = 'A'+str(df.종목코드[i]).zfill(6)
    return df

# df = get_kospi_code('코스피100200', stddate)
df = get_kosdaq_code('코스닥대형주', stddate)
tickers = df.종목코드.str.split('A').str[1].unique().tolist()


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

def get_52w_pf(start_date, tickers) :
    df_prc = get_adj_price('prc', start_date, tickers)
    df_52high = get_adj_price('high', start_date, tickers)
    df_temp = ((df_prc >= df_52high) & (df_prc != 0) & (df_52high != 0)).applymap(lambda x : 1 if x else 0)[249:len(df_prc)]
    df_temp = df_temp.reset_index()
    df_temp['Month'] = df_temp['Date'].map(lambda x : datetime.strftime(x, '%Y-%m'))
    df_temp = df_temp.set_index('Date')
    df_sig = df_temp.groupby('Month').sum()
    df_sig_t = df_sig.T
    col = df_sig_t.columns.tolist()
    df_pf = pd.DataFrame()
    df_pf['종목코드'] = df_sig_t.loc[df_sig_t[col[0]] != 0].index.tolist()
    df_pf['일자'] = col[0]
    df_pf['편입영업일'] = datetime.strptime(stock.get_nearest_business_day_in_a_week((datetime.strftime(datetime.strptime(df_pf['일자'].iloc[0] + "-05", "%Y-%m-%d")+ relativedelta(months=1),"%Y%m%d"))),"%Y%m%d")
    for i in range(1,len(col)-1) :
        df_pf_temp = pd.DataFrame()
        try:
            df_pf_temp['종목코드'] = df_sig_t.loc[df_sig_t[col[i]] != 0].index.tolist()
            df_pf_temp['일자'] = col[i]
            df_pf_temp['편입영업일'] = datetime.strptime(stock.get_nearest_business_day_in_a_week((datetime.strftime(datetime.strptime(df_pf_temp['일자'].iloc[0] + "-05", "%Y-%m-%d") + relativedelta(months=1), "%Y%m%d"))),"%Y%m%d")
        except:
            df_pf_temp = pd.DataFrame({'종목코드': ['226490'],
                                       '일자': [col[i]],
                                       '편입영업일': [datetime.strptime(stock.get_nearest_business_day_in_a_week((datetime.strftime(datetime.strptime(col[i] + "-05","%Y-%m-%d") + relativedelta(months=1),"%Y%m%d"))),"%Y%m%d")]})
        df_pf = pd.concat([df_pf, df_pf_temp])
    df_pf['종목명'] = ''
    for i in range(0,len(df_pf)) :
        try :
            df_pf['종목명'].iloc[i] = stock.get_market_ticker_name(df_pf['종목코드'].iloc[i])
        except :
            df_pf['종목명'].iloc[i] = '코스피'
    df_pf['종목코드'] = 'A' + df_pf['종목코드']
    return df_pf

df_pf = get_52w_pf(start_date, tickers)


df_pf.to_excel('C:/Users/ysj/Desktop/pffaㄴs.xlsx')
