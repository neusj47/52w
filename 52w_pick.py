import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings( 'ignore' )
import numpy as np
from pykrx import stock
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta



# 0. 데이터 가져오기
df_input = pd.read_csv('C:/Users/ysj/Desktop/52w_signal.csv', encoding = 'CP949')
code = df_input.columns[1:len(df_input.columns)].tolist()
# name = df_input.iloc[1][1:len(df_input.columns)].tolist()
df_input = df_input.iloc[5:len(df_input)].reset_index(drop=True)

# 1. 52주 신고가 종목 선별하기
df_input['mon'] = df_input.Symbol.map(lambda x: datetime.strftime(datetime.strptime(x, '%Y-%m-%d'),'%Y-%m'))
df_input = df_input[df_input.columns[1:].tolist()]
df = df_input.groupby('mon').sum()
dfs = df[13:len(df)]
dft = dfs.T
col = dft.columns.tolist()

df_pf = pd.DataFrame()
df_pf['종목코드'] = dft.loc[dft[col[0]] != 0].index.tolist()
df_pf['일자'] = col[0]
df_pf['편입영업일'] = datetime.strptime(stock.get_nearest_business_day_in_a_week((datetime.strftime(datetime.strptime(df_pf['일자'].iloc[0] + "-05", "%Y-%m-%d")+ relativedelta(months=1),"%Y%m%d"))),"%Y%m%d")

for i in range(1,len(col)) :
     df_pf_temp = pd.DataFrame()
     df_pf_temp['종목코드'] = dft.loc[dft[col[i]] != 0].index.tolist()
     df_pf_temp['일자'] = col[i]
     df_pf_temp['편입영업일'] = datetime.strptime(stock.get_nearest_business_day_in_a_week((datetime.strftime(datetime.strptime(df_pf_temp['일자'].iloc[0] + "-05", "%Y-%m-%d")+ relativedelta(months=1),"%Y%m%d"))),"%Y%m%d")
     df_pf = pd.concat([df_pf, df_pf_temp])

# 3. 데이터 출력
df_pf.to_excel('C:/Users/ysj/Desktop/52주종목s.xlsx')