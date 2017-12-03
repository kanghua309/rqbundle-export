import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime
from rqalpha.api import *

MAX_HISTORY_DAY=10000

def _save(stock,stockdf,conn):
    try:
        #print "save ----- :", stock,stockdf.head(10)
        stockdf = stockdf.sort_index(ascending=True)
        stockdf[['open', 'high', 'close', 'low', 'volume']].to_sql(stock, conn, if_exists='append')
    except Exception, arg:
        print "exceptions:", stock, arg





def init(context):
  stocks_info = all_instruments(type='CS')
  context.symbols_info = stocks_info['order_book_id']
  context.step = 0

def before_trading(context):
    pass

def handle_bar(context, bar_dict):
    if context.step == 0:
        conn = sqlite3.connect('History.db')
        cnt = 0
        for s in context.symbols_info:
            print cnt, s[:-5]
            x=  history_bars(s, 10000, '1d')
            #print type(x),np.shape(x)
            df = pd.DataFrame(x)
            df['datetime'] = df['datetime'].map(lambda x: pd.Timestamp(str(x)))
            #df['datetime'] = df['datetime'].astype('datetime64[ns]')
            y = df.set_index(['datetime'])
            y.index.rename('date',True)
            _save(s[:-5],y,conn)
            cnt += 1
            # if cnt == 10:
            #    break
        context.step += 1
        conn.close()
