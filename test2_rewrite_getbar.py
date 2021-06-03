from fmz import *
from sqlalchemy import create_engine
import pandas as pd
"""
这里的方法可能 且极有可能只适用于fre 大于等于1s的交易
This function is adapt from get_bars in Library fmz.yz
It only contain 1000 values
and  it will return  a dataframe with timestamp in millisecond instead of datetime 
and instead of using DatetimeIndex
Author Yune 
"""
def get_bars_with_timestamp_13(symbol, unit='1d', start=None, end=None, count=1000):
    if hasattr(unit, 'endswith'):
        if unit.endswith('d'):
            unit = int(unit[:-1]) * 1440
        elif unit.endswith('h'):
            unit = int(unit[:-1]) * 60
        elif unit.endswith('m'):
            unit = int(unit[:-1])
    ts_to = int(time.time())
    if end is not None:
        end = end.replace('/', '-')
        ts_to = int(time.mktime(datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S" if ' ' in end else "%Y-%m-%d").timetuple()))
    if start is not None:
        start = start.replace('/', '-')
        ts_from = int(time.mktime(datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S" if ' ' in start else "%Y-%m-%d").timetuple()))
        if end is None:
            ts_to = ts_from+(unit*100*(count+10))
    else:
        if end is None:
            ts_from = 0
            ts_to = 0
        else:
            ts_from = ts_to-(unit*100*(count+10))
    params = {"symbol": symbol, "resolution": unit, "from": ts_from, "to": ts_to, "size": count}
    data = json.loads(httpGet("http://"+ CLUSTER_IP + "/chart/history?"+urlencode(params), CLUSTER_DOMAIN))
    try:
        import pandas as pd
        from pandas.plotting import register_matplotlib_converters
        register_matplotlib_converters()
    except:
        return data
    index = []
    for ele in data:
        ele[0]=ele[0]*1000
        
    columns=["timestamp","open", "high", "low", "close", "volume"]
    if len(data) > 0 and len(data[0]) == 7:
        columns.append("openInterest")
    return pd.DataFrame(data,columns=columns)

"""
Mr Chang wrote this function ,
however I doute its Functional performance like time cost and memory cost
"""
def get_history_data(fre,ex):
    frame = pd.DataFrame()
    sta=datetime.datetime(2017, 1, 7)
    end = datetime.datetime.now()
    while  end - sta > datetime.timedelta(days=1):
        number = fre[0]
        if fre[-1] == 'm':
            endate= datetime.timedelta(minutes = 1000 * int(number)) + sta
        else:
            endate= datetime.timedelta(hours = 1000 * int(number)) + sta

        start = str(pd.to_datetime(sta))[:19]
        sta = endate
        endate2= str(pd.to_datetime(endate))[:19]
        df = get_bars_with_timestamp_13(ex, fre, start, endate2)
        frame = frame.append(df)
    return frame

engine = create_engine('mysql+pymysql://root:The1isyou@localhost:3306/ohlcv_data?charset=utf8')#连接到本地数据库root:The1isyou@localhost:3306/ohlcv_data

"""
!!!Be careful about using following functions.
It takes about 15-30 mintues to override the whole database 
Author Yune
"""
#This is the function for override BIT databse. the frequency will be 1 minute 
# There will be about 2000000 rows 
# Author Yune
def override_history_phlcv_BIT_databse():
    BIT_history_data=get_history_data('1m','binance.btc_usdt')
    BIT_history_data.to_sql('bitcoin_data',engine,index=False,if_exists='replace')

#This is the function for override ETH databse. the frequency will be 1 minute 
# Author Yune
def override_history_phlcv_ETH_databse():
    BIT_history_data=get_history_data('1m','binance.eth_usdt')
    BIT_history_data.to_sql('eth_data',engine,index=False,if_exists='replace')