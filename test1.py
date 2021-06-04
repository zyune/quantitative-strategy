import ccxt
import pandas as pd
from sqlalchemy import create_engine

apikey = ""
secretkey = ""
password=""

okex=ccxt.okex5({
    'apiKey':apikey,
    'secret':secretkey,
    'password':password,
    'enableRateLimit': True  
})
engine = create_engine('mysql+pymysql://root:The1isyou@localhost:3306/ohlcv_data?charset=utf8')#连接到本地数据库root:The1isyou@localhost:3306/ohlcv_data

#function that insert a row of BTC phlcv data into local database
def insert_phlcv_BTC():
    ohlcv=okex.fetch_ohlcv('BTC/USDT') 
    df = pd.DataFrame(ohlcv)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close','volume']
    df.tail(1).to_sql('bitcoin_data',engine,index=False,if_exists='append')
#function that insert a row of ETH phlcv data into local database
def insert_phlcv_ETH(): #最后一个_后面的币的名字改 用大写字母代表币种
    ohlcv=okex.fetch_ohlcv('ETH/USDT')  #这边改成 币名/USDT
    df = pd.DataFrame(ohlcv)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close','volume']
    df.tail(1).to_sql('eth_data',engine,index=False,if_exists='append') #数据库名 eth_data 小写
    
def insert_phlcv(arg): #传入参数 插入数据到指定币种数据库
    ohlcv=okex.fetch_ohlcv(arg)  #这边改成 币名/USDT
    df = pd.DataFrame(ohlcv)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close','volume']
    df.tail(1).to_sql('eth_data',engine,index=False,if_exists='append') #数据库名 eth_data 小写
    
insert_phlcv_ETH()
