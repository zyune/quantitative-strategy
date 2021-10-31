import ccxt
import pandas as pd
import numpy as np
import time
import datetime
import temple.strategy_temple as st

# -------------------------------------------------
h_window = 3
s_window = 8

account = 15000
price = 2300
multi = 10
# real
# api = '2006cea3-f890-40f6-b5c0-28053e38e49e'
# secret = '95BA05578C7E401D7A7B6D1F9F91ECA7'

api = '321e133f-9642-4383-9c22-57334f567807'
secret = 'E82F25CC0663B8218F182766E0B206AD'
# symbol1 = 'BTC/USDT'
# symbol2 = 'BTC-USDT-SWAP'
symbol1 = 'ETH/USDT'
symbol2 = 'ETH-USDT-SWAP'
# --------------------------------------------------
trade = ccxt.okex5({
    'apiKey': api,
    'secret': secret,
    'password': 'zjs980426',
    'demo': '1'
})

pos = account/price*multi

print('初始化完成')
while True:
    # try:
    # 仓位管理
    order_waiting = trade.fetch_open_orders(symbol2)
    position_waiting = 0
    if len(order_waiting) > 0:
        for i in order_waiting:
            print('position waiting: '+str(i['remaining']))

            if i['side'] == 'sell':
                position_waiting -= int(i['remaining'])
            else:
                position_waiting += int(i['remaining'])

    # 因子计算
    hour_price = pd.DataFrame(trade.fetch_ohlcv(symbol1, '1m', limit=max(h_window, s_window) + 1))
    hour_price[0] = pd.to_datetime(hour_price[0] * 1000000)
    hour_price['decide'] = (hour_price[2] + hour_price[3] + hour_price[4]) / 3


    def wma(series):
        t = len(series)
        w_arrray = 2 * (np.arange(t) + 1) / (t * (t + 1))
        return sum(w_arrray * series)


    h1 = hour_price['decide'].rolling(int(h_window / 2)).apply(wma)
    h2 = hour_price['decide'].rolling(h_window).apply(wma)
    h3 = 2 * h1 - h2
    hour_price['hma'] = h3.rolling(round(np.sqrt(h_window))).apply(wma)
    hour_price['sma'] = hour_price['decide'].rolling(s_window).mean()
    d1 = hour_price['decide'].iloc[-2]
    d2 = hour_price['sma'].iloc[-2]
    d3 = hour_price['hma'].iloc[-2]
    d4 = hour_price['hma'].iloc[-3]

    long_price = trade.fetch_ticker(symbol2)['close'] * 0.9998
    short_price = trade.fetch_ticker(symbol2)['close'] * 1.0002


    # 开仓平仓
    def modify_order(gap_time):
        try:
            orders = trade.private_get_trade_orders_pending()['data']
            for i in orders:
                symbol_to_amend = i['instId']
                side = i['side']
                total = i['sz']
                finished = i['accFillSz']
                size = int(total) - int(finished)
                open_time = i['uTime']
                gap = time.time() - int(open_time)/1000
                order_id = i['ordId']
                if gap > gap_time:
                    if side == 'buy':
                        price = trade.fetch_ticker(symbol_to_amend)['close'] * 0.9998
                    else:
                        price = trade.fetch_ticker(symbol_to_amend)['close'] * 1.0002
                    trade.private_post_trade_amend_order(
                        params={'instId': symbol_to_amend, 'ordId': order_id, 'newPx': str(price)})
        except :
            print('order has been completed')
            print(datetime.datetime.now())
    modify_order(60)

    def reach_target_pos(target_pos, symbol):
        position_now_all = trade.fetch_positions(symbol)
        if len(position_now_all) > 0:
            position_now = int(position_now_all[0]['pos'])
        else:
            position_now = 0
        # print('position_now: '+ str(position_now))
        if target_pos > position_now:
            if position_waiting == 0:
                od = trade.create_limit_buy_order(symbol, int(target_pos - position_now), long_price,
                                                  params={'tdMode': 'cross'})
            elif position_waiting + position_now < target_pos:
                for i in range(len(trade.fetch_open_orders(symbol))):
                    id = trade.fetch_open_orders(symbol)[i]['info']['ordId']
                    trade.cancel_order(id, symbol)
                od = trade.create_limit_buy_order(symbol, int(target_pos - position_now), long_price,
                                                  params={'tdMode': 'cross'})
        elif target_pos < position_now:
            if position_waiting == 0:
                od = trade.create_limit_sell_order(symbol, int(position_now - target_pos), short_price,
                                                   params={'tdMode': 'cross'})
            elif position_waiting + position_now > target_pos:
                for i in range(len(trade.fetch_open_orders(symbol))):
                    id = trade.fetch_open_orders(symbol)[i]['info']['ordId']
                    trade.cancel_order(id, symbol)
                od = trade.create_limit_sell_order(symbol, int(position_now - target_pos), short_price,
                                                   params={'tdMode': 'cross'})


    if d1 > d2 and d3 >= d4:
        # print('long')
        reach_target_pos(pos, symbol2)
    elif d1 < d2 and d3 <= d4:
        # print('short')
        reach_target_pos(-pos, symbol2)
    elif (d1 > d2 and d3 <= d4) or (d1 < d2 and d3 >= d4):
        # print('cover')
        reach_target_pos(0, symbol2)

    # time.sleep(10)
    # print(datetime.datetime.now())
    # except:
    #     continue
    # time.sleep(1)
