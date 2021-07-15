import os
import ccxt
import time
import pymysql
import logging
tm = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
log_name = tm + '_huobi.log'
logging.basicConfig(filename=log_name, level=logging.INFO,format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
# 连接database
db = pymysql.connect(
    host='127.0.0.1',
    user ='这里输用户名',
    password ='这里输密码',
    database ='huobi',)

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

#在数据库中创建新的订单信息表
cursor.execute("DROP TABLE IF EXISTS order_info")
sql = """CREATE TABLE order_info (
         order_id VARCHAR(100) NOT NULL,
         side VARCHAR(10),
         price FLOAT,  
         amount FLOAT,
         related_id VARCHAR(100))"""
cursor.execute(sql)
# huobi
apikey = ‘这里输火币账户的apikey’
secretkey = ‘这里输火币账户的secretkey’

huobi=ccxt.huobipro({
    'apiKey':apikey,
    'secret':secretkey,
})


order_symbol='ETH/USDT'
order_type='limit'
order_side='buy'
order_amount=0.04

ETH_Last=huobi.fetch_ticker(order_symbol)['last']
logging.info('ETH 最新价格:'+str(ETH_Last))

order_price=ETH_Last-0.3

take_order=huobi.create_order(order_symbol,order_type,order_side,order_amount,order_price)
logging.info(take_order)

takeorder_id=take_order['id']
logging.info(takeorder_id)

order_status=huobi.fetch_order(takeorder_id,order_symbol)
logging.info(order_status)

takeorder_side=order_status['side']
logging.info(takeorder_side)

takeorder_price=order_status['price']
logging.info(takeorder_price)

sql = f"""INSERT INTO order_info(order_id,side, price, amount, related_id) VALUES ('{takeorder_id}', '{takeorder_side}', {takeorder_price}, {order_amount}, '{takeorder_id}')"""
cursor.execute(sql)
db.commit()

base_usdt=500

while True:
    # 价格低的优先查找（先买单后卖单）
    sql = 'SELECT * FROM order_info ORDER BY price'
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    for index, row in enumerate(results):
        order_id = row[0]
        side = row[1]
        price = row[2]
        amount = row[3]
        related_id = row[4]

        # 防止服务器请求异常
        while True:
            try:
                order_status = huobi.fetch_order_status(order_id, order_symbol)
                last_price = huobi.fetch_ticker(order_symbol)['last']
                balance = huobi.fetch_balance()
                # 可用余额的监控与交易量的调整
                if balance['USDT']['free'] >= 1.1 * base_usdt:
                    base_usdt = 1.1 * base_usdt
                    order_amount = round(1.1 * order_amount, 3)
                break
            except:
                logging.warning('请求异常，1秒后重试')
                time.sleep(1)
                continue

        if side == 'buy':
            # 当买单成交时
            if order_status == 'closed':
                logging.info('买单成交！')
                # 删除已成交的db记录
                sql = f"DELETE FROM order_info WHERE order_id='{order_id}'"
                cursor.execute(sql)
                db.commit()

                # 在止盈处下卖单
                sell_side = 'sell'
                sell_price = price + 2
                take_sell_order = huobi.create_order(order_symbol, order_type, sell_side, 0.998 * amount, sell_price)
                takeorder_id = take_sell_order['id']
                sql = f"""INSERT INTO order_info(order_id,side, price, amount, related_id) VALUES ('{takeorder_id}', '{sell_side}', {sell_price}, {0.999 * amount}, '{takeorder_id}')"""
                related_id = takeorder_id
                cursor.execute(sql)
                db.commit()
                logging.info(
                    f"在止盈处下卖单成功:\n'{takeorder_id}', '{sell_side}', {sell_price}, {0.998 * amount}, '{takeorder_id}'")

                # 在低一档的价格下买单
                buy_side = 'buy'
                buy_price = price - 2
                    available_usdt = balance['USDT']['free']
                if available_usdt < buy_price * order_amount:
                    logging.info('可用余额不足，无法下新的买单')
                    continue
                take_buy_order = huobi.create_order(order_symbol, order_type, buy_side, order_amount, buy_price)
                takeorder_id = take_buy_order['id']
                sql = f"""INSERT INTO order_info(order_id,side, price, amount, related_id) VALUES ('{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{related_id}')"""
                cursor.execute(sql)
                db.commit()
                logging.info(
                    f"在低一档的价格下买单成功:\n'{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{related_id}")



            # 当前价格远大于所挂买单时，撤销原有的买单并重新根据当前价格下新的买单
            elif order_status == 'open' and len(results) == 1:
                if last_price - price >= 4:
                    logging.info('当前价格远大于所挂买单，撤销原有的买单！')
                    # 删除未成交的买单及db记录
                    sql = "SELECT * FROM order_info where side='buy'"
                    cursor.execute(sql)
                    total = cursor.fetchall()
                    try:
                        for row in total:
                            huobi.cancel_order(row[0], order_symbol)
                    except:
                        logging.warning('无法撤销已成交的买单')
                        continue
                    sql = f"DELETE FROM order_info WHERE side='buy'"
                    cursor.execute(sql)
                    db.commit()
                    logging.info('成功撤销所有买单！')

                    # 在略低于当前价格的地方下买单
                    buy_side = 'buy'
                    buy_price = last_price - 0.3
                    take_buy_order = huobi.create_order(order_symbol, order_type, buy_side, order_amount, buy_price)
                    takeorder_id = take_buy_order['id']
                    sql = f"""INSERT INTO order_info(order_id,side, price, amount, related_id) VALUES ('{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{takeorder_id}')"""
                    cursor.execute(sql)
                    db.commit()
                    logging.info(
                        f"在略低于当前价格的地方下买单成功:\n'{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{takeorder_id}'")



        elif side == 'sell':
            # 当卖单成交时
            if order_status == 'closed':
                logging.info('卖单成交！')

                # 删除已成交的db记录
                sql = f"DELETE FROM order_info WHERE order_id='{order_id}'"
                cursor.execute(sql)
                db.commit()

                # 删除未成交的买单及db记录
                sql = "SELECT * FROM order_info where side='buy'"
                cursor.execute(sql)
                total = cursor.fetchall()
                try:
                    for row in total:
                        huobi.cancel_order(row[0], order_symbol)
                except:
                    logging.warning('无法撤销已成交的买单')
                    continue
                sql = f"DELETE FROM order_info WHERE side='buy'"
                cursor.execute(sql)
                db.commit()
                logging.info('成功撤销所有买单！')

                # 在回调的地方下买单
                buy_side = 'buy'
                buy_price = last_price - 2
                take_buy_order = huobi.create_order(order_symbol, order_type, buy_side, order_amount, buy_price)
                takeorder_id = take_buy_order['id']
                sql = f"""INSERT INTO order_info(order_id,side, price, amount, related_id) VALUES ('{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{takeorder_id}')"""
                cursor.execute(sql)
                db.commit()
                logging.info(
                    f"在回调的地方下买单成功:\n'{takeorder_id}', '{buy_side}', {buy_price}, {order_amount}, '{takeorder_id}'")

            # 若价格低的卖单还未成交，则价格高的不可能成交
            else:
                time.sleep(1)
                break

        time.sleep(1)
