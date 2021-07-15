from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,

)
from vnpy.trader.constant import Interval
import pandas as pd
import talib
import numpy as np


class fzdongneng(CtaTemplate):
    dongnengzhouqi = 13
    rolling = 34
    hcxs = 8
    qushi = 1

    atr = 0
    duozuigao = 0
    kongzuidi = 0
    ruchang = 0

    parameters = ['dongnengzhouqi', 'rolling', 'hcxs']
    variables = ['tr', 'duozhisun', 'duozuigao', 'kongzhisun', 'kongzuidi', 'ruchang']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg1 = BarGenerator(self.on_bar, 1, self.on_1h_bar, interval=Interval.MINUTE)
        self.bg4 = BarGenerator(self.on_bar, 1, self.on_4h_bar, interval=Interval.MINUTE)
        self.bg2 = BarGenerator(self.on_bar, 4, self.on_day_bar, interval=Interval.HOUR)

        self.am1 = ArrayManager()
        self.am2 = ArrayManager()
        self.am4 = ArrayManager()
        self.dongneng_series = []

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(20)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")
        self.put_event()

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

        self.put_event()

    def before_bar(self,bar:BarData):
        pass

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg1.update_tick(tick)
        self.bg4.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.bg1.update_bar(bar)
        self.bg2.update_bar(bar)
        self.bg4.update_bar(bar)
        if self.pos == 0:
            self.duozuigao = bar.high_price
            self.kongzuidi = bar.low_price
        elif self.pos > 0:
            self.duozuigao = max(self.duozuigao, bar.high_price)
            self.kongzuidi = bar.low_price
        elif self.pos < 0:
            self.duozuigao = bar.high_price
            self.kongzuidi = min(self.kongzuidi, bar.low_price)

    def on_1h_bar(self, bar: BarData):
        am = self.am1
        am.update_bar(bar)
        if not am.inited:
            return
        # kongjian1 = 0.1 * self.atr * self.hcxs
        kongjian2 = 0.01 * bar.close_price * self.hcxs
        kongjian1 = 0.01 * bar.close_price * self.hcxs

        duozhisun = (2 * self.duozuigao - kongjian1 - kongjian2) / 2
        kongzhisun = (2 * self.kongzuidi + kongjian1 + kongjian2) / 2
        if self.pos > 0:
            if bar.close_price < duozhisun:
                self.sell(bar.close_price, abs(self.pos))

        if self.pos < 0:
            if bar.close_price > kongzhisun:
                self.cover(bar.close_price, abs(self.pos))
        self.put_event()

    def on_day_bar(self, bar: BarData):
        am = self.am2
        am.update_bar(bar)
        if not am.inited:
            return
        self.atr = am.atr(21)
        diff_array = pd.Series(am.ema(34, array=True) - am.ema(89, array=True)).abs()
        # diff_now = diff_array.iloc[-1]
        diff_ma = diff_array.rolling(8, min_periods=0).mean()
        trend = diff_array - diff_ma

        if trend.iloc[-1] < 0:
            self.qushi = 1
        else:
            self.qushi = 1

    def on_4h_bar(self, bar: BarData):
        dongneng_series = self.dongneng_series
        am4 = self.am4
        am4.update_bar(bar)
        if not am4.inited:
            return
        vma = pd.Series(am4.volume[-100:]).rolling(self.dongnengzhouqi).mean()
        dongliang = bar.volume / vma.iloc[-1]

        xishu = 0
        high = bar.high_price
        low = bar.low_price
        kai = bar.open_price
        shou = bar.close_price
        syx = high - max(shou, kai)
        xyx = min(kai, shou) - low
        shiti = abs(kai - shou)
        zongti = high - low
        # xishu = syx/zongti + xyx/zongti + (shou - kai)/zongti
        if zongti != 0:
            xishu = (2 * shou - high - low) / zongti
            # xishu = (- 1.272 * syx + 1.272 * xyx - kai + shou)/(1.272*zongti)
        tr = (high - low) / (high + low)
        dongneng_series.append(dongliang * xishu * tr)
        # dongneng_series.append(xishu * tr)
        dongneng_series = pd.Series(dongneng_series)
        # dongneng = pd.Series(dongneng_series[-150:]).rolling(self.rolling).sum()
        dongneng_normal = (dongneng_series-dongneng_series.rolling(100).mean())/dongneng_series.rolling(100).std()
        # dongneng = dongneng_normal.rolling(self.rolling).sum()
        dongneng = dongneng_normal.ewm(span=self.rolling, adjust=True).mean()


        pos = 100000 / bar.close_price

        if len(dongneng) > 20:

            # if (dongneng.iloc[-1] - dongneng.iloc[-2] > 0):
            if (dongneng.iloc[-1] > 0) & (dongneng.iloc[-2] < 0):
                self.cancel_all()
                if self.pos == 0:
                    self.buy(bar.close_price, pos)
                if self.pos < 0:
                    self.cover(bar.close_price, abs(self.pos))
                    self.buy(bar.close_price, pos)

            # if (dongneng.iloc[-1] - dongneng.iloc[-2] < 0):
            if (dongneng.iloc[-1] < 0) & (dongneng.iloc[-2] > 0):
                self.cancel_all()
                if self.pos == 0:
                    self.short(bar.close_price, pos)
                if self.pos > 0:
                    self.sell(bar.close_price, abs(self.pos))
                    self.short(bar.close_price, pos)

        self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass
