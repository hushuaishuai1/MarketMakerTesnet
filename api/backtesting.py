import backtrader as bt
import pandas as pd
from api.binance_connector import get_historical_data

class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 8),
        ('ema_slow', 50),
        ('rsi_period', 14),
        ('atr_period', 14),
        ('atr_multiplier_sl', 2.0),  # Ajustable
        ('atr_multiplier_tp', 3.0),  # Ajustable
    )

    def __init__(self):
        self.ema_fast = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RelativeStrengthIndex(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.AverageTrueRange(self.data, period=self.params.atr_period)
        self.macd = bt.indicators.MACDHisto(self.data.close)
        self.trailing_stop = None

    def next(self):
        if not self.position:
            if self.ema_fast > self.ema_slow and self.rsi < 70:
                stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                self.buy_bracket(stopprice=stop_loss, limitprice=take_profit)
        else:
            self.trailing_stop = max(self.trailing_stop or stop_loss, self.data.close[0] - (2 * self.atr[0]))
            if self.data.close[0] <= self.trailing_stop:
                self.sell()

def run_backtesting(symbol: str, interval: str):
    try:
        df = get_historical_data(symbol, interval, limit=1000)

        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        cerebro = bt.Cerebro()
        data_feed = bt.feeds.PandasData(dataname=df, datetime='timestamp')
        cerebro.adddata(data_feed)

        cerebro.addstrategy(MomentumStrategy)
        cerebro.broker.set_cash(1000)
        cerebro.broker.setcommission(commission=0.001)
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="capital")
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

        results = cerebro.run()

        capital_evol = results[0].analyzers.capital.get_analysis()
        sharpe_ratio = results[0].analyzers.sharpe.get_analysis().get('sharperatio')
        drawdown = results[0].analyzers.drawdown.get_analysis()

        return {
            "capital_evol": capital_evol,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": drawdown['max']['drawdown'],
            "max_drawdown_duration": drawdown['max']['len']
        }

    except Exception as e:
        return {"detail": f"Error ejecutando el backtesting: {e}"}
