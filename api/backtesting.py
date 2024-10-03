import backtrader as bt
import pandas as pd
import logging
from api.binance_connector import get_historical_data

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 8),
        ('ema_slow', 35),
        ('rsi_period', 18),
        ('atr_period', 18),
        ('atr_multiplier_sl', 2.3),
        ('atr_multiplier_tp', 3.2),
    )

    def __init__(self):
        self.ema_fast = bt.indicators.EMA(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.stop_loss = None
        self.take_profit = None
        self.order = None  # Track order
        self.buy_price = None  # Track buy price

    def log(self, txt, dt=None):
        dt = dt or self.data.datetime.date(0)
        logging.info(f'{dt.isoformat()} - {txt}')

    def next(self):
        if self.order:
            return  # Wait for the order to be completed

        # Log cash and value before placing a trade
        cash_available = self.broker.getcash()
        portfolio_value = self.broker.getvalue()
        self.log(f"Cash: {cash_available}, Portfolio Value: {portfolio_value}")

        # Check if no position and a buy signal appears
        if not self.position:
            if self.ema_fast[0] > self.ema_slow[0] and self.rsi[0] < 70:
                self.stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                self.take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                self.log(f"BUY SIGNAL: SL = {self.stop_loss}, TP = {self.take_profit}, Price = {self.data.close[0]}")

                # Calculate size based on available cash and 5% capital usage
                size = (cash_available * 0.05) / self.data.close[0]  # 5% of available cash
                self.log(f"Placing BUY order with size: {size}, Cash available: {cash_available}")
                
                self.order = self.buy_bracket(
                    size=size,
                    stopprice=self.stop_loss,
                    limitprice=self.take_profit
                )  # Place buy order with stop-loss and take-profit
        else:
            # Sell logic based on conditions
            if self.data.close[0] >= self.take_profit or self.data.close[0] <= self.stop_loss:
                self.log(f'SELL SIGNAL: Price = {self.data.close[0]}')
                self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, Commission: {order.executed.comm}')
                self.buy_price = order.executed.price
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, Commission: {order.executed.comm}')
            self.order = None

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin Not Enough/Rejected')

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl}, NET {trade.pnlcomm}')


def run_backtesting(symbol: str, interval: str, limit: int = 1000):
    try:
        df = get_historical_data(symbol, interval, limit)
        
        # Clean and prepare data
        df.dropna(subset=['close', 'open', 'high', 'low', 'volume'], inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        # Set up Backtrader
        cerebro = bt.Cerebro()
        cerebro.addstrategy(MomentumStrategy)
        cerebro.broker.set_cash(1000)  # Initial cash
        cerebro.broker.setcommission(commission=0.001)  # 0.1% commission per trade
        cerebro.addsizer(bt.sizers.PercentSizer, percents=45)  # 5% capital per trade

        data_feed = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data_feed)

        logging.info(f"Starting Backtesting for {symbol}...")
        cerebro.run()

        final_value = cerebro.broker.getvalue()
        logging.info(f"Backtesting completed. Final portfolio value: {final_value}")
        return {"final_value": final_value}

    except Exception as e:
        logging.error(f"Error running backtesting: {str(e)}")
        return {"detail": f"Error running backtesting: {e}"}
