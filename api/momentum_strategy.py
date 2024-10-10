import backtrader as bt
import pandas as pd
import numpy as np
import logging
from api.binance_connector import get_historical_data

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# (Opcional) Normalización del volumen
def normalize_data(df):
    """
    Normaliza los datos de volumen utilizando StandardScaler.

    Args:
        df (pd.DataFrame): DataFrame con los datos de precios históricos.

    Returns:
        pd.DataFrame: DataFrame con los datos normalizados.
    """
    scaler = StandardScaler()
    df['volume'] = scaler.fit_transform(df[['volume']])
    return df

# Estrategia Momentum con condiciones mejoradas
class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 12),
        ('ema_slow', 30),
        ('rsi_period', 14),
        ('atr_period', 14),
        ('atr_multiplier_sl', 1.2),  # Multiplicador razonable para Stop-Loss
        ('atr_multiplier_tp', 2.2),  # Multiplicador razonable para Take-Profit
    )

    def __init__(self):
        self.ema_fast = bt.indicators.EMA(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)

        self.stop_loss = None
        self.take_profit = None
        self.order = None
        self.buy_price = None
        self.start_cash = self.broker.getcash()
        self.returns = []

    def log(self, txt, level=logging.INFO, dt=None):
        dt = dt or self.data.datetime.datetime(0)
        logging.log(level, f'{dt.isoformat()} - {txt}')

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED: Price: {order.executed.price}, Size: {order.executed.size}')
                self.buy_price = order.executed.price
            elif order.issell():
                pnl = order.executed.pnl
                self.log(f'SELL EXECUTED: Price: {order.executed.price}, PnL: {pnl}')
                self.returns.append((order.executed.price - self.buy_price) / self.buy_price)
                self.buy_price = None
            self.order = None

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'Order Canceled/Margin Not Enough/Rejected', level=logging.WARNING)
            self.order = None

    def next(self):
        if self.order:
            return

        # Verificar que los precios y ATR sean válidos
        if self.data.close[0] > 0 and not np.isnan(self.atr[0]):
            if not self.position:
                if self.ema_fast[0] > self.ema_slow[0] and self.rsi[0] < 70:
                    self.stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                    self.take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                    self.log(f'BUY SIGNAL: SL = {self.stop_loss}, TP = {self.take_profit}, Price = {self.data.close[0]}')

                    size = self.broker.getcash() * 0.03 / self.data.close[0]  # Usar 5% del capital disponible
                    self.order = self.buy(size=size)
            else:
                if self.data.close[0] >= self.take_profit or self.data.close[0] <= self.stop_loss:
                    self.log(f'SELL SIGNAL: Price = {self.data.close[0]}')
                    self.order = self.sell()
        else:
            self.log(f'Datos inválidos. Precio: {self.data.close[0]}, ATR: {self.atr[0]}', level=logging.WARNING)

    def stop(self):
        final_value = self.broker.getvalue()
        pnl = final_value - self.start_cash
        self.log(f"Final Portfolio Value: {final_value}, PnL: {pnl}")
        if len(self.returns) > 1:
            sharpe_ratio = (np.mean(self.returns) / np.std(self.returns)) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        self.log(f"Sharpe Ratio: {sharpe_ratio}")

def run_backtesting(symbol: str, interval: str, limit: int, window_size: int = 10):
    try:
        df = get_historical_data(symbol, interval, limit)
        if df.empty:
            raise ValueError("No se obtuvieron datos históricos.")

        df = df.dropna(subset=['close', 'open', 'high', 'low', 'volume'])
        # df = normalize_data(df)  # Eliminar o comentar esta línea
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        cerebro = bt.Cerebro()
        cerebro.addstrategy(MomentumStrategy)
        cerebro.broker.set_cash(1000)  # Dinero inicial
        cerebro.broker.setcommission(commission=0.01)  # Comisión de 0.1%
        cerebro.addsizer(bt.sizers.PercentSizer, percents=3)  # 5% del capital por trade

        data_feed = bt.feeds.PandasData(dataname=df, name=symbol)
        cerebro.adddata(data_feed)

        logging.info(f"Starting Backtesting for {symbol} with interval {interval}...")
        cerebro.run()

        final_value = cerebro.broker.getvalue()
        logging.info(f"Backtesting completed. Final portfolio value: {final_value}")

        return {"final_value": final_value}

    except Exception as e:
        logging.error(f"Error running backtesting: {str(e)}", exc_info=True)
        return {"detail": f"Error running backtesting: {e}"}
