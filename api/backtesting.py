import backtrader as bt
import pandas as pd
import logging
import numpy as np
from api.binance_connector import get_historical_data

# Configuración del logging detallado
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Estrategia Momentum sin normalización de precios
class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 12),
        ('ema_slow', 30),
        ('rsi_period', 14),
        ('atr_period', 14),
        ('atr_multiplier_sl', 1.5),
        ('atr_multiplier_tp', 2.5),
    )

    def __init__(self):
        # Indicadores
        self.ema_fast = bt.indicators.EMA(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)

        # Variables para gestión de órdenes
        self.order = None
        self.buy_price = None

        # Variables para métricas
        self.start_cash = self.broker.getcash()
        self.returns = []

    def log(self, txt, level=logging.INFO, dt=None):
        dt = dt or self.data.datetime.datetime(0)
        logging.log(level, f'{dt.isoformat()} - {txt}')

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, '
                         f'Commission: {order.executed.comm}')
                self.buy_price = order.executed.price
            elif order.issell():
                pnl = order.executed.pnl
                self.log(f'SELL EXECUTED, Price: {order.executed.price}, PnL: {pnl}, '
                         f'Commission: {order.executed.comm}')
                # Calcular retorno y resetear buy_price
                if self.buy_price:
                    retorno = (order.executed.price - self.buy_price) / self.buy_price
                    self.returns.append(retorno)
                    self.buy_price = None  # Resetear buy_price después de la venta
            self.order = None

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin Not Enough/Rejected', level=logging.WARNING)
            self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f'OPERATION RESULT - Gross PnL: {trade.pnl}, Net PnL: {trade.pnlcomm}')

    def next(self):
        if self.order:
            return

        # Log cash and value before placing a trade
        cash_available = self.broker.getcash()
        portfolio_value = self.broker.getvalue()
        self.log(f"Cash: {cash_available}, Portfolio Value: {portfolio_value}")

        # Verificar que los precios y ATR sean válidos
        if not np.isnan(self.data.close[0]) and not np.isnan(self.atr[0]) and self.data.close[0] > 0:
            if not self.position:
                if self.ema_fast[0] > self.ema_slow[0] and self.rsi[0] < 70:
                    # Calcular Stop-Loss y Take-Profit
                    self.stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                    self.take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                    
                    # Validar que SL y TP sean lógicos y realistas
                    if self.stop_loss > 0 and self.take_profit > self.data.close[0]:
                        self.log(f"BUY SIGNAL: SL = {self.stop_loss}, TP = {self.take_profit}, "
                                 f"Price = {self.data.close[0]}")
                        
                        # Colocar orden de compra
                        self.order = self.buy()
                    else:
                        self.log(f"Invalid SL/TP: SL = {self.stop_loss}, TP = {self.take_profit}. Skipping order.", level=logging.WARNING)
            else:
                # Verificar si se alcanzó el TP o SL
                if self.data.close[0] >= self.take_profit or self.data.close[0] <= self.stop_loss:
                    self.log(f'SELL SIGNAL: Price = {self.data.close[0]}')
                    self.order = self.sell()
        else:
            self.log("Data not valid for trading at this moment.", level=logging.WARNING)

    def stop(self):
        final_value = self.broker.getvalue()
        pnl = final_value - self.start_cash
        self.log(f"Final Portfolio Value: {final_value}, PnL: {pnl}")

        # Calcular métricas
        if len(self.returns) > 1:
            sharpe_ratio = (np.mean(self.returns) / np.std(self.returns)) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        self.log(f"Sharpe Ratio: {sharpe_ratio}")

def run_backtesting(symbol: str, interval: str, limit: int = 1000):
    try:
        # Obtener datos históricos
        df = get_historical_data(symbol, interval, limit)
        if df.empty:
            raise ValueError("No se obtuvieron datos históricos.")

        # Limpiar datos
        df = df.dropna(subset=['close', 'open', 'high', 'low', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        # Configurar Backtrader
        cerebro = bt.Cerebro()
        cerebro.addstrategy(MomentumStrategy)
        cerebro.broker.set_cash(1000)  # Capital inicial
        cerebro.broker.setcommission(commission=0.005)  # 0.05% comisión por trade
        cerebro.addsizer(bt.sizers.PercentSizer, percents=1)  # 1% del capital por operación

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
