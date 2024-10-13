import backtrader as bt
import pandas as pd
import numpy as np
import logging
from api.binance_connector import get_historical_data
import matplotlib.pyplot as plt
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
        ('atr_multiplier_sl', 1.2),
        ('atr_multiplier_tp', 2.2),
    )

    def __init__(self):
        try:
            # Indicadores
            self.ema_fast = bt.indicators.EMA(self.data.close, period=self.params.ema_fast)
            self.ema_slow = bt.indicators.EMA(self.data.close, period=self.params.ema_slow)
            self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)
            self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        except bt.Strategy.Error as e:
            self.log(f'Error inicializando indicadores: {e}', level=logging.ERROR)
            raise e

        # Variables para gestión de órdenes
        self.order = None
        self.buy_price = None

        # Variables para métricas
        self.start_cash = self.broker.getcash()
        self.returns = []
        self.trade_pnls = []
        self.daily_values = []

    def log(self, txt, level=logging.INFO, dt=None):
        dt = dt or self.data.datetime.datetime(0)
        logging.log(level, f'{dt.isoformat()} - {txt}')

    def notify_order(self, order):
        try:
            if order.status in [order.Completed]:
                if order.isbuy():
                    self.log(f'BUY EXECUTED: Price: {order.executed.price}, Size: {order.executed.size}')
                    self.buy_price = order.executed.price
                elif order.issell():
                    pnl = order.executed.pnl
                    self.log(f'SELL EXECUTED: Price: {order.executed.price}, PnL: {pnl}')
                    if self.buy_price:
                        retorno = (order.executed.price - self.buy_price) / self.buy_price
                        self.returns.append(retorno)
                        self.trade_pnls.append(pnl)
                        self.buy_price = None
                self.order = None

            elif order.status in [order.Canceled, order.Margin, order.Rejected]:
                self.log('Order Canceled/Margin Not Enough/Rejected', level=logging.WARNING)
                self.order = None
        except AttributeError as e:
            self.log(f'Atributo no encontrado: {e}', level=logging.ERROR)
        except Exception as e:
            self.log(f'Error inesperado en notify_order: {e}', level=logging.ERROR)

    def notify_trade(self, trade):
        try:
            if trade.isclosed:
                self.log(f'OPERATION RESULT - Gross PnL: {trade.pnl}, Net PnL: {trade.pnlcomm}')
        except Exception as e:
            self.log(f'Error en notify_trade: {e}', level=logging.ERROR)

    def next(self):
        if self.order:
            return

        try:
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
                            self.log(f"BUY SIGNAL: SL = {self.stop_loss}, TP = {self.take_profit}, Price = {self.data.close[0]}")
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
        except Exception as e:
            self.log(f'Error en next: {e}', level=logging.ERROR)

    def stop(self):
        try:
            final_value = self.broker.getvalue()
            pnl = final_value - self.start_cash

            # Cálculo del drawdown máximo
            peak = self.start_cash
            max_drawdown = 0
            for value in self.daily_values:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

            # Ratio de ganancias/pérdidas
            total_trades = len(self.trade_pnls)
            winning_trades = len([pnl for pnl in self.trade_pnls if pnl > 0])
            losing_trades = len([pnl for pnl in self.trade_pnls if pnl <= 0])
            win_loss_ratio = (winning_trades / losing_trades) if losing_trades > 0 else 'Inf'

            # Calmar Ratio
            annual_return = (final_value / self.start_cash) - 1
            calmar_ratio = (annual_return / max_drawdown) if max_drawdown > 0 else 'Inf'

            self.log(f"Final Portfolio Value: {final_value}, PnL: {pnl}")
            self.log(f"Maximum Drawdown: {max_drawdown * 100:.2f}%")
            self.log(f"Win/Loss Ratio: {win_loss_ratio}")
            self.log(f"Calmar Ratio: {calmar_ratio}")

            # Visualización del valor del portafolio en el tiempo
            plt.figure(figsize=(12, 6))
            plt.plot(self.daily_values)
            plt.title('Valor del Portafolio en el Tiempo')
            plt.xlabel('Períodos')
            plt.ylabel('Valor del Portafolio')
            plt.grid(True)
            plt.show()
        except Exception as e:
            self.log(f'Error en stop: {e}', level=logging.ERROR)

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
