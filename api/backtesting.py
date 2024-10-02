import backtrader as bt
import pandas as pd
import logging
from api.binance_connector import get_historical_data

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definir la estrategia de momentum
class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 8),
        ('ema_slow', 50),
        ('rsi_period', 14),
        ('atr_period', 14),
        ('atr_multiplier_sl', 2.0),
        ('atr_multiplier_tp', 3.0),
    )

    def __init__(self):
        self.ema_fast = bt.indicators.EMA(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.ATR(self.data, period=self.params.atr_period)
        self.stop_loss = None
        self.take_profit = None

    def next(self):
        if not self.position:
            if self.ema_fast[0] > self.ema_slow[0] and self.rsi[0] < 70:
                self.stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                self.take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                self.buy_bracket(stopprice=self.stop_loss, limitprice=self.take_profit)
        else:
            if self.data.close[0] >= self.take_profit:
                self.sell()

# Función para ejecutar el backtesting
def run_backtesting(symbol: str, interval: str, limit: int = 1000):
    try:
        # Obtener los datos históricos de Binance
        df = get_historical_data(symbol, interval, limit)
        
        # Asegurar que las columnas estén en el tipo correcto
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Inicializar cerebro para backtesting
        cerebro = bt.Cerebro()

        # Convertir el DataFrame de pandas a un DataFeed de Backtrader
        data_feed = bt.feeds.PandasData(dataname=df, datetime='timestamp')
        cerebro.adddata(data_feed)

        # Añadir la estrategia a Cerebro
        cerebro.addstrategy(MomentumStrategy)

        # Capital inicial
        cerebro.broker.set_cash(1000)
        cerebro.broker.setcommission(commission=0.001)

        # Ejecutar el backtesting
        results = cerebro.run()

        # Mostrar el valor final después del backtesting
        final_value = cerebro.broker.getvalue()
        return {"final_value": final_value}

    except Exception as e:
        return {"detail": f"Error ejecutando el backtesting: {e}"}
