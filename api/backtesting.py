import backtrader as bt
import pandas as pd
import requests
from api.binance_connector import get_historical_data

# Definir el indicador OBV
class OnBalanceVolume(bt.Indicator):
    lines = ('obv',)

    def __init__(self):
        self.addminperiod(1)

    def next(self):
        if self.data.close[0] > self.data.close[-1]:
            self.lines.obv[0] = self.lines.obv[-1] + self.data.volume[0]
        elif self.data.close[0] < self.data.close[-1]:
            self.lines.obv[0] = self.lines.obv[-1] - self.data.volume[0]
        else:
            self.lines.obv[0] = self.lines.obv[-1]

# Estrategia que incluye MACD, OBV, y señales de momentum
class MomentumStrategy(bt.Strategy):
    params = (
        ('ema_fast', 8),
        ('ema_slow', 50),
        ('rsi_period', 14),
        ('atr_period', 14),
        ('atr_multiplier_sl', 2),
        ('atr_multiplier_tp', 3),
    )

    def __init__(self):
        self.ema_fast = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.params.ema_fast)
        self.ema_slow = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.params.ema_slow)
        self.rsi = bt.indicators.RelativeStrengthIndex(self.data.close, period=self.params.rsi_period)
        self.atr = bt.indicators.AverageTrueRange(self.data, period=self.params.atr_period)
        self.macd = bt.indicators.MACDHisto(self.data.close)  # MACD
        self.obv = OnBalanceVolume(self.data)  # OBV

    def next(self):
        if not self.position:
            if self.ema_fast > self.ema_slow and self.rsi < 70:
                print(f"Buying at {self.data.close[0]} on {self.data.datetime.date(0)}")
                stop_loss = self.data.close[0] - (self.atr[0] * self.params.atr_multiplier_sl)
                take_profit = self.data.close[0] + (self.atr[0] * self.params.atr_multiplier_tp)
                self.buy_bracket(stopprice=stop_loss, limitprice=take_profit)
        elif self.ema_fast < self.ema_slow or self.rsi > 70:
            print(f"Selling at {self.data.close[0]} on {self.data.datetime.date(0)}")
            self.sell()

def run_backtesting(symbol: str, interval: str):
    try:
        # Hacemos la solicitud a la API para obtener los datos históricos
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}"
        response = requests.get(url)
        data = response.json()

        # Convertimos los datos a un DataFrame
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
            'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume', 'ignore'
        ])

        # Convertir las columnas numéricas a float
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # Convertir el timestamp a formato datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Configurar cerebro de Backtrader
        cerebro = bt.Cerebro()

        # Convertir DataFrame de pandas en un DataFeed de Backtrader
        data_feed = bt.feeds.PandasData(dataname=df, datetime='timestamp')
        cerebro.adddata(data_feed)

        # Añadir la estrategia
        cerebro.addstrategy(MomentumStrategy)

        # Capital inicial
        cerebro.broker.set_cash(1000)

        # Comisión por operación
        cerebro.broker.setcommission(commission=0.001)

        # Agregar analyzer para registrar la evolución del capital y otras métricas
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="capital")
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

        # Ejecutar el backtest
        results = cerebro.run()

        # Obtener las métricas
        capital_evol = results[0].analyzers.capital.get_analysis()
        sharpe_ratio = results[0].analyzers.sharpe.get_analysis().get('sharperatio')
        drawdown = results[0].analyzers.drawdown.get_analysis()

        # Retornar las métricas en formato JSON
        return {
            "capital_evol": capital_evol,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": drawdown['max']['drawdown'],
            "max_drawdown_duration": drawdown['max']['len']
        }

    except Exception as e:
        return {"detail": f"Error ejecutando el backtesting: {e}"}
