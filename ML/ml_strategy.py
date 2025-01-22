# ml_strategy
import backtrader as bt
import pandas as pd
import numpy as np
import joblib
import logging
import os
from binance.client import Client
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

 
def fetch_binance_data(symbol, interval, limit=9000):
    try:
        logging.info(f"Obteniendo datos para {symbol}, intervalo {interval}, límite {limit}")
        client = Client(api_key=API_KEY, api_secret=API_SECRET)
        
        klines = []
        limit_per_request = 1000
        end_time = None
        
        while len(klines) < limit:
            fetch_limit = min(limit_per_request, limit - len(klines))
            new_klines = client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=fetch_limit,
                endTime=end_time
            )
            if not new_klines:
                break
            klines.extend(new_klines)
            end_time = new_klines[0][0] - 1  # Actualizar el tiempo de finalización para la siguiente iteración

        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
            'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume', 'ignore'
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].astype(float)
        df.sort_index(inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error al obtener datos de Binance: {str(e)}")
        return pd.DataFrame()
class OBVIndicator(bt.Indicator):
    lines = ('obv',)
    def __init__(self):
        self.addminperiod(2)

    def next(self):
        if len(self) == 1:
            self.lines.obv[0] = self.data.volume[0]
        else:
            if self.data.close[0] > self.data.close[-1]:
                self.lines.obv[0] = self.lines.obv[-1] + self.data.volume[0]
            elif self.data.close[0] < self.data.close[-1]:
                self.lines.obv[0] = self.lines.obv[-1] - self.data.volume[0]
            else:
                self.lines.obv[0] = self.lines.obv[-1]

class MLStrategy(bt.Strategy):
    params = (('stop_loss', 0.16), ('take_profit', 0.33), ('umbral_probabilidad', 0.95),)

    def __init__(self):
        model_data = joblib.load('ML/xgboost_001model.pkl')
        self.model = model_data['model']
        self.feature_names = model_data['feature_names']

        scaler_data = joblib.load('ML/scaler001.pkl')
        self.scaler = scaler_data['scaler']

        self.ema12 = bt.indicators.ExponentialMovingAverage(self.data.close, period=12)
        self.ema26 = bt.indicators.ExponentialMovingAverage(self.data.close, period=26)
        self.rsi = bt.indicators.RelativeStrengthIndex(self.data.close, period=16)
        self.bollinger = bt.indicators.BollingerBands(self.data.close, period=20)
        self.macd = bt.indicators.MACD(self.data.close)
        self.atr = bt.indicators.AverageTrueRange(self.data, period=14)
        self.obv = OBVIndicator(self.data)
        
        self.order = None

    def next(self):
        if len(self.data) < 26:
            return

        try:
            features = {
                'close': self.data.close[0],
                'volume': self.data.volume[0],
                'ema_12': self.ema12[0],
                'ema_26': self.ema26[0],
                'rsi': self.rsi[0],
                'bollinger_hband': self.bollinger.lines.top[0],
                'bollinger_lband': self.bollinger.lines.bot[0],
                'macd_diff': self.macd.macd[0] - self.macd.signal[0],
                'atr': self.atr[0],
                'obv': self.obv[0],
                'log_return': np.log(self.data.close[0] / self.data.close[-1]),
                'ema_diff': self.ema12[0] - self.ema26[0],
                'volatility': pd.Series([np.log(self.data.close[-i] / self.data.close[-i-1]) for i in range(1,17)]).std()
            }
        except IndexError:
            return

        features_df = pd.DataFrame([features])[self.feature_names]
        features_scaled = self.scaler.transform(features_df)

        prob = self.model.predict_proba(features_scaled)[0][1]
        logging.info(f"Predicted Probability: {prob}")

        if prob > self.params.umbral_probabilidad and not self.position:
            if self.data.close[0] < self.bollinger.lines.bot[0] and self.obv[0] > self.obv[-1]:
                self._place_order('buy')
        elif prob < (1 - self.params.umbral_probabilidad) and self.position:
            if self.data.close[0] > self.bollinger.lines.top[0] and self.obv[0] < self.obv[-1]:
                self._place_order('sell')

    def _place_order(self, order_type):
        try:
            if order_type == 'buy' and not self.position:
                cash = self.broker.get_cash()
                price = self.data.close[0]
                size = (cash * 0.88) / price
                self.order = self.buy(size=size)
                logging.info(f"Orden de compra colocada, Tamaño: {size}")
            elif order_type == 'sell' and self.position:
                size = self.position.size
                self.order = self.sell(size=size)
                logging.info(f"Orden de venta colocada, Tamaño: {size}")
        except Exception as e:
            logging.error(f"Error al colocar la orden: {e}")

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                logging.info(f"Compra completada, Precio: {order.executed.price}, Tamaño: {order.executed.size}")
            elif order.issell():
                logging.info(f"Venta completada, Precio: {order.executed.price}, Tamaño: {order.executed.size}")
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logging.warning("Orden cancelada/marginada/rechazada")
        self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            pnl = trade.pnl
            logging.info(f'Operación cerrada, Ganancia/Pérdida: {pnl}')

    def stop(self):
        valor_inicial = self.broker.startingcash
        valor_final = self.broker.getvalue()
        roi = (valor_final / valor_inicial - 1) * 100
        logging.info(f"Valor inicial del portafolio: {valor_inicial}")
        logging.info(f"Valor final del portafolio: {valor_final}")
        logging.info(f"Retorno de la inversión: {roi:.2f}%")

def run_ml_backtesting(symbol: str, interval: str, limit: int = 9000):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MLStrategy)

    df = fetch_binance_data(symbol, interval, limit)
    if df.empty:
        logging.error("Backtesting fallido: DataFrame vacío.")
        return

    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)
    cerebro.broker.set_cash(1000.0)
    cerebro.broker.setcommission(commission=0.0013)
    cerebro.run()
    cerebro.plot(style='candlestick')


if __name__ == '__main__':
    run_ml_backtesting('BTCUSDT', '5m', limit=9000)
