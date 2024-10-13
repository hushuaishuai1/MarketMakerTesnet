import backtrader as bt
import matplotlib.pyplot as plt
import matplotlib

from api.binance_connector import get_historical_data
from ML.model_training import train_model
import pandas as pd
import logging


matplotlib.use('TkAgg') 
class MLStrategy(bt.Strategy):
    params = (('model_path', 'ML/trained_model.pkl'),)

    def __init__(self):
        self.model = None
        try:
            import joblib
            self.model = joblib.load(self.params.model_path)
            logging.info(f"Modelo cargado desde {self.params.model_path}")
        except FileNotFoundError:
            logging.error(f"Modelo no encontrado en {self.params.model_path}")
        except Exception as e:
            logging.error(f"Error al cargar el modelo: {e}")

    def next(self):
        if self.model is None:
            return

        features = self.get_features()
        if features is None:
            return

        prediction = self.model.predict([features])[0]

        if prediction == 1 and not self.position:
            self.buy()
            logging.info(f"Compra en {self.data.datetime.datetime(0)} al precio {self.dataclose[0]}")
        elif prediction == -1 and self.position:
            self.sell()
            logging.info(f"Venta en {self.data.datetime.datetime(0)} al precio {self.dataclose[0]}")

    def get_features(self):
        try:
            features = [self.data.close[0], self.data.volume[0]]
            return features
        except IndexError:
            return None

def plot_results(df: pd.DataFrame):
    plt.figure(figsize=(10, 6))
    plt.plot(df['close'], label='Close Price', alpha=0.6)
    plt.plot(df['ema_12'], label='EMA 12', linestyle='--', alpha=0.7)
    plt.plot(df['ema_26'], label='EMA 26', linestyle='--', alpha=0.7)
    plt.fill_between(df.index, df['bollinger_lband'], df['bollinger_hband'], color='gray', alpha=0.3, label='Bollinger Bands')
    plt.legend(loc='best')
    plt.title('Estrategia de Momentum con ML')
    plt.xlabel('Fecha')
    plt.ylabel('Precio')
    plt.savefig('ML/results.png')  # Guardar como PNG
    logging.info("Gráfica guardada como ML/results.png")

def run_ml_backtesting(symbol: str, interval: str, limit: int = 5000, model_path: str = 'ML/trained_model.pkl'):
    try:
        df = get_historical_data(symbol, interval, limit)
        if df.empty:
            logging.error("No se obtuvieron datos históricos para el símbolo proporcionado.")
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]

        plot_results(df)  # Mostrar resultados en gráficos

        data_feed = bt.feeds.PandasData(dataname=df)

        cerebro = bt.Cerebro()
        cerebro.adddata(data_feed)
        cerebro.addstrategy(MLStrategy, model_path=model_path)
        cerebro.broker.setcash(1000)
        cerebro.run()
        cerebro.plot(style='candlestick')  # Asegurarse de que se generen las gráficas
    except Exception as e:
        logging.error(f"Error en run_ml_backtesting: {e}", exc_info=True)
