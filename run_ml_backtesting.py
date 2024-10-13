import backtrader as bt
from ML.ml_strategy import MLStrategy
from api.binance_connector import get_historical_data
import pandas as pd
import logging

def run_ml_backtesting(symbol: str, interval: str, limit: int = 1000):
    try:
        df = get_historical_data(symbol, interval, limit)
        if df.empty:
            raise ValueError("No se obtuvieron datos históricos para el símbolo proporcionado.")

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        # Asegúrate de que el DataFrame tiene las columnas correctas
        df = df[['open', 'high', 'low', 'close', 'volume']]

        data_feed = bt.feeds.PandasData(dataname=df)

        cerebro = bt.Cerebro()
        cerebro.adddata(data_feed)
        cerebro.addstrategy(MLStrategy, model_path='ML/trained_model.pkl')
        cerebro.broker.setcash(1000)
        cerebro.run()
        cerebro.plot()
    except ValueError as ve:
        logging.error(f"ValueError en run_ml_backtesting: {ve}")
    except FileNotFoundError as fe:
        logging.error(f"FileNotFoundError en run_ml_backtesting: {fe}")
    except Exception as e:
        logging.error(f"Error inesperado en run_ml_backtesting: {e}", exc_info=True)

if __name__ == "__main__":
    # Ejemplo de ejecución
    run_ml_backtesting('BTCUSDT', '1h', 1000)
