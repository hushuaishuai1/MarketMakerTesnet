from binance.client import Client
import json
import pandas as pd
import os

# Cargar las claves de API desde config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# Inicializar el cliente de Binance
client = Client(config['BINANCE_API_KEY'], config['BINANCE_API_SECRET'])

def get_historical_data(symbol, interval, limit):
    """
    Obtiene los datos históricos de la criptomoneda desde Binance.
    Parameters:
    - symbol: El par de criptomonedas (ej. 'BTCUSDT')
    - interval: El intervalo de tiempo (ej. '1h', '15m')
    - limit: La cantidad de velas que se desean obtener (máximo 1000)
    """
    # Obtener datos históricos
    klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)

    # Crear un DataFrame con los datos
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])

    # Convertir tipos de datos con información de zona horaria UTC
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Seleccionar solo las columnas necesarias
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

    return df
