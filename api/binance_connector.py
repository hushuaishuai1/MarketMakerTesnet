import json
import requests
import pandas as pd
from binance.client import Client

# Cargar las claves de API desde config.json
with open('config.json', 'r') as f:
    config = json.load(f)

client = Client(config['BINANCE_API_KEY'], config['BINANCE_API_SECRET'])

def get_historical_data(symbol: str, interval: str, limit: int = 1000):
    base_url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Formatear los datos en un DataFrame
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'quote_asset_volume', 'number_of_trades', 
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 
            'ignore'
        ])

        # Convertir el timestamp a datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Asegurarse de que las columnas clave son del tipo float
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

        return df

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error obteniendo datos históricos de Binance: {e}")
