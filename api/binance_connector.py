import json
import requests
import pandas as pd
from binance.client import Client
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar las claves de API desde el archivo config.json
with open('config.json', 'r') as f:
    config = json.load(f)

client = Client(config['BINANCE_API_KEY'], config['BINANCE_API_SECRET'])

def get_historical_data(symbol: str, interval: str, limit: int = 1000):
    """
    Obtiene datos históricos de un par de criptomonedas desde la API de Binance.
    
    Args:
        symbol (str): El símbolo de la criptomoneda (por ejemplo, 'BTCUSDT').
        interval (str): El intervalo de tiempo para cada vela (por ejemplo, '5m', '1h').
        limit (int): El número máximo de velas a obtener (por defecto, 1000).
    
    Returns:
        pd.DataFrame: Un DataFrame con los datos históricos formateados.
    
    Raises:
        Exception: Si hay un problema al obtener los datos desde Binance.
    """
    base_url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Lanza una excepción si la solicitud falla
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

        # Asegurar que las columnas clave son del tipo float
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

        logging.info(f"Datos obtenidos correctamente para {symbol} con intervalo {interval}")
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos históricos de Binance: {e}")
        raise Exception(f"Error obteniendo datos históricos de Binance: {e}")

# Ejemplo de uso
if __name__ == '__main__':
    symbol = 'BTCUSDT'
    interval = '5m'
    limit = 10000
    df = get_historical_data(symbol, interval, limit)
    print(df.head())  # Mostrar las primeras filas del DataFrame
