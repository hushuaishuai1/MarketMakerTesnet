# binance_connector.py

import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
import os
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar las claves de API desde variables de entorno
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# Verificar que las claves de API estén presentes
if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    raise ValueError("Las claves de API de Binance no están configuradas. Por favor, configúralas en las variables de entorno.")

# Inicializar el cliente de Binance
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

def get_historical_data(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    try:
        logging.info(f"Obteniendo datos históricos para {symbol} con intervalo {interval} y límite {limit}")
        # Obtener datos históricos de Binance
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        
        # Convertir los datos a un DataFrame
        data = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convertir open_time a datetime y establecerlo como una columna
        data['timestamp'] = pd.to_datetime(data['open_time'], unit='ms')
        
        # Seleccionar columnas relevantes
        data = data[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        # Convertir tipos de datos a float
        for column in ['open', 'high', 'low', 'close', 'volume']:
            data[column] = data[column].astype(float)
        
        logging.info(f"Datos históricos obtenidos exitosamente para {symbol}")
        return data
    except BinanceAPIException as e:
        logging.error(f"Error al obtener datos históricos de Binance: {e}")
        raise e
    except Exception as e:
        logging.error(f"Error inesperado al obtener datos históricos: {e}")
        raise e
