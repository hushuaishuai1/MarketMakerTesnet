import pandas as pd
from binance.client import Client
import os
import logging
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves de API desde variables de entorno
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Verificación de claves API
if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    logging.error("Las claves API no están configuradas correctamente. Verifique las variables de entorno.")
    raise ValueError("Faltan las claves API de Binance.")

# Conectar a Binance Spot Testnet
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
client.API_URL = 'https://testnet.binance.vision/api'

# Función para obtener datos históricos
def get_historical_data(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    try:
        logging.info(f"Obteniendo datos históricos para {symbol} con intervalo {interval} y límite {limit}")
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)

        if not klines:
            raise ValueError(f"No se obtuvieron datos históricos para el símbolo {symbol}.")

        # Convertir los datos a un DataFrame
        data = pd.DataFrame(klines, columns=[
            'datetime', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])

        # Convertir 'datetime' a datetime y establecerlo como índice
        data['datetime'] = pd.to_datetime(data['datetime'], unit='ms')
        data.set_index('datetime', inplace=True)

        # Seleccionar las columnas relevantes y convertir los tipos de datos
        data = data[['open', 'high', 'low', 'close', 'volume']].astype(float)

        logging.info(f"Datos históricos obtenidos exitosamente para {symbol}")
        return data
    except Exception as e:
        logging.error(f"Error al obtener datos históricos para {symbol}: {e}")
        raise e


# Bloque principal para ejecutar el cliente de Binance
if __name__ == "__main__":
    try:
        # Obtener datos históricos de ejemplo
        symbol = 'BTCUSDT'
        interval = '5m'
        limit = 1000
        historical_data = get_historical_data(symbol, interval, limit)
        print(historical_data.head())
    except Exception as e:
        logging.error(f"Error en la ejecución del script principal: {e}")