# ML/data_coin.py

import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import ta
import time
from typing import List, Optional
import os
import sys
from datetime import datetime, timedelta

# Configuración de logging
logger = logging.getLogger('data_coin')
logger.setLevel(logging.INFO)

# Formato de log
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler para archivo de log
file_handler = logging.FileHandler('ML/logs/data_coin.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Handler para consola
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def interval_to_milliseconds(interval: str) -> int:
    """
    Convertir un intervalo de tiempo a milisegundos.
    
    Args:
        interval (str): Intervalo de tiempo (e.g., '15m', '1h').
    
    Returns:
        int: Intervalo en milisegundos.
    """
    ms = None
    if interval.endswith('m'):
        ms = int(interval[:-1]) * 60 * 1000
    elif interval.endswith('h'):
        ms = int(interval[:-1]) * 60 * 60 * 1000
    elif interval.endswith('d'):
        ms = int(interval[:-1]) * 24 * 60 * 60 * 1000
    elif interval.endswith('w'):
        ms = int(interval[:-1]) * 7 * 24 * 60 * 60 * 1000
    elif interval.endswith('M'):
        ms = int(interval[:-1]) * 30 * 24 * 60 * 60 * 1000
    else:
        raise ValueError(f"Intervalo de tiempo no soportado: {interval}")
    return ms

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, start_ts: int, end_ts: Optional[int] = None, limit: int = 1000) -> List[List]:
    """
    Obtener klines de Binance API con reintentos en caso de fallos.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP asíncrona.
        symbol (str): Símbolo de trading (e.g., 'BTCUSDT').
        interval (str): Intervalo de las velas (e.g., '15m').
        start_ts (int): Timestamp de inicio en milisegundos.
        end_ts (int, optional): Timestamp de fin en milisegundos.
        limit (int): Número máximo de velas por solicitud.
    
    Returns:
        List[List]: Lista de klines obtenidas.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"  # Endpoint para Binance Futures
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_ts,
        'limit': limit
    }
    if end_ts:
        params['endTime'] = end_ts

    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                if isinstance(data, list):
                    return data
                else:
                    logger.error(f"Respuesta inesperada para {symbol}: {data}")
                    return []
        except Exception as e:
            logger.error(f"Error al obtener datos para {symbol} (Intento {attempt+1}/{max_retries}): {e}")
            await asyncio.sleep(2)
    logger.error(f"No se pudo obtener datos para {symbol} después de {max_retries} intentos.")
    return []

async def get_historical_data(symbol: str, interval: str, start_str: str, end_str: Optional[str] = None, max_candles: int = 50000, zscore_threshold: float = 3.0) -> pd.DataFrame:
    """
    Obtener datos históricos desde la API de Binance entre fechas específicas.
    
    Args:
        symbol (str): El par de trading, por ejemplo, 'BTCUSDT'.
        interval (str): El intervalo para las velas, por ejemplo, '15m'.
        start_str (str): Fecha de inicio en formato '1 Jan, 2021'.
        end_str (str, optional): Fecha de fin en formato '1 Jan, 2021'. Si se omite, obtiene datos hasta el presente.
        max_candles (int): Número máximo de velas a obtener.
        zscore_threshold (float): Umbral de Z-score para detección de outliers.
    
    Returns:
        pd.DataFrame: Un DataFrame con datos históricos OHLCV y cálculos de indicadores.
    """
    logger.info(f"Obteniendo datos históricos para {symbol} desde {start_str} hasta {end_str}")
    limit = 1000  # Máximo permitido por solicitud
    timeframe = interval_to_milliseconds(interval)
    
    try:
        start_ts = int(pd.to_datetime(start_str).timestamp() * 1000)
    except Exception as e:
        logger.error(f"Error al convertir start_str '{start_str}' a timestamp: {e}")
        return pd.DataFrame()
    
    try:
        end_ts = int(pd.to_datetime(end_str).timestamp() * 1000) if end_str else int(datetime.now().timestamp() * 1000)
    except Exception as e:
        logger.error(f"Error al convertir end_str '{end_str}' a timestamp: {e}")
        return pd.DataFrame()

    klines = []
    total_fetched = 0

    async with aiohttp.ClientSession() as session:
        while total_fetched < max_candles and start_ts < end_ts:
            fetch_limit = min(limit, (max_candles - total_fetched))
            data = await fetch_klines(session, symbol, interval, start_ts, end_ts, fetch_limit)
            if not data:
                logger.info(f"No se obtuvieron más datos para {symbol}.")
                break

            klines.extend(data)
            fetched = len(data)
            total_fetched += fetched
            logger.info(f"Obtenidas {fetched} velas para {symbol}. Total: {total_fetched}/{max_candles}")

            # Actualizar el timestamp de inicio para la siguiente solicitud
            last_open_time = data[-1][0]
            start_ts = last_open_time + timeframe

            # Evitar exceder las llamadas a la API
            await asyncio.sleep(0.2)

    if not klines:
        logger.error(f"No se obtuvieron datos para {symbol}.")
        return pd.DataFrame()

    # Parsear los datos
    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])

    # Convertir tipos de datos
    try:
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df[numeric_columns] = df[numeric_columns].astype(float)
    except Exception as e:
        logger.error(f"Error al convertir tipos de datos para {symbol}: {e}")
        return pd.DataFrame()

    # Establecer el índice
    df.set_index('open_time', inplace=True)

    # Eliminar duplicados
    df = df[~df.index.duplicated(keep='first')]

    # Manejo de valores faltantes y outliers
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    initial_len = len(df)
    df.dropna(inplace=True)
    final_len = len(df)
    if final_len < initial_len:
        logger.warning(f"Se eliminaron {initial_len - final_len} filas con valores faltantes para {symbol}.")

    # Detección y tratamiento de outliers usando Z-score
    df['zscore_close'] = (df['close'] - df['close'].mean()) / df['close'].std()
    initial_len = len(df)
    df = df[df['zscore_close'].abs() <= zscore_threshold]  # Mantener valores dentro del umbral
    final_len = len(df)
    if final_len < initial_len:
        logger.warning(f"Se eliminaron {initial_len - final_len} outliers basados en Z-score para {symbol}.")
    df.drop('zscore_close', axis=1, inplace=True)

    # Calcular indicadores técnicos esenciales y avanzados
    try:
        # Bollinger Bands
        bollinger = ta.volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
        df['bollinger_mavg'] = bollinger.bollinger_mavg()
        df['bollinger_hband'] = bollinger.bollinger_hband()
        df['bollinger_lband'] = bollinger.bollinger_lband()

        # RSI
        df['rsi'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()

        # ATR para gestión de riesgos
        df['atr'] = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()

        # MACD
        macd = ta.trend.MACD(close=df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()

        # Log Retornos
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))

        # Volatilidad Histórica
        df['volatility'] = df['log_return'].rolling(window=30).std() * np.sqrt(30)
    except Exception as e:
        logger.error(f"Error al calcular indicadores técnicos para {symbol}: {e}")
        return pd.DataFrame()

    # Eliminar filas con NaN generadas por los indicadores
    initial_len = len(df)
    df.dropna(inplace=True)
    final_len = len(df)
    if final_len < initial_len:
        logger.warning(f"Se eliminaron {initial_len - final_len} filas con NaN de indicadores para {symbol}.")

    # Verificar calidad de los datos
    if df.empty:
        logger.error(f"Después de calcular indicadores, el DataFrame para {symbol} está vacío.")
        return pd.DataFrame()
    else:
        logger.info(f"Datos para {symbol} procesados correctamente. Total de filas: {len(df)}")

    return df

async def fetch_multiple_symbols(symbols: List[str], interval: str, start_str: str, end_str: Optional[str] = None, max_candles: int = 50000, zscore_threshold: float = 3.0) -> pd.DataFrame:
    """
    Obtener datos históricos para múltiples símbolos y consolidarlos en un solo DataFrame.
    
    Args:
        symbols (List[str]): Lista de símbolos de trading.
        interval (str): Intervalo de las velas (e.g., '15m').
        start_str (str): Fecha de inicio en formato '1 Jan, 2021'.
        end_str (str, optional): Fecha de fin en formato '1 Jan, 2021'. Si se omite, obtiene datos hasta el presente.
        max_candles (int): Número máximo de velas por símbolo.
        zscore_threshold (float): Umbral de Z-score para detección de outliers.
    
    Returns:
        pd.DataFrame: DataFrame combinado con datos de todos los símbolos.
    """
    logger.info(f"Comenzando a obtener datos para múltiples símbolos: {symbols}")
    tasks = []
    for symbol in symbols:
        tasks.append(get_historical_data(symbol, interval, start_str, end_str, max_candles, zscore_threshold))
    dfs = await asyncio.gather(*tasks)
    data_frames = []
    for df, symbol in zip(dfs, symbols):
        if not df.empty:
            # Renombrar columnas para tener un multiíndice
            df_renamed = df.copy()
            df_renamed.columns = pd.MultiIndex.from_product([[symbol], df.columns])
            data_frames.append(df_renamed)
        else:
            logger.warning(f"Datos vacíos para {symbol}.")
    if data_frames:
        combined_df = pd.concat(data_frames, axis=1)

        # Sincronización de series temporales: asegurar alineación temporal
        combined_df = combined_df.sort_index()
        combined_df.index = pd.to_datetime(combined_df.index)  # Asegurar que el índice es datetime
        pandas_freq = interval.replace('m', 'T')  # Cambiar 'm' a 'T' para minutos
        combined_df = combined_df.asfreq(pandas_freq)

        # Convertir columnas a tipos numéricos e interpolar solo las numéricas
        combined_df = combined_df.apply(pd.to_numeric, errors='coerce')
        numeric_cols = combined_df.select_dtypes(include=[np.number]).columns
        combined_df[numeric_cols] = combined_df[numeric_cols].interpolate(method='time')

        # Manejar cualquier NaN residual
        initial_len = len(combined_df)
        combined_df.dropna(inplace=True)
        final_len = len(combined_df)
        if final_len < initial_len:
            logger.warning(f"Se eliminaron {initial_len - final_len} filas con valores faltantes después de la interpolación.")

        logger.info("Datos históricos combinados para múltiples símbolos.")
        return combined_df
    else:
        logger.error("No se obtuvieron datos para ninguno de los símbolos.")
        return pd.DataFrame()

def create_directories():
    """
    Crear directorios necesarios si no existen.
    """
    directories = ['ML/logs', 'ML/data', 'ML/results', 'ML/models']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    logger.info("Directorios necesarios asegurados.")

def save_combined_data(combined_df: pd.DataFrame, file_path: str = 'ML/data/combined_data.csv'):
    """
    Guardar el DataFrame combinado en un archivo CSV.
    
    Args:
        combined_df (pd.DataFrame): DataFrame combinado.
        file_path (str): Ruta del archivo de salida.
    """
    try:
        combined_df.to_csv(file_path)
        logger.info(f"Datos combinados guardados en {file_path}.")
    except Exception as e:
        logger.error(f"Error al guardar datos combinados: {e}")

async def main():
    """
    Función principal para ejecutar la obtención y procesamiento de datos.
    """
    # Lista de símbolos de alta liquidez en Binance Futures
    symbols = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
        'DOGEUSDT', 'SOLUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
        'TRXUSDT', 'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'ETCUSDT'
    ]

    # Definir intervalo y fechas
    interval = '15m'
    end_str = datetime.now().strftime('%d %b, %Y')  # Fecha actual
    start_str = (datetime.now() - timedelta(days=365)).strftime('%d %b, %Y')  # 1 año atrás
    max_candles = 50000  # Número máximo de velas por símbolo
    zscore_threshold = 3.0  # Umbral de Z-score para detección de outliers

    # Crear carpetas necesarias si no existen
    create_directories()

    # Obtener y procesar datos históricos
    combined_df = await fetch_multiple_symbols(symbols, interval, start_str, end_str, max_candles, zscore_threshold)

    if combined_df.empty:
        logger.error("No se pudieron obtener datos históricos combinados.")
        return

    # Guardar el DataFrame combinado en un archivo CSV
    save_combined_data(combined_df, 'ML/data/combined_data.csv')

    logger.info("Obtención y procesamiento de datos completados exitosamente.")

# Permitir que el archivo se importe como módulo sin ejecutar el main
if __name__ == '__main__':
    asyncio.run(main())
