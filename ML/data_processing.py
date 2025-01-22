import asyncio
import aiohttp
import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import os
import sys
from datetime import datetime, timedelta

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Configuración: en esta versión, eliminamos VWAP, ATR, CCI y SMA_200 para simplificar.
config = {
    'data': {
        'symbols': ['BTCUSDT'],
        'interval': '15m',
        'start_date': '2016-06-07',
        'end_date': '2025-01-10',
        'max_candles': 400000
    },
    'output': {
        'processed_data_dir': 'ML/data/processed',
        'log_dir': 'logs',
    }
}

# Lista de columnas a conservar (sin VWAP, ATR, CCI, SMA_200)
FIXED_FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'RSI', 'MACD', 'MACDs', 'MACDh',
    'SMA_10', 'SMA_50', 'EMA_10', 'EMA_50',
    'OBV', 'BBL', 'BBM', 'BBU'
]

def setup_logging(log_dir: str) -> logging.Logger:
    logger = logging.getLogger('fetch_and_process_data')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'data_process.log')

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging(os.path.join(BASE_PATH, config['output']['log_dir']))

def interval_to_milliseconds(interval: str) -> int:
    unit = interval[-1]
    try:
        value = int(interval[:-1])
    except ValueError:
        raise ValueError(f"Formato de intervalo inválido: {interval}")
    
    if unit == 'm':
        return value * 60 * 1000
    elif unit == 'h':
        return value * 60 * 60 * 1000
    elif unit == 'd':
        return value * 24 * 60 * 60 * 1000
    elif unit == 'w':
        return value * 7 * 24 * 60 * 60 * 1000
    elif unit == 'M':
        return value * 30 * 24 * 60 * 60 * 1000
    else:
        raise ValueError(f"Intervalo de tiempo no soportado: {interval}")

async def fetch_klines(session, symbol, interval, start_ts, end_ts, limit=1000) -> list:
    url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_ts,
        'endTime': end_ts,
        'limit': limit
    }
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

async def get_historical_data(symbol: str, interval: str, start_str: str, end_str: str, max_candles: int) -> pd.DataFrame:
    logger.info(f"Obteniendo datos para {symbol} desde {start_str} hasta {end_str}")
    limit = 1000
    timeframe = interval_to_milliseconds(interval)

    start_ts = int(pd.to_datetime(start_str).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_str).timestamp() * 1000)
    klines = []
    total_fetched = 0

    async with aiohttp.ClientSession() as session:
        while total_fetched < max_candles and start_ts < end_ts:
            fetch_limit = min(limit, max_candles - total_fetched)
            data = await fetch_klines(session, symbol, interval, start_ts, end_ts, fetch_limit)
            if not data:
                break

            klines.extend(data)
            total_fetched += len(data)
            last_open_time = data[-1][0]
            start_ts = last_open_time + timeframe
            await asyncio.sleep(0.2)

    if not klines:
        logger.warning(f"No se obtuvieron datos para {symbol} en ese rango.")
        return pd.DataFrame()

    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    return df

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores técnicos básicos con pandas_ta.
    Se conservan únicamente las columnas definidas en FIXED_FEATURES.
    """
    if df.empty:
        logger.warning("DataFrame vacío; no se calcularán indicadores.")
        return df

    df = df.copy()
    df.sort_values(by='open_time', inplace=True)
    df.set_index('open_time', inplace=True)

    # Convertir columnas a float
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)

    # Calcular indicadores básicos
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.sma(length=10, append=True)
    df.ta.sma(length=50, append=True)
    df.ta.ema(length=10, append=True)
    df.ta.ema(length=50, append=True)
    df.ta.obv(append=True)
    df.ta.bbands(length=20, std=2, append=True)

    # Eliminar filas con NaN en OHLCV
    df.dropna(subset=['open','high','low','close','volume'], how='any', inplace=True)

    rename_dict = {
        'RSI_14': 'RSI',
        'MACD_12_26_9': 'MACD',
        'MACDs_12_26_9': 'MACDs',
        'MACDh_12_26_9': 'MACDh',
        'BBL_20_2.0': 'BBL',
        'BBM_20_2.0': 'BBM',
        'BBU_20_2.0': 'BBU',
        'SMA_10': 'SMA_10',
        'SMA_50': 'SMA_50',
        'EMA_10': 'EMA_10',
        'EMA_50': 'EMA_50'
    }
    df.rename(columns=rename_dict, inplace=True)

    for col in FIXED_FEATURES:
        if col not in df.columns:
            logger.warning(f"La columna '{col}' no se generó, se rellena con NaN.")
            df[col] = np.nan

    df = df[FIXED_FEATURES]
    df.reset_index(inplace=True)
    return df

def get_last_saved_timestamp(symbol: str, interval: str) -> datetime:
    """
    Retorna el último timestamp válido guardado en el CSV.
    Si no existe o es inválido, retorna config['data']['start_date'].
    """
    processed_file = os.path.join(
        config['output']['processed_data_dir'],
        f"{symbol}_{interval}_processed.csv"
    )
    if os.path.exists(processed_file):
        try:
            df_existing = pd.read_csv(processed_file, parse_dates=['open_time'])
        except Exception as e:
            logger.error(f"Error al leer el archivo {processed_file}: {e}")
            return pd.to_datetime(config['data']['start_date'])
        # Si el DataFrame está vacío o la columna 'open_time' es toda NaT, usar start_date
        if df_existing.empty or df_existing['open_time'].dropna().empty:
            return pd.to_datetime(config['data']['start_date'])
        last_time = df_existing['open_time'].dropna().max()
        if pd.isna(last_time):
            return pd.to_datetime(config['data']['start_date'])
        return last_time
    else:
        return pd.to_datetime(config['data']['start_date'])

async def main_async():
    os.makedirs(config['output']['processed_data_dir'], exist_ok=True)

    for symbol in config['data']['symbols']:
        last_timestamp = get_last_saved_timestamp(symbol, config['data']['interval'])
        if pd.isna(last_timestamp):
            last_timestamp = pd.to_datetime(config['data']['start_date'])
        start_ts_str = (last_timestamp + timedelta(milliseconds=1)).strftime('%Y-%m-%d %H:%M:%S')

        df_raw = await get_historical_data(
            symbol=symbol,
            interval=config['data']['interval'],
            start_str=start_ts_str,
            end_str=config['data']['end_date'],
            max_candles=config['data']['max_candles']
        )

        if df_raw.empty:
            logger.warning(f"No se descargaron nuevos datos para {symbol}.")
            continue

        df_processed = calculate_indicators(df_raw)

        processed_file = os.path.join(
            config['output']['processed_data_dir'],
            f"{symbol}_{config['data']['interval']}_processed.csv"
        )

        if os.path.exists(processed_file):
            df_existing = pd.read_csv(processed_file, parse_dates=['open_time'])
            if not df_existing.empty:
                df_existing.set_index('open_time', inplace=True)
            else:
                df_existing = pd.DataFrame()
            if not df_processed.empty:
                df_processed.set_index('open_time', inplace=True)
            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_processed])
                df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
                df_combined.reset_index(inplace=True)
            else:
                df_combined = df_processed.reset_index()
        else:
            df_processed.reset_index(inplace=True)
            df_combined = df_processed

        df_combined.sort_values(by='open_time', inplace=True)
        df_combined.to_csv(processed_file, index=False)
        logger.info(f"Datos procesados (sin VWAP/ATR) guardados en {processed_file}")

def main():
    try:
        asyncio.run(main_async())
    except Exception as e:
        logger.error(f"Error en el preprocesamiento: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
