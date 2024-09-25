import pandas as pd
from api.binance_connector import get_historical_data
from app.models import Signal
from tortoise.transactions import in_transaction
import numpy as np
import requests

async def momentum_strategy(symbol: str, interval: str = '1h', limit: int = 1000, min_rsi: int = 20, max_rsi: int = 80, ema_fast_period: int = 8, ema_slow_period: int = 50, atr_mult_sl: float = 2, atr_mult_tp: float = 3):
    # Obtener datos históricos
    df = get_historical_data(symbol, interval, limit)

    # Asegurar que los datos son float, especialmente las columnas de precios
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    # Eliminar NaT en 'timestamp'
    df = df.dropna(subset=['timestamp'])

    # Verificar si hay valores NaN en columnas clave
    df = df.dropna(subset=['close', 'high', 'low', 'volume'])

    # Calcular EMA(ema_fast_period) y EMA(ema_slow_period) en la temporalidad base
    df[f'EMA_{ema_fast_period}'] = df['close'].ewm(span=ema_fast_period, adjust=False).mean()
    df[f'EMA_{ema_slow_period}'] = df['close'].ewm(span=ema_slow_period, adjust=False).mean()

    # Calcular MACD y su línea de señal
    df['MACD'] = df[f'EMA_{ema_fast_period}'] - df[f'EMA_{ema_slow_period}']
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()

    # Calcular RSI ajustado con umbrales min_rsi - max_rsi
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    average_gain = gain.rolling(window=14).mean()
    average_loss = loss.rolling(window=14).mean()
    rs = average_gain / average_loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # Calcular ATR para gestionar stop-loss y take-profit basados en volatilidad
    df['TR'] = np.maximum.reduce([
        df['high'] - df['low'],
        abs(df['high'] - df['close'].shift()),
        abs(df['low'] - df['close'].shift())
    ])
    df['ATR'] = df['TR'].rolling(window=14).mean()

    # Calcular ADX
    df['DM_plus'] = np.where(
        (df['high'] - df['high'].shift()) > (df['low'].shift() - df['low']),
        df['high'] - df['high'].shift(),
        0
    )
    df['DM_minus'] = np.where(
        (df['low'].shift() - df['low']) > (df['high'] - df['high'].shift()),
        df['low'].shift() - df['low'],
        0
    )
    df['TR_smooth'] = df['TR'].rolling(window=14).sum()
    df['DM_plus_smooth'] = df['DM_plus'].rolling(window=14).sum()
    df['DM_minus_smooth'] = df['DM_minus'].rolling(window=14).sum()
    df['DI_plus'] = 100 * (df['DM_plus_smooth'] / df['TR_smooth'])
    df['DI_minus'] = 100 * (df['DM_minus_smooth'] / df['TR_smooth'])
    df['DX'] = 100 * (abs(df['DI_plus'] - df['DI_minus']) / (df['DI_plus'] + df['DI_minus']))
    
    # Evitar valores nulos
    df['DX'] = df['DX'].fillna(0)
    df['ADX'] = df['DX'].rolling(window=14).mean()

    # Calcular Volumen Promedio (EMA del volumen)
    df['Volume_EMA'] = df['volume'].ewm(span=20, adjust=False).mean()

    # Obtener datos en temporalidad superior (4h)
    df_higher = get_historical_data(symbol, '4h', limit)
    
    # Convertir 'timestamp' a tz-naive
    df_higher['timestamp'] = pd.to_datetime(df_higher['timestamp'], unit='ms').dt.tz_localize(None)
    df_higher = df_higher.dropna(subset=['timestamp'])
    df_higher['EMA_50'] = df_higher['close'].ewm(span=50, adjust=False).mean()
    df_higher['EMA_200'] = df_higher['close'].ewm(span=200, adjust=False).mean()

    # Alinear temporalidades y calcular tendencia superior
    df_higher.set_index('timestamp', inplace=True)
    df.set_index('timestamp', inplace=True)
    df = df.join(df_higher[['EMA_50', 'EMA_200']], how='left', rsuffix='_higher')
    df.reset_index(inplace=True)

    df['Higher_Trend'] = np.where(df['EMA_50'] > df['EMA_200'], 'bullish', 'bearish')

    # Evitar valores nulos
    df.ffill(inplace=True)
    df.fillna(0, inplace=True)

    # Generar señales de trading con RSI ampliado
    df['signal'] = 0
    df.loc[
        (df[f'EMA_{ema_fast_period}'] > df[f'EMA_{ema_slow_period}']) &
        (df['RSI'] < max_rsi) &
        (df['ADX'] > 25) &
        (df['Higher_Trend'] == 'bullish') &
        (df['volume'] > df['Volume_EMA']),
        'signal'
    ] = 1

    df.loc[
        (df[f'EMA_{ema_fast_period}'] < df[f'EMA_{ema_slow_period}']) &
        (df['RSI'] > min_rsi) &
        (df['ADX'] > 25) &
        (df['Higher_Trend'] == 'bearish') &
        (df['volume'] > df['Volume_EMA']),
        'signal'
    ] = -1

    # Guardar señales en la base de datos
    async with in_transaction():
        for index, row in df.iterrows():
            timestamp = row['timestamp']
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.to_pydatetime()
            await Signal.create(
                symbol=symbol,
                close=row['close'],
                ema_8=row[f'EMA_{ema_fast_period}'],
                ema_23=row[f'EMA_{ema_slow_period}'],
                macd=row['MACD'],
                signal_line=row['Signal_Line'],
                rsi=row['RSI'],
                adx=row['ADX'],
                volume=row['volume'],
                higher_trend=row['Higher_Trend'],
                signal=row['signal'],
                timestamp=timestamp,
                interval=interval
            )

    return df
