import pandas as pd
import numpy as np
import logging
from api.binance_connector import get_historical_data
from app.models import Signal
from tortoise.transactions import in_transaction

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MomentumStrategy:
    def __init__(self, symbol: str, interval: str = '1h', limit: int = 1000, 
                 min_rsi: int = 20, max_rsi: int = 80, ema_fast_period: int = 8, 
                 ema_slow_period: int = 50, atr_mult_sl: float = 2, atr_mult_tp: float = 3):
        self.symbol = symbol
        self.interval = interval
        self.limit = limit
        self.min_rsi = min_rsi
        self.max_rsi = max_rsi
        self.ema_fast_period = ema_fast_period
        self.ema_slow_period = ema_slow_period
        self.atr_mult_sl = atr_mult_sl
        self.atr_mult_tp = atr_mult_tp

    async def run(self):
        # Obtener datos históricos
        df = get_historical_data(self.symbol, self.interval, self.limit)
        logging.info(f"Datos obtenidos para {self.symbol} en el intervalo {self.interval}")

        # Asegurar que los datos son float y eliminar filas con valores NaT o NaN
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        df = df.dropna(subset=['timestamp', 'close', 'high', 'low', 'volume'])

        # Calcular EMA
        df[f'EMA_{self.ema_fast_period}'] = df['close'].ewm(span=self.ema_fast_period, adjust=False).mean()
        df[f'EMA_{self.ema_slow_period}'] = df['close'].ewm(span=self.ema_slow_period, adjust=False).mean()

        # Calcular MACD
        df['MACD'] = df[f'EMA_{self.ema_fast_period}'] - df[f'EMA_{self.ema_slow_period}']
        df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # Calcular RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        average_gain = gain.rolling(window=14).mean()
        average_loss = loss.rolling(window=14).mean()
        rs = average_gain / average_loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # Calcular ATR
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
        df['ADX'] = df['DX'].rolling(window=14).mean()

        # Obtener datos de la temporalidad superior (4h)
        df_higher = get_historical_data(self.symbol, '4h', self.limit)
        df_higher['EMA_50'] = df_higher['close'].ewm(span=50, adjust=False).mean()
        df_higher['EMA_200'] = df_higher['close'].ewm(span=200, adjust=False).mean()

        # Alinear temporalidades
        df_higher.set_index('timestamp', inplace=True)
        df.set_index('timestamp', inplace=True)
        df = df.join(df_higher[['EMA_50', 'EMA_200']], how='left', rsuffix='_higher')

        # Calcular la tendencia de la temporalidad superior (higher_trend)
        df['Higher_Trend'] = np.where(df['EMA_50'] > df['EMA_200'], 'bullish', 'bearish')
        df['Higher_Trend'].fillna('unknown', inplace=True)

        # Generar señales de trading
        df['signal'] = 0
        df.loc[
            (df[f'EMA_{self.ema_fast_period}'] > df[f'EMA_{self.ema_slow_period}']) &
            (df['RSI'] < self.max_rsi) &
            (df['ADX'] > 25) &
            (df['Higher_Trend'] == 'bullish'),
            'signal'
        ] = 1

        df.loc[
            (df[f'EMA_{self.ema_fast_period}'] < df[f'EMA_{self.ema_slow_period}']) &
            (df['RSI'] > self.min_rsi) &
            (df['ADX'] > 25) &
            (df['Higher_Trend'] == 'bearish'),
            'signal'
        ] = -1

        # Guardar señales en la base de datos
        try:
            async with in_transaction():
                for index, row in df.iterrows():
                    timestamp = row['timestamp'].to_pydatetime() if isinstance(row['timestamp'], pd.Timestamp) else row['timestamp']
                    await Signal.create(
                        symbol=self.symbol,
                        close=row['close'],
                        ema_8=row[f'EMA_{self.ema_fast_period}'],
                        ema_50=row[f'EMA_{self.ema_slow_period}'],
                        macd=row['MACD'],
                        signal_line=row['Signal_Line'],
                        rsi=row['RSI'],
                        adx=row['ADX'],
                        volume=row['volume'],
                        higher_trend=row['Higher_Trend'],
                        signal=row['signal'],
                        timestamp=timestamp,
                        interval=self.interval
                    )
        except Exception as e:
            raise Exception(f"Error guardando señales en la base de datos: {e}")

        return df
