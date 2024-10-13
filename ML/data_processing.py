import pandas as pd
import ta
from api.binance_connector import get_historical_data

def prepare_dataset(symbol: str, interval: str, limit: int):
    df = get_historical_data(symbol, interval, limit)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']]

    df['ema_12'] = ta.trend.EMAIndicator(close=df['close'], window=12).ema_indicator()
    df['ema_26'] = ta.trend.EMAIndicator(close=df['close'], window=26).ema_indicator()
    df['rsi'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()
    df['macd'] = ta.trend.MACD(close=df['close']).macd()

    bollinger = ta.volatility.BollingerBands(close=df['close'])
    df['bollinger_hband'] = bollinger.bollinger_hband()
    df['bollinger_lband'] = bollinger.bollinger_lband()

    df.dropna(inplace=True)
    return df

def label_data(df: pd.DataFrame):
    df['future_close'] = df['close'].shift(-1)
    df['signal'] = 0
    df.loc[df['future_close'] > df['close'], 'signal'] = 1
    df.loc[df['future_close'] < df['close'], 'signal'] = -1
    df.dropna(inplace=True)
    return df
