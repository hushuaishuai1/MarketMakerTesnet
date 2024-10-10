import pandas as pd
import numpy as np
import logging
from api.binance_connector import get_historical_data  # Asegúrate de que esta ruta sea correcta
import talib as ta
from sqlalchemy import create_engine
from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import StandardScaler
import joblib

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_price_data(symbol: str, interval: str, limit: int = 1000):
    """
    Obtiene datos históricos de precios desde la API de Binance.
    """
    logging.info(f"Obteniendo datos históricos para {symbol} con intervalo {interval}")
    df = get_historical_data(symbol, interval, limit)
    if df.empty:
        raise ValueError("No se obtuvieron datos históricos.")
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    return df

def calculate_technical_indicators(df: pd.DataFrame):
    """
    Calcula indicadores técnicos y los agrega al DataFrame.
    """
    df['ema_fast'] = ta.EMA(df['close'], timeperiod=8)
    df['ema_slow'] = ta.EMA(df['close'], timeperiod=35)
    df['rsi'] = ta.RSI(df['close'], timeperiod=14)
    df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    df['macd'], df['macd_signal'], df['macd_hist'] = ta.MACD(df['close'])
    df['upper_band'], df['middle_band'], df['lower_band'] = ta.BBANDS(df['close'], timeperiod=20)
    df['momentum'] = ta.MOM(df['close'], timeperiod=10)
    df['volatility'] = df['close'].rolling(window=10).std()
    return df

def generate_target_variable(df: pd.DataFrame, horizon: int = 1):
    """
    Genera la variable objetivo: 1 si el precio sube en el siguiente periodo, 0 si baja.
    """
    df['future_close'] = df['close'].shift(-horizon)
    df['target'] = np.where(df['future_close'] > df['close'], 1, 0)
    df.dropna(inplace=True)
    return df

def preprocess_data(df: pd.DataFrame):
    """
    Realiza el preprocesamiento de los datos:
    - Elimina filas con valores NaN.
    - Normaliza las características numéricas.
    - Maneja el desequilibrio de clases.
    """
    df.dropna(inplace=True)
    X = df.drop(['future_close', 'target'], axis=1)
    y = df['target']
    numeric_features = X.select_dtypes(include=[np.number])
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(numeric_features)
    X_scaled = pd.DataFrame(X_scaled, index=numeric_features.index, columns=numeric_features.columns)
    smote = SMOTE()
    X_resampled, y_resampled = smote.fit_resample(X_scaled, y)
    df_resampled = X_resampled.copy()
    df_resampled['target'] = y_resampled
    df_resampled.index = X.index[smote.sample_indices_]
    joblib.dump(scaler, 'scaler.joblib')
    logging.info("Scaler guardado en scaler.joblib.")
    return df_resampled

def save_to_database(df: pd.DataFrame, table_name: str, db_uri: str):
    """
    Guarda el DataFrame en una base de datos utilizando SQLAlchemy.
    """
    engine = create_engine(db_uri)
    df.to_sql(table_name, engine, if_exists='replace')
    logging.info(f"Datos guardados en la tabla {table_name} de la base de datos.")

if __name__ == "__main__":
    symbol = 'BTCUSDT'
    interval = '5m'
    limit = 1000  # Puedes ajustar el límite de acuerdo a tus necesidades
    db_uri = 'postgresql://username:password@localhost:5432/yourdatabase'  # Reemplaza con tu URI de base de datos

    # Paso 1: Obtener datos de precios
    df_prices = get_price_data(symbol, interval, limit)

    # Paso 2: Calcular indicadores técnicos
    df_with_indicators = calculate_technical_indicators(df_prices)

    # Paso 3: Generar variable objetivo
    df_with_target = generate_target_variable(df_with_indicators)

    # Paso 4: Preprocesar datos
    df_processed = preprocess_data(df_with_target)

    # Paso 5: Guardar en la base de datos
    save_to_database(df_processed, 'trading_data', db_uri)
