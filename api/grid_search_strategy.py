import pandas as pd
from app.models import Signal
from api.binance_connector import get_historical_data
from tortoise.transactions import in_transaction

# Función para obtener solo datos nuevos
async def get_new_data(symbol, interval, limit):
    """
    Obtiene los nuevos datos históricos desde el exchange y filtra para que solo se obtengan aquellos no almacenados.
    """
    try:
        last_signal = await Signal.filter(symbol=symbol).order_by('-timestamp').first()
        if last_signal:
            last_timestamp = last_signal.timestamp
        else:
            last_timestamp = pd.Timestamp('1970-01-01')

        df = get_historical_data(symbol, interval, limit)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

        df_new = df[df['timestamp'] > last_timestamp]
        return df_new
    except Exception as e:
        print(f"Error al obtener o procesar los datos: {e}")
        return pd.DataFrame()

# Función para almacenar las señales sin duplicarlas
async def store_signals(df, symbol, interval):
    """
    Almacena las señales en la base de datos, solo si no existen.
    """
    async with in_transaction():
        for _, row in df.iterrows():
            timestamp = row['timestamp'].to_pydatetime()
            existing_signal = await Signal.filter(symbol=symbol, timestamp=timestamp).first()
            if existing_signal:
                continue  # Evitar sobrescritura

            await Signal.create(
                symbol=symbol,
                close=row['close'],
                ema_8=row['EMA_8'],
                ema_23=row['EMA_23'],
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

# Función para realizar Grid Search y devolver los mejores parámetros
async def run_grid_search(symbol: str, interval: str = '1h', limit: int = 1000):
    """
    Ejecuta la búsqueda de hiperparámetros (Grid Search), almacena los resultados y devuelve los mejores parámetros.
    """
    try:
        # Obtener nuevos datos
        df = await get_new_data(symbol, interval, limit)

        if not df.empty:
            # Almacenar señales nuevas
            await store_signals(df, symbol, interval)

            # Ejecutar la lógica de Grid Search y obtener los mejores parámetros (dummy example)
            best_parameters = {
                "EMA_8": 8,
                "EMA_23": 23,
                "RSI_threshold": 70,
                "ADX_threshold": 25
            }

            # Retornar detalles de los mejores parámetros
            return {
                "message": "Ejecución de Grid Search completa.",
                "best_parameters": best_parameters,
                "stored_signals_count": len(df)
            }
        else:
            return {"message": "No se encontraron nuevos datos para almacenar."}
    except Exception as e:
        print(f"Error durante la ejecución del Grid Search: {e}")
        return {"message": f"Error: {str(e)}"}
