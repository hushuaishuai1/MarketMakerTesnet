import itertools
import pandas as pd
from app.models import Signal, BestParams
from api.binance_connector import get_historical_data
from tortoise.transactions import in_transaction
import numpy as np

# Definir las combinaciones de parámetros con EMA 8 y EMA 23
def grid_search_combinations():
    ema_8_values = [8, 12, 16]
    ema_23_values = [23, 30, 50]
    rsi_values = [30, 50, 70]
    adx_values = [20, 25, 30]
    
    for combination in itertools.product(ema_8_values, ema_23_values, rsi_values, adx_values):
        yield combination

# Evaluar la estrategia de trading
def evaluate_strategy(df, ema_8, ema_23, rsi, adx):
    capital_inicial = 10000
    capital_actual = capital_inicial
    n_operaciones = 0
    n_ganadoras = 0
    ganancias_totales = 0
    pérdidas_totales = 0
    drawdown = 0
    capital_maximo = capital_inicial

    for index, row in df.iterrows():
        if row['signal'] == 1:  # Compra
            n_operaciones += 1
            precio_entrada = row['close']
            for i in range(index + 1, len(df)):
                if df.iloc[i]['signal'] == -1:  # Venta
                    precio_salida = df.iloc[i]['close']
                    break
            else:
                precio_salida = df.iloc[-1]['close']

            ganancia_perdida = (precio_salida - precio_entrada) / precio_entrada * capital_actual
            capital_actual += ganancia_perdida
            capital_maximo = max(capital_maximo, capital_actual)
            drawdown = max(drawdown, (capital_maximo - capital_actual) / capital_maximo)

            if ganancia_perdida > 0:
                n_ganadoras += 1
                ganancias_totales += ganancia_perdida
            else:
                pérdidas_totales += abs(ganancia_perdida)

    roi = (capital_actual - capital_inicial) / capital_inicial * 100
    win_rate = (n_ganadoras / n_operaciones * 100) if n_operaciones > 0 else 0
    risk_reward_ratio = (ganancias_totales / n_ganadoras) / (pérdidas_totales / (n_operaciones - n_ganadoras)) if n_ganadoras > 0 and n_operaciones > n_ganadoras else None

    return {
        "capital_final": capital_actual,
        "roi": roi,
        "win_rate": win_rate,
        "max_drawdown": drawdown,
        "risk_reward_ratio": risk_reward_ratio
    }

# Obtener nuevos datos
async def get_new_data(symbol, interval, limit):
    last_signal = await Signal.filter(symbol=symbol).order_by('-timestamp').first()
    last_timestamp = last_signal.timestamp if last_signal else pd.Timestamp('1970-01-01')

    df = get_historical_data(symbol, interval, limit)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df_new = df[df['timestamp'] > last_timestamp]

    return df_new

# Guardar señales en la base de datos
async def store_signals(df, symbol, interval):
    async with in_transaction():
        for _, row in df.iterrows():
            timestamp = row['timestamp'].to_pydatetime()

            existing_signal = await Signal.filter(symbol=symbol, timestamp=timestamp).first()
            if existing_signal:
                continue

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

# Guardar los mejores parámetros
async def store_best_parameters(symbol, best_params, interval):
    await BestParams.create(
        symbol=symbol,
        interval=interval,
        EMA_8=best_params[0],
        EMA_23=best_params[1],
        RSI_threshold=best_params[2],
        ADX_threshold=best_params[3]
    )

# Ejecutar Grid Search
async def run_grid_search(symbol: str, interval: str = '1h', limit: int = 1000):
    try:
        df = await get_new_data(symbol, interval, limit)

        if not df.empty:
            await store_signals(df, symbol, interval)

            best_combination = None
            best_performance = -float("inf")

            for ema_8, ema_23, rsi, adx in grid_search_combinations():
                df['EMA_8'] = df['close'].ewm(span=ema_8, adjust=False).mean()
                df['EMA_23'] = df['close'].ewm(span=ema_23, adjust=False).mean()
                df['MACD'] = df['EMA_8'] - df['EMA_23']
                df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
                df['RSI'] = df['close'].rolling(window=rsi).mean()
                df['ADX'] = df['ADX'].rolling(window=adx).mean()

                performance = evaluate_strategy(df, ema_8, ema_23, rsi, adx)

                if performance["roi"] > best_performance:
                    best_performance = performance["roi"]
                    best_combination = (ema_8, ema_23, rsi, adx)

            await store_best_parameters(symbol, best_combination, interval)
            return {"message": "Grid Search completo", "best_params": best_combination, "performance": best_performance}
        else:
            return {"message": "No se encontraron nuevos datos para almacenar."}
    
    except Exception as e:
        return {"message": f"Error: {str(e)}"}
