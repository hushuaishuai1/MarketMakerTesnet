#!/usr/bin/env python3
"""
Script para ejecutar la estrategia en Binance Futures Testnet utilizando el modelo entrenado.

Este script:
  - Carga el modelo (pipeline) guardado tras el entrenamiento.
  - Se conecta a Binance Futures Testnet (usa las API keys proporcionadas).
  - Descarga datos históricos (velas de 15 minutos) y calcula indicadores técnicos necesarios.
  - Prepara los datos de entrada usando las mismas _features_ (FIXED_FEATURES) que en el entrenamiento.
  - Genera señales (usando predict_proba) y filtra por tendencia (SMA_50 > SMA_200).
  - Si la señal es de compra y no hay posición abierta, calcula el tamaño de la orden (según riesgo fijo y ATR) y abre una posición LONG.
  - Si la señal es de venta y existe una posición, la cierra con orden de mercado.
  - Espera hasta el cierre de la vela para repetir.
  
> **Aviso:** Este ejemplo es ilustrativo. Realiza pruebas exhaustivas en el testnet y ajusta parámetros de gestión de riesgo y conectividad.
"""

import time
import datetime
import logging
import numpy as np
import pandas as pd
import joblib
import pandas_ta as ta

from binance.client import Client
from binance.enums import *

# ---------------------------- CONFIGURACIÓN DE LOGGING ----------------------------
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# ---------------------------- CONFIGURACIÓN DE API ----------------------------
# Datos de Binance Futures Testnet (usa las claves proporcionadas)
API_KEY = "98dd90555b64331d55025f91e333ac0a55e127247dc3737d13e69461b420db44"
API_SECRET = "b205aad519990488f3750b1fccda3c3473b6f54b618db0b2bda104bce46bea1b"
client = Client(API_KEY, API_SECRET, testnet=True)

# ---------------------------- PARÁMETROS DE LA ESTRATEGIA ----------------------------
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
MODEL_PATH = "./ML/results/XGBoost_Binario_trained_pipeline.joblib"

# Lista fija de features usada en el entrenamiento
FIXED_FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'RSI', 'MACD', 'MACDs', 'MACDh',
    'SMA_10', 'SMA_50', 'EMA_10', 'EMA_50',
    'OBV', 'BBL', 'BBM', 'BBU',
    'STOCHk', 'STOCHd', 'CCI', 'SMA_200'
]

PROB_THRESHOLD = 0.6  # Umbral a partir del cual se considera señal de compra

# Parámetros para gestión del riesgo y stops (valores de ejemplo)
ATR_PERIOD     = 14
ATR_SL_MULT    = 0.21   # stop-loss = precio entrada - (ATR * 0.21)
ATR_TP_MULT    = 0.42   # take-profit = precio entrada + (ATR * 0.42)
SLIPPAGE_PCT   = 0.0001  # 0.01% de slippage aproximado
RISK_PER_TRADE = 0.01    # Riesgo del 1% del balance por operación

# ---------------------------- FUNCIONES AUXILIARES ----------------------------

def load_model(model_path):
    """Carga el modelo (pipeline) guardado."""
    try:
        model = joblib.load(model_path)
        logger.info(f"Modelo cargado desde {model_path}")
        return model
    except Exception as e:
        logger.error(f"Error al cargar el modelo: {e}")
        return None

def get_historical_klines(symbol, interval, limit=500):
    """
    Obtiene datos históricos (klines) de Binance Futures Testnet y retorna un DataFrame.
    """
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
        df["open"] = pd.to_numeric(df["open"])
        df["high"] = pd.to_numeric(df["high"])
        df["low"] = pd.to_numeric(df["low"])
        df["close"] = pd.to_numeric(df["close"])
        df["volume"] = pd.to_numeric(df["volume"])
        df.set_index("open_time", inplace=True)
        df.sort_index(inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error al obtener klines: {e}")
        return None

def add_technical_indicators(df):
    """
    Calcula indicadores técnicos usando pandas_ta de forma similar al preprocesado de entrenamiento.
    Se calculan RSI, MACD, medias, OBV, Bollinger Bands, STOCH, CCI y SMA_200.
    """
    df = df.copy()
    # Eliminar columnas previas de indicadores (si existen)
    for col in ['RSI', 'MACD', 'MACDs', 'MACDh', 'SMA_10', 'SMA_50', 
                'EMA_10', 'EMA_50', 'OBV', 'BBL', 'BBM', 'BBU', 'STOCHk', 'STOCHd', 'CCI', 'SMA_200']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    # Calcular indicadores
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.sma(length=10, append=True)
    df.ta.sma(length=50, append=True)
    df.ta.ema(length=10, append=True)
    df.ta.ema(length=50, append=True)
    df.ta.obv(append=True)
    df.ta.bbands(length=20, std=2, append=True)
    df.ta.stoch(append=True)
    df.ta.cci(length=14, append=True)
    df.ta.sma(length=200, append=True)
    # Renombrar columnas para que coincidan con FIXED_FEATURES
    rename_dict = {
        'RSI_14': 'RSI',
        'MACD_12_26_9': 'MACD',
        'MACDs_12_26_9': 'MACDs',
        'MACDh_12_26_9': 'MACDh',
        'SMA_10': 'SMA_10',
        'SMA_50': 'SMA_50',
        'EMA_10': 'EMA_10',
        'EMA_50': 'EMA_50',
        'OBV': 'OBV',
        'BBL_20_2.0': 'BBL',
        'BBM_20_2.0': 'BBM',
        'BBU_20_2.0': 'BBU',
        'STOCHk_14_3_3': 'STOCHk',
        'STOCHd_14_3_3': 'STOCHd',
        'CCI_14_0.015': 'CCI',
        'SMA_200': 'SMA_200'
    }
    df.rename(columns=rename_dict, inplace=True)
    # Asegurar que existan todas las columnas requeridas
    for col in FIXED_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    df.dropna(subset=FIXED_FEATURES, inplace=True)
    return df

def prepare_live_data(df):
    """
    Prepara los datos (cálculo de indicadores y selección de features) para alimentar al modelo.
    """
    df_prepared = add_technical_indicators(df)
    X = df_prepared[FIXED_FEATURES].copy()
    return X, df_prepared

def compute_atr(df, period=ATR_PERIOD):
    """
    Calcula el ATR (Average True Range) de forma sencilla para la gestión de riesgo.
    """
    df = df.copy()
    df['prev_close'] = df['close'].shift(1)
    df['tr'] = df.apply(lambda row: max(row['high']-row['low'], 
                                          abs(row['high']-row['prev_close']), 
                                          abs(row['low']-row['prev_close'])), axis=1)
    atr = df['tr'].rolling(period).mean()
    return atr.iloc[-1]

def get_account_balance():
    """
    Retorna el balance disponible (en USDT) en la cuenta de futures.
    """
    try:
        account_info = client.futures_account_balance()
        for asset in account_info:
            if asset['asset'] == 'USDT':
                return float(asset['balance'])
    except Exception as e:
        logger.error(f"Error al obtener balance: {e}")
    return None

def get_open_position():
    """
    Retorna información de posición abierta en SYMBOL (si existe).
    """
    try:
        positions = client.futures_position_information(symbol=SYMBOL)
        for pos in positions:
            if float(pos['positionAmt']) != 0:
                return pos
    except Exception as e:
        logger.error(f"Error al obtener posición: {e}")
    return None

def calculate_position_size(balance, entry_price, stop_loss):
    """
    Calcula el tamaño de la posición basado en el riesgo fijo (RISK_PER_TRADE).
    """
    risk_amount = balance * RISK_PER_TRADE
    risk_per_unit = entry_price - stop_loss
    if risk_per_unit <= 0:
        return 0
    qty = risk_amount / risk_per_unit
    return qty

def place_market_order(side, quantity):
    """
    Coloca una orden de mercado en Binance Futures.
    """
    try:
        order = client.futures_create_order(
            symbol=SYMBOL,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=quantity
        )
        logger.info(f"Orden {side} colocada: {order}")
        return order
    except Exception as e:
        logger.error(f"Error al colocar orden: {e}")
        return None

# ---------------------------- FUNCIÓN PRINCIPAL DE LA ESTRATEGIA ----------------------------

def run_strategy():
    model = load_model(MODEL_PATH)
    if model is None:
        logger.error("No se pudo cargar el modelo. Terminando ejecución.")
        return
    
    logger.info("Iniciando estrategia en Binance Futures Testnet...")
    
    while True:
        try:
            # 1. Obtener datos históricos recientes (últimas 500 velas)
            df = get_historical_klines(SYMBOL, TIMEFRAME, limit=500)
            if df is None or df.empty:
                logger.warning("No se pudieron obtener datos de velas.")
                time.sleep(60)
                continue
            
            # 2. Preparar datos (calcular indicadores y extraer FIXED_FEATURES)
            X, df_prepared = prepare_live_data(df)
            if X.empty:
                logger.warning("Datos insuficientes tras aplicar indicadores.")
                time.sleep(60)
                continue
            
            # 3. Usar la última vela para predecir la señal
            latest_data = X.iloc[[-1]]  # DataFrame de una sola fila
            signal_prob = model.predict_proba(latest_data)[0][1]
            predicted_class = 1 if signal_prob >= PROB_THRESHOLD else 0
            logger.info(f"Probabilidad de compra: {signal_prob:.4f} - Señal: {predicted_class}")
            
            # 4. Filtrar señal por tendencia: solo operar si SMA_50 > SMA_200 en la vela actual
            latest_indicator = df_prepared.iloc[-1]
            if latest_indicator['SMA_50'] <= latest_indicator['SMA_200']:
                logger.info("Tendencia bajista detectada (SMA_50 <= SMA_200). Se ignora señal de compra.")
                predicted_class = 0
            
            # 5. Verificar si hay posición abierta
            position = get_open_position()
            
            # 6. Si señal de compra y no hay posición, abrir posición LONG
            if predicted_class == 1 and position is None:
                entry_price = latest_indicator['close']
                atr = compute_atr(df)
                stop_loss = entry_price - ATR_SL_MULT * atr
                take_profit = entry_price + ATR_TP_MULT * atr
                balance = get_account_balance()
                if balance is None:
                    logger.error("No se pudo obtener balance, se omite la operación.")
                    time.sleep(60)
                    continue
                
                qty = calculate_position_size(balance, entry_price, stop_loss)
                qty = round(qty, 3)  # Ajustar la cantidad según la precisión permitida
                if qty <= 0:
                    logger.warning("Cantidad calculada <= 0, se omite la operación.")
                    time.sleep(60)
                    continue
                
                logger.info(f"Abrir posición LONG: Precio de entrada: {entry_price}, Stop Loss: {stop_loss}, Take Profit: {take_profit}, Cantidad: {qty}")
                place_market_order(SIDE_BUY, qty)
                # Aquí podrías programar la colocación de órdenes de stop y TP (si lo deseas)
                
            # 7. Si la señal es de venta y existe posición abierta, cerrar la posición
            elif predicted_class == 0 and position is not None:
                pos_qty = float(position['positionAmt'])
                if pos_qty > 0:
                    logger.info("Señal de venta detectada, cerrando posición LONG.")
                    place_market_order(SIDE_SELL, abs(pos_qty))
                # Si manejas posiciones cortas, agrega lógica similar
            
            else:
                logger.info("No se realizan acciones en este ciclo.")
            
            # 8. Esperar hasta el cierre de la siguiente vela
            now = datetime.datetime.utcnow()
            last_candle_time = df.index[-1]
            next_candle_time = last_candle_time + datetime.timedelta(minutes=15)
            sleep_seconds = (next_candle_time - now).total_seconds()
            if sleep_seconds < 0 or sleep_seconds > 900:
                sleep_seconds = 60  # Valor de respaldo
            logger.info(f"Esperando {sleep_seconds:.0f} segundos hasta la próxima vela.")
            time.sleep(sleep_seconds)
        
        except Exception as e:
            logger.error(f"Error en el ciclo principal: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run_strategy()
