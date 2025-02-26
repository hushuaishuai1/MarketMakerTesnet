from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from tortoise.contrib.fastapi import register_tortoise
from tortoise.transactions import in_transaction
from ML.data_processing import get_historical_data, calculate_indicators

from app.models import Trade, HistoricalPrice
from dotenv import load_dotenv
import os
import uvicorn
import asyncio
import logging
import pandas as pd
from datetime import datetime

# Importamos las funciones de data_processing (si quieres usar su lógica de descarga y procesado)
# Si no quieres indicadores, simplemente aprovecha la parte de fetch_klines.


# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Configuración del logging para registrar eventos y errores
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicialización de la aplicación FastAPI
app = FastAPI(
    title="Binance-Postgres Service",
    description="Proyecto de Backend para conectar a Binance, descargar y almacenar datos en PostgreSQL."
)

# Configuración de la conexión a PostgreSQL utilizando Tortoise ORM
DATABASE_CONFIG = {
    'connections': {
        'default': 'postgres://{user}:{password}@{host}:{port}/{database}'.format(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            database=os.getenv("POSTGRES_DB")
        )
    },
    'apps': {
        'models': {
            'models': ['app.models'],  # Asegúrate de que el path sea correcto
            'default_connection': 'default',
        }
    }
}

# Registro de la configuración de Tortoise ORM con FastAPI
register_tortoise(
    app,
    config=DATABASE_CONFIG,
    generate_schemas=True,  # Genera las tablas automáticamente si no existen
    add_exception_handlers=True,
)

# --------------------------------------------------------------------------------
# Esquemas de Pydantic para validación (ejemplo con Trades e HistoricalPrice)
# --------------------------------------------------------------------------------
class TradeSchema(BaseModel):
    symbol: str
    price: float
    volume: float
    timestamp: str  # En formato ISO o "YYYY-MM-DD HH:MM:SS"

class HistoricalPriceSchema(BaseModel):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: str

# --------------------------------------------------------------------------------
# Función utilitaria para insertar si no existe
# --------------------------------------------------------------------------------
async def store_data_if_not_exists(model, data_dict):
    """
    Inserta un registro en la tabla indicada por 'model' únicamente si no existe 
    otro registro con el mismo (symbol, timestamp).
    """
    async with in_transaction():
        # Convertimos timestamp a datetime (por seguridad, usando pandas o datetime)
        timestamp = pd.to_datetime(data_dict['timestamp'])
        symbol = data_dict['symbol']

        # Verificar si ya existe
        existing_data = await model.filter(symbol=symbol, timestamp=timestamp).first()
        if existing_data:
            return False  # No se inserta si ya existe

        # Crear un nuevo registro
        await model.create(**data_dict)
        return True

# --------------------------------------------------------------------------------
# Endpoints para Trades
# --------------------------------------------------------------------------------
@app.post("/trades/", tags=["Trades"])
async def create_trade(trade: TradeSchema):
    """
    Crea un nuevo Trade en la base de datos.
    """
    try:
        data_dict = trade.dict()
        success = await store_data_if_not_exists(Trade, data_dict)
        if success:
            return {"status": "Trade created successfully"}
        else:
            return {"status": "Trade already exists"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating trade: {e}")

@app.get("/trades/", tags=["Trades"])
async def get_trades():
    """
    Retorna todos los Trades almacenados.
    """
    trades = await Trade.all().order_by("-timestamp")
    return trades

# --------------------------------------------------------------------------------
# Endpoints para Historical Prices
# --------------------------------------------------------------------------------
@app.post("/historical_prices/", tags=["HistoricalPrices"])
async def create_historical_price(price: HistoricalPriceSchema):
    """
    Crea un nuevo registro de precio histórico.
    """
    try:
        data_dict = price.dict()
        success = await store_data_if_not_exists(HistoricalPrice, data_dict)
        if success:
            return {"status": "Historical price created successfully"}
        else:
            return {"status": "Historical price already exists"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating historical price: {e}")

@app.get("/historical_prices/", tags=["HistoricalPrices"])
async def list_historical_prices():
    """
    Retorna los precios históricos almacenados (paginado básico si deseas).
    """
    prices = await HistoricalPrice.all().order_by("-timestamp")
    return prices

# --------------------------------------------------------------------------------
# Endpoint para obtener (descargar) datos desde Binance y guardarlos en DB
# --------------------------------------------------------------------------------
@app.get("/fetch_and_store_data/{symbol}", tags=["Binance"])
async def fetch_and_store_data(
    symbol: str,
    interval: str = Query("15m", description="Intervalo de velas, e.g. '1m', '15m', '1h'"),
    start_date: str = Query("2020-01-01", description="Fecha de inicio en formato YYYY-MM-DD"),
    end_date: str = Query("2025-01-01", description="Fecha de fin en formato YYYY-MM-DD"),
    max_candles: int = Query(1000, description="Máximo de velas a descargar")
):
    """
    Descarga datos históricos de Binance usando la lógica de `data_processing.py`,
    calcula indicadores (opcional) y guarda cada vela en la tabla HistoricalPrice.

    - `symbol`: Ejemplo 'BTCUSDT'
    - `interval`: Ejemplo '15m'
    - `start_date`: Fecha de inicio, formato YYYY-MM-DD
    - `end_date`: Fecha de fin, formato YYYY-MM-DD
    - `max_candles`: Límite de velas a descargar
    """
    try:
        # 1) Descarga los datos sin procesar (open, high, low, close, volume, etc.)
        df_raw = await get_historical_data(symbol, interval, start_date, end_date, max_candles)
        
        if df_raw.empty:
            return {"message": f"No se obtuvieron datos para {symbol} con ese rango."}
        
        # 2) Si quieres procesar con indicadores, descomenta esta línea:
        df_processed = calculate_indicators(df_raw)
        # Si NO quieres calcular indicadores, y solo guardar OHLCV, usa df_raw:
        # df_processed = df_raw
        
        # Ajustar nombres de columnas al modelo HistoricalPrice (o personalizar)
        # Observa que en `data_processing`, la columna de tiempo es "open_time".
        # Vamos a renombrarla a "timestamp" y asegurarnos de que existan: symbol, open, high, low, close, volume.
        df_processed = (
            calculate_indicators(df_raw)
            .copy()
            .rename(columns={"open_time": "timestamp"})
        )
        df_processed["symbol"] = symbol

        # 3) Guardar en la base de datos
        # Recorremos cada fila y llamamos a store_data_if_not_exists
        inserted_count = 0
        for _, row in df_processed.iterrows():
            data_dict = {
                "symbol": row["symbol"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "timestamp": str(row["timestamp"])  # Convertir a str para store_data_if_not_exists
            }
            success = await store_data_if_not_exists(HistoricalPrice, data_dict)
            if success:
                inserted_count += 1
        
        return {
            "message": "Datos descargados y almacenados con éxito.",
            "total_fetched": len(df_processed),
            "inserted_new": inserted_count
        }
    except Exception as e:
        logging.error(f"Error al descargar/almacenar datos para {symbol}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------------------------------------------------------------
# Punto de entrada para desarrollo local
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
