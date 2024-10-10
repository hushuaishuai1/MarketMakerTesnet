from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from tortoise.contrib.fastapi import register_tortoise
from tortoise.transactions import in_transaction
from app.models import Trade, HistoricalPrice, Order, CurrencyPair, StrategyResult, Signal
from dotenv import load_dotenv
import os
import uvicorn
import asyncio
from api.momentum_strategy import run_backtesting
from api.binance_connector import get_historical_data
from api.grid_search_strategy import run_grid_search
from app.models import BestParams
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor
import logging
import pandas as pd  # Importación necesaria para manejar pandas
from typing import Dict, Any
from datetime import datetime  # Agregar datetime para timestamps

# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Configuración del logging para registrar eventos y errores
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicialización de la aplicación FastAPI
app = FastAPI()

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
    generate_schemas=True,  # Genera las tablas automáticamente
    add_exception_handlers=True,
)

# Función para almacenar datos en la base de datos si no existen
async def store_data_if_not_exists(model, data_dict):
    async with in_transaction():
        # Convertir el timestamp de string a datetime
        timestamp = pd.to_datetime(data_dict['timestamp'])
        symbol = data_dict['symbol']

        # Verificar si ya existe el registro en la base de datos
        existing_data = await model.filter(symbol=symbol, timestamp=timestamp).first()
        if existing_data:
            return False  # No se inserta si ya existe

        # Crear un nuevo registro en la base de datos
        await model.create(**data_dict)
        return True

# Definición de los esquemas de Pydantic para validar los datos entrantes
class TradeSchema(BaseModel):
    symbol: str
    price: float
    volume: float
    timestamp: str

class HistoricalPriceSchema(BaseModel):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: str

class OrderSchema(BaseModel):
    symbol: str
    type: str
    price: float
    volume: float
    status: str
    timestamp: str

class StrategyResultSchema(BaseModel):
    strategy_name: str
    final_value: float
    pnl: float
    sharpe_ratio: float
    timestamp: str

class CurrencyPairSchema(BaseModel):
    base_currency: str
    quote_currency: str

# Inicialización del ThreadPoolExecutor para manejar tareas síncronas en segundo plano
executor = ThreadPoolExecutor()

# Función asíncrona para ejecutar el backtesting en el ThreadPoolExecutor
async def run_backtest_async(symbol, interval, limit, window_size=10):
    loop = asyncio.get_event_loop()
    try:
        # Ejecutar la función de backtesting en un hilo separado
        metrics = await loop.run_in_executor(executor, run_backtesting, symbol, interval, limit, window_size)
        return metrics
    except Exception as e:
        logging.error(f"Error ejecutando run_backtesting: {e}", exc_info=True)
        raise e

# Función asíncrona para guardar las métricas del backtesting en la base de datos
async def save_metrics_to_db(metrics: Dict[str, Any]):
    # Asegúrate de que las métricas tengan valores válidos
    metrics['return_on_investment'] = metrics.get('return_on_investment', 0.0)
    metrics['success_rate'] = metrics.get('success_rate', 0.0)
    metrics['final_value'] = metrics.get('final_value', 1000.0)  # Valor predeterminado
    metrics['pnl'] = metrics.get('pnl', 0.0)
    metrics['sharpe_ratio'] = metrics.get('sharpe_ratio', 0.0)
    
    try:
        await StrategyResult.create(
            strategy_name='MomentumStrategy',
            return_on_investment=metrics['return_on_investment'],
            success_rate=metrics['success_rate'],
            final_value=metrics['final_value'],
            pnl=metrics['pnl'],
            sharpe_ratio=metrics['sharpe_ratio'],
            timestamp=datetime.now()  # Usar datetime para el timestamp
        )
    except Exception as e:
        logging.error(f"Error al guardar las métricas en la base de datos: {e}")


# Endpoint para crear un nuevo trade
@app.post("/trades/")
async def create_trade(trade: TradeSchema):
    try:
        data_dict = trade.dict()
        success = await store_data_if_not_exists(Trade, data_dict)
        if success:
            return {"status": "Trade created successfully"}
        else:
            return {"status": "Trade already exists"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating trade: {e}")

# Endpoint para crear un nuevo precio histórico
@app.post("/historical_prices/")
async def create_historical_price(price: HistoricalPriceSchema):
    try:
        data_dict = price.dict()
        success = await store_data_if_not_exists(HistoricalPrice, data_dict)
        if success:
            return {"status": "Historical price created successfully"}
        else:
            return {"status": "Historical price already exists"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating historical price: {e}")

# Endpoint para obtener precios históricos de un símbolo específico
@app.get("/historical_prices/{symbol}")
async def get_historical(symbol: str, interval: str = '1h', limit: int = 1000):
    try:
        df = get_historical_data(symbol, interval, limit)
        if df.empty:
            raise HTTPException(status_code=404, detail="No historical data found for the given symbol.")
        
        # Resetear el índice para que 'timestamp' sea una columna
        df.reset_index(inplace=True)
        
        # Convertir el DataFrame a JSON
        result_json = df.to_dict(orient='records')
        return result_json
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al obtener datos históricos: {e}")

# Endpoint para ejecutar la estrategia de momentum
@app.get("/momentum/{symbol}")
async def run_momentum(symbol: str, interval: str = '5m', limit: int = 8000, window_size: int = 10):
    try:
        # Ejecutar el backtesting de manera asíncrona
        result = await run_backtest_async(symbol, interval, limit, window_size)
        
        # Crear un esquema para las métricas obtenidas
        metrics_schema = StrategyResultSchema(
            strategy_name="MomentumStrategy",
            final_value=result.get("final_value", 0.0),
            pnl=result.get("pnl", 0.0),
            sharpe_ratio=result.get("sharpe_ratio", 0.0),
            timestamp=datetime.now().isoformat()  # Usar datetime para timestamp
        )
        
        # Guardar las métricas en la base de datos
        await save_metrics_to_db(metrics_schema.dict())
        
        return {"result": "Estrategia ejecutada con éxito", "details": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ejecutando la estrategia: {str(e)}")

# Endpoint para ejecutar el backtesting
@app.get("/backtesting/{symbol}")
async def backtesting(symbol: str, interval: str = Query('5m'), limit: int = 8000, window_size: int = 10):
    try:
        # Ejecutar el backtesting de manera asíncrona
        result = await run_backtest_async(symbol, interval, limit, window_size)
        return result
    except Exception as e:
        return {"detail": f"Error ejecutando el backtesting: {e}"}

# Endpoint para ejecutar una búsqueda en cuadrícula (grid search)
@app.get("/run-grid-search/{symbol}")
async def run_search(symbol: str, interval: str = '1h', limit: int = 1000):
    try:
        result = await run_grid_search(symbol, interval, limit)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ejecutando grid search: {e}")

# Otros endpoints para obtener trades y señales
@app.get("/trades/")
async def get_trades():
    trades = await Trade.all()
    return trades

@app.get("/signals/{symbol}")
async def get_signals(symbol: str, interval: str = '1h'):
    signals = await Signal.filter(symbol=symbol, interval=interval).all()
    return signals

# Inicialización de la base de datos y ejecución del servidor FastAPI
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
