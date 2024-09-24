from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from tortoise.contrib.fastapi import register_tortoise
from tortoise.transactions import in_transaction
from app.models import Trade, HistoricalPrice, Order, CurrencyPair, StrategyResult, Signal
from dotenv import load_dotenv
import os
import uvicorn
import asyncio
from api.momentum_strategy import momentum_strategy
from api.backtesting import run_backtesting
from api.binance_connector import get_historical_data
from api.grid_search_strategy import run_grid_search
from app.models import BestParams
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from api.backtesting import run_backtesting
from api.grid_search_strategy import run_grid_search
import requests


# Cargar variables de entorno
load_dotenv()

# Configuración de la app de FastAPI
app = FastAPI()

# Configuración de PostgreSQL en Tortoise ORM
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

# Registro de la configuración en FastAPI
register_tortoise(
    app,
    config=DATABASE_CONFIG,
    generate_schemas=True,
    add_exception_handlers=True,
)

# Función centralizada para evitar sobrescritura de cualquier tipo de dato
async def store_data_if_not_exists(model, data_dict):
    async with in_transaction():
        timestamp = data_dict['timestamp'].to_pydatetime()
        symbol = data_dict['symbol']

        existing_data = await model.filter(symbol=symbol, timestamp=timestamp).first()
        if existing_data:
            return False  # No se inserta si ya existe

        await model.create(**data_dict)
        return True

# Pydantic Schemas para validar los datos que recibes
class TradeSchema(BaseModel):
    symbol: str
    price: float
    volume: float
    timestamp: str

class HistoricalPriceSchema(BaseModel):
    symbol: str
    price: float
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
    return_on_investment: float
    success_rate: float
    timestamp: str

class CurrencyPairSchema(BaseModel):
    base_currency: str
    quote_currency: str

# Endpoints para Trades
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

# Endpoints para HistoricalPrice
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

@app.get("/historical_prices/{symbol}")
async def get_historical(symbol: str, interval: str = '1h', limit: int = 1000):
    try:
        df = get_historical_data(symbol, interval, limit)
        df['timestamp'] = df['timestamp'].astype(str)
        result_json = df.to_dict(orient='records')
        return result_json
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al obtener datos históricos: {e}")

# Endpoint para ejecutar la estrategia de momentum
@app.get("/momentum/{symbol}")
async def execute_momentum(symbol: str, interval: str = '1h', limit: int = 1000):
    try:
        result = await momentum_strategy(symbol, interval=interval, limit=limit)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error ejecutando la estrategia: {e}")

# Endpoint para ejecutar el backtesting
@app.get("/backtesting/{symbol}")
async def backtesting(symbol: str, interval: str = Query('1h')):
    try:
        # Llamar a la función run_backtesting pasando symbol e interval
        result = run_backtesting(symbol=symbol, interval=interval)
        return result
    except Exception as e:
        return {"detail": f"Error ejecutando el backtesting: {e}"}

@app.get("/run-grid-search/{symbol}")
async def run_search(symbol: str, interval: str = '1h', limit: int = 1000):
    result = await run_grid_search(symbol, interval, limit)
    return result

# Otros endpoints para obtener datos y actualizar los registros
@app.get("/trades/")
async def get_trades():
    trades = await Trade.all()
    return trades

@app.get("/signals/{symbol}")
async def get_signals(symbol: str, interval: str = '1h'):
    signals = await Signal.filter(symbol=symbol, interval=interval).all()
    return signals

# Automatización de trading cada 5 minutos
scheduler = AsyncIOScheduler()

async def execute_trading_tasks():
    pairs = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT']
    for pair in pairs:
        await momentum_strategy(pair, '1h', 1000)

scheduler.add_job(execute_trading_tasks, 'interval', minutes=5)

@app.on_event("startup")
async def start_scheduler():
    scheduler.start()

@app.on_event("shutdown")
async def shutdown_scheduler():
    scheduler.shutdown()

# Inicialización de la base de datos y ejecución del servidor FastAPI
if __name__ == "__main__":
    asyncio.run(init_db())
    uvicorn.run(app, host="127.0.0.1", port=8000)
