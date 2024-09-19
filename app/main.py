from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tortoise.contrib.fastapi import register_tortoise
from tortoise.transactions import in_transaction
from app.models import Trade, HistoricalPrice, Order, CurrencyPair, StrategyResult
from dotenv import load_dotenv
import os
import uvicorn
import asyncio
from api.momentum_strategy import momentum_strategy
from api.risk_manager import calculate_atr, set_stop_loss
from api.trade_executor import execute_trade
from api.binance_connector import client  # Conexión a Binance desde binance_connector
from app.models import Signal
from backtesting.backtesting import run_backtesting
from api.binance_connector import client, get_historical_data 
import traceback


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

# Funciones auxiliares
async def init_db():
    await Tortoise.init(config=DATABASE_CONFIG)
    await Tortoise.generate_schemas()

# Endpoints para Trades
@app.post("/trades/")
async def create_trade(trade: TradeSchema):
    try:
        async with in_transaction():
            new_trade = await Trade.create(
                symbol=trade.symbol,
                price=trade.price,
                volume=trade.volume,
                timestamp=trade.timestamp
            )
        return {"status": "Trade created successfully", "trade_id": new_trade.id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating trade: {e}")

@app.get("/trades/")
async def get_trades():
    trades = await Trade.all()
    return trades

@app.get("/trades/{trade_id}")
async def get_trade(trade_id: int):
    try:
        trade = await Trade.get(id=trade_id)
        return trade
    except Trade.DoesNotExist:
        raise HTTPException(status_code=404, detail="Trade not found")

@app.put("/trades/{trade_id}")
async def update_trade(trade_id: int, trade: TradeSchema):
    try:
        existing_trade = await Trade.get(id=trade_id)
        existing_trade.symbol = trade.symbol
        existing_trade.price = trade.price
        existing_trade.volume = trade.volume
        existing_trade.timestamp = trade.timestamp
        await existing_trade.save()
        return {"status": "Trade updated successfully"}
    except Trade.DoesNotExist:
        raise HTTPException(status_code=404, detail="Trade not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error updating trade: {e}")

@app.delete("/trades/{trade_id}")
async def delete_trade(trade_id: int):
    try:
        trade = await Trade.get(id=trade_id)
        await trade.delete()
        return {"status": "Trade deleted successfully"}
    except Trade.DoesNotExist:
        raise HTTPException(status_code=404, detail="Trade not found")

# Endpoints para HistoricalPrice
@app.post("/historical_prices/")
async def create_historical_price(price: HistoricalPriceSchema):
    try:
        new_price = await HistoricalPrice.create(
            symbol=price.symbol,
            price=price.price,
            timestamp=price.timestamp
        )
        return {"status": "Historical price created successfully", "price_id": new_price.id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating historical price: {e}")

@app.get("/historical_prices/")
async def get_historical_prices():
    prices = await HistoricalPrice.all()
    return prices

@app.get("/historical_prices/{symbol}")
async def get_historical(symbol: str, interval: str = '1h', limit: int = 1000):
    """
    Endpoint para obtener datos históricos de precios.
    """
    try:
        df = get_historical_data(symbol, interval, limit)
        df['timestamp'] = df['timestamp'].astype(str)  # Convertir timestamp a string para JSON
        result_json = df.to_dict(orient='records')
        return result_json
    except Exception as e:
        print(f"Error al obtener datos históricos: {e}")
        raise HTTPException(status_code=400, detail=f"Error al obtener datos históricos: {e}")
@app.put("/historical_prices/{price_id}")
async def update_historical_price(price_id: int, price: HistoricalPriceSchema):
    try:
        existing_price = await HistoricalPrice.get(id=price_id)
        existing_price.symbol = price.symbol
        existing_price.price = price.price
        existing_price.timestamp = price.timestamp
        await existing_price.save()
        return {"status": "Historical price updated successfully"}
    except HistoricalPrice.DoesNotExist:
        raise HTTPException(status_code=404, detail="Historical price not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error updating historical price: {e}")

@app.delete("/historical_prices/{price_id}")
async def delete_historical_price(price_id: int):
    try:
        price = await HistoricalPrice.get(id=price_id)
        await price.delete()
        return {"status": "Historical price deleted successfully"}
    except HistoricalPrice.DoesNotExist:
        raise HTTPException(status_code=404, detail="Historical price not found")






#  Endpoint para obtener todas las señales almacenadas
 
@app.get("/signals/")
async def get_signals(limit: int = 200):
    """
    Endpoint para obtener todas las señales almacenadas.
    
    Parameters:
    - limit: Límite de señales a obtener (por defecto 100)
    
    Returns:
    - Lista de señales en formato JSON
    """
    try:
        # Obtener las señales con un límite
        signals = await Signal.all().limit(limit)  # Limitar el número de señales obtenidas
        return signals  # Retornar las señales en formato JSON

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error obteniendo señales: {e}")
# Inicialización de la base de datos y ejecución del servidor FastAPI
# Endpoint para ejecutar el backtesting
# Endpoint para ejecutar el backtesting
# Endpoint para ejecutar la estrategia de momentum
@app.get("/momentum/{symbol}")
async def execute_momentum(symbol: str, interval: str = '1h', limit: int = 100):
    try:
        result = await momentum_strategy(symbol, interval=interval, limit=limit)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error ejecutando la estrategia: {e}")


# Endpoint para ejecutar el backtesting
@app.get("/backtesting/{symbol}")
async def backtesting_endpoint(symbol: str, interval: str = '1h'):
    try:
        result = await run_backtesting(symbol, interval=interval)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    asyncio.run(init_db())
    uvicorn.run(app, host="127.0.0.1", port=8000)


@app.get("/backtesting/{symbol}")
async def backtesting_endpoint(symbol: str, interval: str = '1h'):
    try:
        result = await run_backtesting(symbol, interval=interval)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    asyncio.run(init_db())
    uvicorn.run(app, host="127.0.0.1", port=8000)
