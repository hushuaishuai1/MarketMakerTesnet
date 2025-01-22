import sys
import os
import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from tortoise.transactions import in_transaction

# Agregar directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import Trade  # Modelo Trade
from database import init, close  # Inicialización y cierre de DB

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_data(symbol: str):
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=2000"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Error al obtener datos: {response.status}")
                    return []
                data = await response.json()
                logger.info(f"Datos recibidos para {symbol}: {len(data)} trades")
                return data
        except aiohttp.ClientError as e:
            logger.error(f"Error del cliente: {e}")
            return []
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            return []

async def upsert_trade(symbol, price, volume, timestamp):
    """
    Inserta o actualiza un trade en la base de datos utilizando SQL manual.
    """
    query = """
    INSERT INTO trades (symbol, price, volume, timestamp)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (symbol, timestamp)
    DO UPDATE SET price = EXCLUDED.price, volume = EXCLUDED.volume;
    """
    try:
        async with in_transaction() as conn:
            await conn.execute_query(query, [symbol, price, volume, timestamp])
        logger.debug(f"Trade insertado o actualizado: {symbol}, {timestamp}")
    except Exception as e:
        logger.error(f"Error al insertar/actualizar trade: {e}")

async def insert_data():
    """
    Inserta los datos de trades en la base de datos.
    """
    try:
        await init()  # Inicializa la base de datos
        trades = await fetch_data('BTCUSDT', limit=5000)
             # Obtener datos de trades

        if not trades:
            logger.warning("No se obtuvieron datos para insertar.")
            return

        inserted_trades = 0  # Contador para trades insertados

        for trade in trades:
            # Verificar y obtener valores de trade
            symbol = trade.get('symbol', 'BTCUSDT')
            price = float(trade.get('price', 0.0))
            volume = float(trade.get('qty', 0.0))
            time = trade.get('time')

            if not time:
                logger.warning(f"Registro malformado: {trade}")
                continue  # Saltar registros malformados

            timestamp = datetime.utcfromtimestamp(int(time) / 1000.0).replace(tzinfo=timezone.utc)

            # Insertar o actualizar trade
            await upsert_trade(symbol, price, volume, timestamp)
            inserted_trades += 1

        logger.info(f"Se insertaron o actualizaron {inserted_trades} trades.")
    except Exception as e:
        logger.error(f"Error al insertar trades: {e}", exc_info=True)
    finally:
        await close()  # Cerrar conexión a la base de datos

if __name__ == "__main__":
    asyncio.run(insert_data())
