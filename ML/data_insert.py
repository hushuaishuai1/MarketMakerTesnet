import aiohttp
import asyncio
import logging
from tortoise import Tortoise
from app.models import HistoricalPrice  # Asegúrate de que esta ruta sea correcta

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de la base de datos
DATABASE_CONFIG = {
    "connections": {
        "default": "postgres://oscarsql:ioppoiopi0@localhost:5432/DbBinance",  # Ajusta la URI si es necesario
    },
    "apps": {
        "models": {
            "models": ["app.models", "aerich.models"],  # Ajusta según tu estructura de directorios
            "default_connection": "default",
        }
    }
}

async def init():
    """Inicializa la conexión a la base de datos."""
    await Tortoise.init(config=DATABASE_CONFIG)
    await Tortoise.generate_schemas()

async def close():
    """Cierra las conexiones a la base de datos."""
    await Tortoise.close_connections()

async def fetch_data(symbol):
    """Obtiene datos históricos de la API."""
    url = f"http://localhost:8000/historical_prices/{symbol}?interval=5m&limit=9000"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            logging.info(f"Datos recuperados para {symbol}: {data}")  # Agrega esta línea
            return data

async def insert_data(symbol):
    """Inserta los datos históricos en la base de datos."""
    await init()  # Inicializa la conexión

    data = await fetch_data(symbol)  # Obtiene los datos

    if data:
        for entry in data:
            # Verifica que todos los campos requeridos estén presentes
            if all(key in entry for key in ['symbol', 'open', 'high', 'low', 'close', 'volume', 'timestamp']):
                await HistoricalPrice.create(**entry)  # Inserta cada registro en la base de datos
            else:
                logging.warning(f"Registro incompleto, no se insertó: {entry}")
        logging.info(f"Datos insertados exitosamente para {symbol}.")
    else:
        logging.warning(f"No se encontraron datos para {symbol}.")

    await close()  # Cierra la conexión

if __name__ == "__main__":
    try:
        symbol = "BTCUSDT"  # Cambia esto si deseas otro símbolo
        asyncio.run(insert_data(symbol))  # Ejecuta la inserción de datos
    except Exception as e:
        logging.error(f"Error al insertar datos: {e}")  # Manejo de errores
