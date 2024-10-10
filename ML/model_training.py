import pandas as pd
import asyncio
from tortoise import Tortoise
from app.models import HistoricalPrice
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de la base de datos
DATABASE_CONFIG = {
    "connections": {
        "default": "postgres://oscarsql:ioppoiopi0@localhost:5432/DbBinance",
    },
    "apps": {
        "models": {
            "models": ["app.models", "aerich.models"],
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

async def load_data():
    """Carga los datos históricos desde la base de datos."""
    await init()
    prices = await HistoricalPrice.all().values()  # Obtiene todos los registros en formato de diccionario
    await close()
    return pd.DataFrame(prices)  # Convierte a DataFrame

if __name__ == "__main__":
    try:
        df = asyncio.run(load_data())
        logging.info("Datos cargados exitosamente:")
        print(df)  # Muestra el DataFrame
    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
