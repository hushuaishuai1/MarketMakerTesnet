from tortoise import Tortoise, run_async
import os
from dotenv import load_dotenv
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de la base de datos
DATABASE_CONFIG = {
    'connections': {
        'default': f"postgres://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
                   f"{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/"
                   f"{os.getenv('POSTGRES_DB')}"
    },
    'apps': {
        'models': {
            'models': ['aerich.models', 'app.models'],  # Incluir 'aerich.models'
            'default_connection': 'default',
            'timezone': 'UTC',  # Asegurar que se maneja en UTC
        }
    }
}

async def init():
    """Inicializa la conexión a la base de datos y genera los esquemas."""
    await Tortoise.init(config=DATABASE_CONFIG)
    await Tortoise.generate_schemas()
    logger.info("Base de datos inicializada y esquemas generados.")

async def close():
    """Cierra las conexiones a la base de datos."""
    await Tortoise.close_connections()
    logger.info("Conexiones a la base de datos cerradas.")
