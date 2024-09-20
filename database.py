from tortoise import Tortoise

DATABASE_CONFIG = {
    "connections": {
        "default": "postgres://oscarsql:ioppoiopi0@localhost:5432/DbBinance",  # Conexión a PostgreSQL
    },
    "apps": {
        "models": {
            "models": ["app.models", "aerich.models"],  # Modelos de Aerich para migraciones
            "default_connection": "default",
        }
    }
}

async def init():
    """Inicializa la conexión a la base de datos y genera esquemas si es necesario"""
    await Tortoise.init(config=DATABASE_CONFIG)
    await Tortoise.generate_schemas()

async def close():
    """Cierra las conexiones a la base de datos"""
    await Tortoise.close_connections()
