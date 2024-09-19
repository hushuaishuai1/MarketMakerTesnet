import asyncio
from tortoise import Tortoise

DATABASE_CONFIG = {
    'connections': {
        "default": "postgres://oscarsql :ioppoiopi0@localhost:5432/DbBinance",  # Actualiza con tus valores
    },
    'apps': {
        'models': {
            'models': ['app.models', 'aerich.models'],
            'default_connection': 'default',
        }
    }
}

async def init_db():
    print("Initializing database...")
    try:
        await Tortoise.init(config=DATABASE_CONFIG)
        print("Database initialized.")
        await Tortoise.generate_schemas()
        print("Schemas created.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connections...")
        await Tortoise.close_connections()
        print("Connections closed.")

if __name__ == "__main__":
    asyncio.run(init_db())
