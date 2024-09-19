import asyncio
from tortoise import Tortoise
from database import DATABASE_CONFIG

async def init_db():
    await Tortoise.init(config=DATABASE_CONFIG)
    await Tortoise.generate_schemas()

async def close_db():
    await Tortoise.close_connections()

async def main():
    await init_db()
    print("Database initialized and schemas generated")
    await close_db()

if __name__ == "__main__":
    asyncio.run(main())
