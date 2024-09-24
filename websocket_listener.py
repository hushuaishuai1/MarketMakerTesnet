import asyncio
import json
import logging
import websockets
from app.models import HistoricalPrice
from database import init, close  # Tortoise setup
from settings import settings  # Settings con las credenciales de Binance
import pytz
from datetime import datetime
logging.basicConfig(level=logging.INFO)

async def listen_to_binance_websocket():
    logging.info("Conectando al WebSocket de Binance...")
    async with websockets.connect(f"wss://stream.binance.com:9443/ws/{settings.BINANCE_STREAMS}") as websocket:
        while True:
            try:
                data = await websocket.recv()
                message = json.loads(data)
                await handle_message(message)
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}")
                await asyncio.sleep(settings.BACKOFF_INTERVAL)  # Retroceso en caso de error

async def handle_message(message):
    # Extraer datos del mensaje
    symbol = message['s']
    price = float(message['p'])
    volume = float(message['q'])  # Agregar el volumen
    trade_id = message['T']  # ID del trade
    timestamp = message['E']

    # Convertir el timestamp a datetime (milisegundos)
    from datetime import datetime
    timestamp = datetime.fromtimestamp(timestamp / 1000, pytz.UTC)

    # Guardar en la base de datos usando Tortoise
    new_price = HistoricalPrice(symbol=symbol, price=price, timestamp=timestamp)
    await new_price.save()

    # Opcionalmente, guardar también volumen e ID del trade en otro modelo si es necesario
    logging.info(f"Guardado {symbol} - Precio: {price}, Volumen: {volume}, Trade ID: {trade_id}")

async def main():
    try:
        await init()  # Inicializar conexión con Tortoise
        await asyncio.gather(listen_to_binance_websocket())
    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")
    finally:
        await close()  # Cerrar conexión con Tortoise

if __name__ == "__main__":
    asyncio.run(main())
