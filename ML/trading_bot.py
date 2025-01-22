# ML/trading_bot.py

import os
import logging
from binance.client import Client
from binance.enums import *

# Configuración de logging
logger = logging.getLogger('trading_bot')
logger.setLevel(logging.INFO)

# Configuración de claves API
BINANCE_API_KEY = os.getenv('BINANCE_FUTURES_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_FUTURES_API_SECRET')

# Cliente de Binance Futures Testnet
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True)
client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'

def place_order(symbol, side, quantity, order_type=ORDER_TYPE_MARKET, leverage=2):
    """
    Colocar una orden en Binance Futures Testnet.

    Args:
        symbol (str): Símbolo del par, por ejemplo, 'BTCUSDT'.
        side (str): 'BUY' o 'SELL'.
        quantity (float): Cantidad a comprar o vender.
        order_type (str): Tipo de orden.
        leverage (int): Apalancamiento.
    """
    try:
        # Establecer apalancamiento
        client.futures_change_leverage(symbol=symbol, leverage=leverage)

        # Colocar orden
        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type=order_type,
            quantity=quantity
        )
        logger.info(f"Orden colocada: {order}")
    except Exception as e:
        logger.error(f"Error al colocar orden: {e}")

