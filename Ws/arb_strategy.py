# arb_strategy_testnet.py

import logging
import os
import math
import time
import random
from dotenv import load_dotenv
from binance import Client, ThreadedWebsocketManager
from binance.enums import *
from binance.helpers import round_step_size
from threading import Thread
from queue import Queue

# Cargar variables de entorno desde el archivo .env
load_dotenv()
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')

# Verificar que las claves de API están configuradas
if not api_key or not api_secret:
    raise ValueError("Las claves de API de Testnet no están configuradas en el archivo .env")

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Establecer en DEBUG para obtener información detallada

# Formato de los mensajes
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler para la consola (DEBUG y superiores)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Handler para el archivo (DEBUG y superiores)
try:
    file_handler = logging.FileHandler('arb_bot_testnet.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
except Exception as e:
    logging.error(f"No se pudo configurar el archivo de log: {e}")

# Diccionario global para almacenar los precios
prices = {}
order_books = {}

# Variables para seguimiento de capital y operaciones
initial_balance = 200.0  # Capital inicial en USDT
current_balance = initial_balance
total_trades = 0
successful_trades = 0

# Parámetros de la estrategia
starting_amount = 15.0  # Cantidad inicial en USDT para cada operación
fee_rate = 0.00075  # Comisión del 0.075% (ajusta según tu nivel de usuario)
min_profit = 0.04  # Umbral mínimo de ganancia en USDT (ajusta según tu preferencia)
min_required_amount = 10.0  # Monto mínimo requerido para operar

# Priorizar monedas (asegúrate de que están disponibles en la Testnet)
prioritized_currencies = ['BTC', 'ETH', 'BNB', 'LTC', 'TRX', 'XRP', 'BUSD', 'USDT']

# Crear cola con tamaño máximo
queue_maxsize = 10000  # Tamaño máximo de la cola
ws_queue = Queue(maxsize=queue_maxsize)

def main():
    global client, symbols

    try:
        # Crear instancia del cliente sincrónico para la Testnet
        client = Client(api_key, api_secret, testnet=True)
        logging.info("Conectado a Binance Testnet")

        # Obtener información del exchange para manejar restricciones
        exchange_info = client.get_exchange_info()
        symbol_info = {s['symbol']: s for s in exchange_info['symbols']}

        # Filtrar pares disponibles y activos
        symbols = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']

        # Filtrar los símbolos a los priorizados disponibles
        symbols = [s for s in symbols if is_prioritized_pair(s)]

        logging.debug(f"Símbolos disponibles para operar: {symbols}")

        # Iniciar el websocket en un hilo separado
        ws_thread = Thread(target=start_socket)
        ws_thread.daemon = True
        ws_thread.start()
        logging.info("Hilo del WebSocket iniciado")

        # Iniciar el hilo de procesamiento de mensajes
        processing_thread = Thread(target=process_messages, args=(symbol_info,))
        processing_thread.daemon = True
        processing_thread.start()
        logging.info("Hilo de procesamiento de mensajes iniciado")

        # Mantener el hilo principal activo
        while True:
            time.sleep(1)

    except Exception as e:
        logging.error(f"Error al iniciar el cliente de Binance: {e}")
    finally:
        # Cerrar el cliente
        client.close_connection()
        logging.info("Conexión con Binance cerrada")

def start_socket():
    while True:
        try:
            # Crear el WebSocket Manager para la Testnet
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=True)
            twm.start()
            logging.info("WebSocket Manager iniciado")

            # Suscribirse a los streams de los pares priorizados
            streams = [symbol.lower() + '@bookTicker' for symbol in symbols]
            logging.info(f"Suscribiéndose a {len(streams)} streams")
            for stream in streams:
                symbol = stream.split('@')[0]
                twm.start_symbol_book_ticker_socket(callback=handle_socket_message, symbol=symbol)
                logging.debug(f"Stream iniciado para el símbolo: {symbol}")

            # Mantener el websocket activo
            twm.join()
        except Exception as e:
            logging.error(f"Error en el WebSocket: {e}. Reconectando en 5 segundos...")
            time.sleep(5)

def handle_socket_message(msg):
    try:
        ws_queue.put(msg, timeout=1)
        logging.debug(f"Mensaje recibido y agregado a la cola: {msg}")
    except Queue.Full:
        logging.error("La cola de mensajes está llena. Mensaje descartado.")

def is_prioritized_pair(symbol):
    base, quote = parse_symbol(symbol)
    return base in prioritized_currencies and quote in prioritized_currencies

def process_messages(symbol_info):
    last_processed_time = 0
    processing_interval = 1  # Procesar cada 1 segundo
    while True:
        try:
            res = ws_queue.get(timeout=60)
            current_time = time.time()
            process_data(res, symbol_info)
            if current_time - last_processed_time >= processing_interval:
                last_processed_time = current_time
                if len(prices) >= len(prioritized_currencies) * (len(prioritized_currencies) - 1):
                    logging.info("Buscando oportunidades de arbitraje...")
                    find_arbitrage_opportunities(symbol_info)
        except Exception as e:
            logging.error(f"Error al procesar datos: {e}")

def process_data(data, symbol_info):
    if 'e' in data and data['e'] == 'error':
        logging.error(f"Error del WebSocket: {data['m']}")
        return
    if 's' in data and 'b' in data and 'a' in data:
        symbol = data['s']
        bid_price = float(data['b'])
        ask_price = float(data['a'])
        prices[symbol] = {'bid': bid_price, 'ask': ask_price}
        logging.debug(f"Actualización de precios para {symbol}: bid={bid_price}, ask={ask_price}")
    else:
        logging.warning(f"Mensaje inesperado recibido: {data}")

def build_graph():
    graph = {}
    for pair in prices:
        base, quote = parse_symbol(pair)
        if base in prioritized_currencies and quote in prioritized_currencies:
            if 'bid' in prices[pair] and 'ask' in prices[pair]:
                bid_price = prices[pair]['bid']
                ask_price = prices[pair]['ask']

                # Añadir arista para orden de compra
                if base not in graph:
                    graph[base] = {}
                graph[base][quote] = -math.log(bid_price * (1 - fee_rate))

                # Añadir arista para orden de venta
                if quote not in graph:
                    graph[quote] = {}
                graph[quote][base] = -math.log(1 / ask_price * (1 - fee_rate))
            else:
                logging.warning(f"Precios no disponibles para el par {pair}")
    return graph

def parse_symbol(symbol):
    # Asumiendo que los símbolos tienen 3 o 4 caracteres de base y quote
    if symbol.endswith('USDT'):
        return symbol[:-4], 'USDT'
    elif symbol.endswith('BUSD'):
        return symbol[:-4], 'BUSD'
    elif symbol.endswith('BTC'):
        return symbol[:-3], 'BTC'
    elif symbol.endswith('ETH'):
        return symbol[:-3], 'ETH'
    elif symbol.endswith('BNB'):
        return symbol[:-3], 'BNB'
    else:
        # Asumir que la quote es de 3 caracteres
        return symbol[:-3], symbol[-3:]

def find_arbitrage_opportunities(symbol_info):
    logging.debug("Construyendo el grafo para buscar ciclos de arbitraje")
    graph = build_graph()
    currencies = list(graph.keys())
    for source in currencies:
        cycle = bellman_ford(graph, source)
        if cycle:
            logging.info(f"Oportunidad de arbitraje detectada: {cycle}")
            execute_arbitrage(cycle, symbol_info)
            break  # Ejecutar una oportunidad a la vez
    logging.debug("No se encontraron oportunidades de arbitraje en esta iteración")

def bellman_ford(graph, source):
    distance = {vertex: float('inf') for vertex in graph}
    predecessor = {vertex: None for vertex in graph}
    distance[source] = 0

    for _ in range(len(graph) - 1):
        updated = False
        for u in graph:
            for v in graph[u]:
                weight = graph[u][v]
                if distance[u] + weight < distance[v]:
                    distance[v] = distance[u] + weight
                    predecessor[v] = u
                    updated = True
        if not updated:
            break

    # Detección de ciclos negativos
    for u in graph:
        for v in graph[u]:
            weight = graph[u][v]
            if distance[u] + weight < distance[v]:
                # Se encontró un ciclo negativo
                cycle = [v]
                next_vertex = u
                while next_vertex not in cycle:
                    cycle.append(next_vertex)
                    next_vertex = predecessor[next_vertex]
                cycle.append(next_vertex)
                cycle.reverse()
                return cycle
    return None

def execute_arbitrage(cycle, symbol_info):
    global current_balance, total_trades, successful_trades
    try:
        # Cálculo del tamaño de la posición
        max_risk_per_trade = 0.02  # 2% del capital actual
        amount_to_invest = current_balance * max_risk_per_trade
        if amount_to_invest < min_required_amount:
            logging.warning("Cantidad a invertir menor que el mínimo requerido.")
            return

        route = []
        for i in range(len(cycle) - 1):
            base = cycle[i]
            quote = cycle[i + 1]
            pair = get_symbol(base, quote)
            if pair not in prices:
                logging.error(f"Pares necesarios no disponibles: {pair}")
                return
            route.append(pair)

        logging.info(f"Ruta de arbitraje encontrada: {route}")

        # Calcular ganancia potencial con slippage
        profit = calculate_potential_profit_with_slippage(route, amount_to_invest)
        if profit > min_profit:
            logging.info(f"Oportunidad de arbitraje rentable en la ruta {route}. Ganancia potencial: {profit:.2f} USDT")
            total_trades += 1
            order_type = decide_order_type(route[0])  # Decidir el tipo de orden
            # Ejecutar operaciones en la Testnet
            success = execute_trades(symbol_info, amount_to_invest, route, order_type=order_type)
            if success:
                successful_trades += 1
                current_balance += profit
                success_rate = (successful_trades / total_trades) * 100
                logging.info(f"Operación exitosa en la ruta {route}. Tasa de éxito: {success_rate:.2f}% - Balance actual: {current_balance:.2f} USDT")
            else:
                logging.info("Operación fallida en la Testnet.")
        else:
            logging.info(f"No se detectó una oportunidad rentable en la ruta {route}. Ganancia potencial: {profit:.2f} USDT")
    except Exception as e:
        logging.error(f"Error al ejecutar arbitraje: {e}")

def get_symbol(base, quote):
    if base + quote in symbols:
        return base + quote
    elif quote + base in symbols:
        return quote + base
    else:
        return None

def calculate_potential_profit_with_slippage(route, amount_to_invest):
    amount = amount_to_invest
    for i in range(len(route)):
        pair = route[i]
        base, quote = parse_symbol(pair)
        side = ''
        if base + quote == pair:
            # Comprar base usando quote
            side = 'buy'
            qty = amount / prices[pair]['ask']
            order_book = get_order_book(pair)
            vwap_price = calculate_vwap(order_book, qty, side='buy')
            if vwap_price is None:
                return 0  # No hay suficiente liquidez
            amount = amount / vwap_price * (1 - fee_rate)
        else:
            # Vender base para obtener quote
            side = 'sell'
            qty = amount  # Vendemos la cantidad actual
            order_book = get_order_book(pair)
            vwap_price = calculate_vwap(order_book, qty, side='sell')
            if vwap_price is None:
                return 0  # No hay suficiente liquidez
            amount = amount * vwap_price * (1 - fee_rate)
        time.sleep(0.05)  # Pequeña pausa para evitar limitaciones
    final_amount = amount
    profit = final_amount - amount_to_invest
    return profit

# Función para obtener la profundidad del mercado
def get_order_book(symbol, limit=100):
    try:
        order_book = client.get_order_book(symbol=symbol, limit=limit)
        return order_book
    except BinanceAPIException as e:
        logging.error(f"Error al obtener el libro de órdenes para {symbol}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado al obtener el libro de órdenes para {symbol}: {e}")
        return None

def calculate_vwap(order_book, amount, side='buy'):
    """
    Calcula el VWAP para una cantidad determinada.

    :param order_book: Datos del libro de órdenes.
    :param amount: Cantidad que deseas comprar o vender.
    :param side: 'buy' o 'sell'.
    :return: Precio VWAP.
    """
    if order_book is None:
        return None
    orders = order_book['asks'] if side == 'buy' else order_book['bids']
    total_quantity = 0
    total_cost = 0
    for price_str, qty_str in orders:
        price = float(price_str)
        qty = float(qty_str)
        if total_quantity + qty >= amount:
            qty_needed = amount - total_quantity
            total_cost += price * qty_needed
            total_quantity += qty_needed
            break
        else:
            total_cost += price * qty
            total_quantity += qty
    if total_quantity < amount:
        # No hay suficiente liquidez en el libro de órdenes
        return None
    vwap_price = total_cost / amount
    return vwap_price

def adjust_quantity(symbol, quantity, symbol_info):
    info = symbol_info[symbol]
    step_size = None
    min_qty = None
    max_qty = None

    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            step_size = float(f['stepSize'])
            min_qty = float(f['minQty'])
            max_qty = float(f['maxQty'])
            break

    if step_size and min_qty and max_qty:
        quantity = max(min(quantity, max_qty), min_qty)
        quantity = round_step_size(quantity, step_size)
        if quantity < min_qty or quantity > max_qty:
            logging.warning(f"Cantidad ajustada {quantity} fuera de límites para {symbol}. Min: {min_qty}, Max: {max_qty}")
            return None
    else:
        logging.warning(f"No se encontró 'LOT_SIZE' para {symbol}. Usando cantidad original.")
        quantity = float(f"{quantity:.8f}")

    return quantity

def decide_order_type(pair):
    spread = prices[pair]['ask'] - prices[pair]['bid']
    spread_percentage = spread / prices[pair]['bid'] if prices[pair]['bid'] > 0 else 0
    if spread_percentage < 0.001:  # Menor al 0.1%
        return ORDER_TYPE_MARKET
    else:
        return ORDER_TYPE_LIMIT

def execute_trades(symbol_info, amount_to_invest, route, order_type=ORDER_TYPE_MARKET):
    try:
        logging.info(f"Ejecutando operaciones de arbitraje en la ruta {route} con órdenes {order_type}...")
        amount = amount_to_invest
        for i in range(len(route)):
            pair = route[i]
            base, quote = parse_symbol(pair)
            side = 'BUY' if base + quote == pair else 'SELL'
            qty = amount / prices[pair]['ask'] if side == 'BUY' else amount
            qty = adjust_quantity(pair, qty, symbol_info)
            price = prices[pair]['ask'] if side == 'BUY' else prices[pair]['bid']
            if qty is None:
                logging.warning(f"Cantidad ajustada inválida para {pair}. Operación abortada.")
                return False
            # Ejecutar la orden con manejo de errores
            order = retry_api_call(lambda: client.create_order(
                symbol=pair,
                side=side,
                type=order_type,
                quantity=qty,
                price=price if order_type == ORDER_TYPE_LIMIT else None,
                timeInForce=TIME_IN_FORCE_GTC if order_type == ORDER_TYPE_LIMIT else None
            ))
            if order is None:
                logging.error(f"No se pudo ejecutar la orden para {pair}. Operación abortada.")
                return False
            logging.info(f"Orden de {side} completada: {order}")
            amount = float(order['cummulativeQuoteQty']) if side == 'SELL' else float(order['executedQty'])
            # Implementar stop-loss si es necesario
            time.sleep(0.1)
        logging.info(f"Operación completada en la ruta {route}.")
        return True
    except (BinanceAPIException, BinanceOrderException) as e:
        logging.error(f"Error al ejecutar órdenes: {e}")
        return False
    except Exception as e:
        logging.error(f"Error inesperado al ejecutar órdenes: {e}")
        return False

def retry_api_call(func, max_retries=5):
    for attempt in range(max_retries):
        try:
            return func()
        except (BinanceAPIException, BinanceRequestException, ConnectionError) as e:
            wait_time = 2 ** attempt + random.uniform(0, 1)
            logging.warning(f"Error en la llamada a la API: {e}. Reintentando en {wait_time:.2f} segundos...")
            time.sleep(wait_time)
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            break
    logging.error("Máximo número de reintentos alcanzado.")
    return None

if __name__ == "__main__":
    main()
