#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ejemplo de bot de Market Making conservador para Binance Futures con:
 - User Data Stream para manejar fills reales y posición
 - Múltiples capas de órdenes
 - Ajustes en gamma y ATR
 - Gestión de cartera (peso máximo) y control ante movimientos bruscos
 - Manejo de stop loss y take profit basados en ATR
"""

import asyncio
import math
import json
import hmac
import hashlib
import logging
import csv
import os
import time
from datetime import datetime
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN, ROUND_UP

import aiohttp
import websockets

# ============================
# Parámetros Principales
# ============================

# --- ATR, Volumen y Spread ---
CAPITAL = 200  # Capital total de referencia (en USDT)
ATR_WINDOW = 14
ATR_MULTIPLIER = 1.6         # Ajusta el spread basado en ATR (cubre comisión y deja ganancia)
MIN_DESIRED_SPREAD = 0.003   # Spread mínimo deseado (ej. 0.3%)

# --- Stop Loss / Take Profit ---
ATR_MULTIPLIER_STOP = 0.35
ATR_MULTIPLIER_PROFIT = 0.5
VOLUME_MULTIPLIER = 0.0000001

# --- Modelo Avellaneda-Stoikov ---
GAMMA = 0.013
T_HORIZON = 1.5

# --- Órdenes en múltiples capas ---
NUM_LAYERS = 3
PRICE_OFFSET = 1.6
LAYER_DISTANCE = 1.4
POST_ONLY = False  # Si quieres forzar sólo órdenes tipo POST_ONLY (limit maker), pon True

# --- Frecuencia de refresco y trailing stop ---
REFRESH_MIN = 10
REFRESH_MAX = 20
TRAILING_STOP_PCT = 0.05  # Si cae el equity 5% desde el máximo, cierra posición

# --- Comisión y mínimos ---
FEE_RATE = 0.0001           # 0.01%
MIN_NOTIONAL = 5.0          # Mínimo notional de Binance (usado en la función adjust_qty)

# --- Conexión (Testnet en este ejemplo) ---
BINANCE_BASE_URL = "https://testnet.binancefuture.com"
BINANCE_WS_BASE = "wss://stream.binancefuture.com/ws"

API_KEY = "98dd90555b64331d55025f91e333ac0a55e127247dc3737d13e69461b420db44"       # <--- Coloca aquí tu API KEY válida
API_SECRET = "b205aad519990488f3750b1fccda3c3473b6f54b618db0b2bda104bce46bea1b" # <--- Coloca aquí tu API SECRET válida

# --- Símbolos a operar ---
SYMBOLS = ["BTCUSDT"]

# Riesgo máximo de posición (en unidades del activo). 
# Ej.: si es BTCUSDT y pones 0.01, significa 0.01 BTC.
RISK_MAX_POSITION = {
    "BTCUSDT": 0.001  
}

# Información base del instrumento
INSTRUMENT_INFO = {
    "BTCUSDT": {"tick_size": 0.1, "lot_size": 0.001},
}

# --- Constantes nuevas para filtros y gestión de cartera ---
MIN_VOLATILITY_RATIO = 0.0005  # Volatilidad mínima requerida (relativa)
MIN_IMBALANCE = 0.1            # Imbalance mínimo requerido para colocar órdenes
MAX_POSITION_WEIGHT = 0.05     # Peso máximo permitido en cartera (5% del capital)
MARKET_MOVE_THRESHOLD = 0.05   # Movimiento brusco: 5%

# =====================
# Configuración Logging
# =====================
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

# =========================================
# Funciones auxiliares para redondeos
# =========================================
def get_precision(value):
    """Obtiene la cantidad de decimales para un tick_size o lot_size dado."""
    d = Decimal(str(value))
    return abs(d.as_tuple().exponent)

def round_to_tick(value, tick_size, mode="floor"):
    """Redondea un precio a su tick_size más cercano."""
    precision = get_precision(tick_size)
    d_value   = Decimal(str(value))
    d_tick    = Decimal(str(tick_size))
    if mode == "floor":
        result = (d_value // d_tick) * d_tick
    elif mode == "ceil":
        result = (d_value / d_tick).to_integral_value(rounding=ROUND_UP) * d_tick
    else:
        result = d_value.quantize(d_tick)

    quantizer = Decimal('1.' + '0' * precision)
    return float(result.quantize(quantizer, rounding=ROUND_DOWN))

def adjust_qty(qty, price, lot_size, min_notional):
    """
    Ajusta la cantidad para no violar:
    - El paso mínimo (lot_size)
    - El mínimo notional de 5 USDT (por ejemplo)
    """
    precision      = get_precision(lot_size)
    d_qty          = Decimal(str(qty))
    d_lot          = Decimal(str(lot_size))
    d_price        = Decimal(str(price))
    d_min_notional = Decimal(str(min_notional))

    adjusted = (d_qty // d_lot) * d_lot
    if adjusted * d_price < d_min_notional:
        min_units = (d_min_notional / (d_price * d_lot)).to_integral_value(rounding=ROUND_UP)
        adjusted  = min_units * d_lot

    quantizer = Decimal('1.' + '0' * precision)
    return float(adjusted.quantize(quantizer, rounding=ROUND_DOWN))

# ==================================
# Cliente Asíncrono Binance Futures
# ==================================
class AsyncBinanceFuturesClient:
    """
    Cliente asíncrono para interactuar con la API de Binance Futures.
    Maneja firmas, reintentos y obtiene balance, listenKey, etc.
    """
    def __init__(self, api_key, api_secret, base_url):
        self.api_key    = api_key
        self.api_secret = api_secret.encode('utf-8')
        self.base_url   = base_url
        self.session    = aiohttp.ClientSession()

    def _sign(self, query_string):
        return hmac.new(self.api_secret, query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def send_request(self, method, endpoint, params=None, max_retries=5):
        if params is None:
            params = {}
        params['timestamp']  = int(time.time() * 1000)
        params['recvWindow'] = 10000

        query_string = urlencode(params)
        signature    = self._sign(query_string)
        query_string += f"&signature={signature}"

        url     = self.base_url + endpoint + "?" + query_string
        headers = {"X-MBX-APIKEY": self.api_key}

        for _ in range(max_retries):
            try:
                async with self.session.request(method.upper(), url, headers=headers, timeout=10) as response:
                    if response.status in [200, 201]:
                        return await response.json()
                    else:
                        text = await response.text()
                        logger.warning(f"Error {response.status}: {text}")
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Excepción en request a {endpoint}: {e}")
                await asyncio.sleep(1)
        return None

    async def get_account_balance(self):
        """Obtiene el balance de USDT en la cuenta de Futures."""
        endpoint = "/fapi/v2/balance"
        resp     = await self.send_request("GET", endpoint)
        if resp:
            for item in resp:
                if item.get("asset") == "USDT":
                    return float(item.get("balance", 0))
        return None

    async def start_user_data_stream(self):
        """Obtiene el listenKey para el User Data Stream."""
        endpoint = "/fapi/v1/listenKey"
        resp = await self.send_request("POST", endpoint, params={})
        if resp and "listenKey" in resp:
            return resp["listenKey"]
        else:
            logger.error(f"Error iniciando user data stream: {resp}")
            return None

    async def keepalive_user_data_stream(self, listen_key):
        """Mantiene vivo el stream enviando un PUT cada 30 minutos aprox."""
        endpoint = "/fapi/v1/listenKey"
        params = {"listenKey": listen_key}
        resp = await self.send_request("PUT", endpoint, params=params)
        if resp is not None:
            logger.info("Keep-alive del User Data Stream exitoso.")

    async def close(self):
        await self.session.close()

# ============================
# Clase de Control de Riesgo
# ============================
class RiskManager:
    """
    Controla la exposición máxima, evalúa stops, take profits,
    peso en cartera y movimientos bruscos.
    """
    def __init__(self, max_position, 
                 atr_multiplier_stop=ATR_MULTIPLIER_STOP, 
                 atr_multiplier_profit=ATR_MULTIPLIER_PROFIT):
        self.max_position          = max_position
        self.atr_multiplier_stop   = atr_multiplier_stop
        self.atr_multiplier_profit = atr_multiplier_profit

    def check_exposure_limit(self, position):
        """Chequea si la posición excede el límite absoluto en unidades del activo."""
        return abs(position) > self.max_position

    async def evaluate_stop_take(self, bot, atr_value):
        """Evalúa Stop Loss y Take Profit en múltiplos de ATR."""
        if bot.avg_entry_price is None or bot.mid_price is None or atr_value is None:
            return
        
        current_price = bot.mid_price

        if bot.current_position > 0:
            stop_level   = bot.avg_entry_price - self.atr_multiplier_stop * atr_value
            profit_level = bot.avg_entry_price + self.atr_multiplier_profit * atr_value
        else:
            stop_level   = bot.avg_entry_price + self.atr_multiplier_stop * atr_value
            profit_level = bot.avg_entry_price - self.atr_multiplier_profit * atr_value

        # Stop Loss
        if (bot.current_position > 0 and current_price <= stop_level) or \
           (bot.current_position < 0 and current_price >= stop_level):
            logger.warning(
                f"[{bot.symbol}] Stop Loss activado. Precio actual={current_price:.2f}, nivel stop={stop_level:.2f}"
            )
            await bot.close_position()

        # Take Profit
        elif (bot.current_position > 0 and current_price >= profit_level) or \
             (bot.current_position < 0 and current_price <= profit_level):
            logger.info(
                f"[{bot.symbol}] Take Profit activado. Precio actual={current_price:.2f}, nivel profit={profit_level:.2f}"
            )
            await bot.close_position()

    async def check_portfolio_limits(self, bot, account_balance):
        """
        Verifica que el valor de la posición actual no supere un porcentaje máximo de la cartera.
        """
        if bot.mid_price is None or account_balance <= 0:
            return False

        position_value = abs(bot.current_position * bot.mid_price)
        weight = position_value / account_balance
        if weight > MAX_POSITION_WEIGHT:
            logger.warning(
                f"[{bot.symbol}] Exceso de peso en cartera: {weight*100:.2f}% > {MAX_POSITION_WEIGHT*100:.2f}%"
            )
            await bot.close_position()
            return True
        return False

    async def check_market_move(self, bot, previous_mid_price):
        """
        Si el precio actual se mueve de forma brusca respecto al anterior (más del umbral definido),
        se cierran las posiciones para evitar riesgos.
        """
        if previous_mid_price and bot.mid_price:
            move = abs(bot.mid_price - previous_mid_price) / previous_mid_price
            if move > MARKET_MOVE_THRESHOLD:
                logger.warning(
                    f"[{bot.symbol}] Movimiento brusco del mercado detectado: {move*100:.2f}%"
                )
                await bot.close_all_positions()
                return True
        return False

# ==================================
# Estrategia / Indicadores
# ==================================
class StrategyManager:
    """
    Calcula ATR, spread dinámico, tamaño de orden y refresh rate basado en la volatilidad.
    También obtiene/actualiza tick_size y lot_size de Binance.
    """
    def __init__(self, symbol, client: AsyncBinanceFuturesClient):
        self.symbol = symbol
        self.client = client

        if symbol in INSTRUMENT_INFO:
            self.tick_size = INSTRUMENT_INFO[symbol]["tick_size"]
            self.lot_size  = INSTRUMENT_INFO[symbol]["lot_size"]
        else:
            raise ValueError(f"No hay configuración para {symbol}")

        self.pricePrecision    = get_precision(self.tick_size)
        self.quantityPrecision = get_precision(self.lot_size)

        self.atr_multiplier    = ATR_MULTIPLIER
        self.volume_multiplier = VOLUME_MULTIPLIER
        
        self.dynamic_spread       = 0.001
        self.dynamic_order_size   = 0
        self.dynamic_refresh_rate = REFRESH_MAX
        self.last_atr             = None

        self.best_bid_size = 0
        self.best_ask_size = 0

        # Actualizar instrumento de manera asíncrona
        asyncio.create_task(self.update_instrument_info())

    async def update_instrument_info(self):
        """Obtiene filtros de símbolo desde Binance y actualiza tick_size y lot_size."""
        endpoint = "/fapi/v1/exchangeInfo"
        resp     = await self.client.send_request("GET", endpoint)
        if resp and "symbols" in resp:
            for sym in resp["symbols"]:
                if sym["symbol"] == self.symbol:
                    for f in sym["filters"]:
                        if f["filterType"] == "PRICE_FILTER":
                            self.tick_size = float(f["tickSize"])
                            self.pricePrecision = get_precision(self.tick_size)
                        if f["filterType"] == "LOT_SIZE":
                            self.lot_size = float(f["stepSize"])
                            self.quantityPrecision = get_precision(self.lot_size)
                    logger.info(
                        f"[{self.symbol}] Actualizado: lotSize={self.lot_size}, tickSize={self.tick_size}"
                    )
                    break

    async def get_candle_data(self, interval="1m", limit=ATR_WINDOW+1):
        """Descarga velas para el cálculo del ATR."""
        endpoint = "/fapi/v1/klines"
        params   = {"symbol": self.symbol, "interval": interval, "limit": limit}
        resp     = await self.client.send_request("GET", endpoint, params=params)
        candles  = []
        if resp:
            for entry in resp:
                candles.append({
                    "open":  entry[1],
                    "high":  entry[2],
                    "low":   entry[3],
                    "close": entry[4]
                })
        return candles

    def calculate_atr(self, candles):
        """Calcula el ATR simple a partir de una lista de velas."""
        if len(candles) < 2:
            return None
        tr_values = []
        for i in range(1, len(candles)):
            current = candles[i]
            prev    = candles[i-1]
            high    = float(current["high"])
            low     = float(current["low"])
            prev_close = float(prev["close"])
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low  - prev_close)
            )
            tr_values.append(tr)
        return sum(tr_values) / len(tr_values)

    async def compute_indicators(self, mid_price):
        """Calcula ATR, spread dinámico, tamaño de orden y refresh rate."""
        if mid_price <= 0:
            return

        # 1) ATR
        candles = await self.get_candle_data()
        atr_val = self.calculate_atr(candles)
        if atr_val is None:
            return
        self.last_atr = atr_val

        # 2) Spread basado en ATR
        base_spread = (atr_val / mid_price) * self.atr_multiplier
        if base_spread <= 0:
            base_spread = MIN_DESIRED_SPREAD

        # Asegura un mínimo spread
        if base_spread < MIN_DESIRED_SPREAD:
            factor         = MIN_DESIRED_SPREAD / base_spread
            dynamic_spread = (atr_val / mid_price) * (self.atr_multiplier * factor)
        else:
            dynamic_spread = base_spread
        self.dynamic_spread = dynamic_spread

        # 3) Orden basada en volumen 24h (muy simple)
        endpoint = "/fapi/v1/ticker/24hr"
        params   = {"symbol": self.symbol}
        resp     = await self.client.send_request("GET", endpoint, params=params)
        vol_24h  = float(resp.get("volume", 1_000_000)) if resp else 1_000_000
        size     = (vol_24h * self.volume_multiplier)
        size     = math.floor(size / self.lot_size) * self.lot_size
        self.dynamic_order_size = max(size, self.lot_size)

        # 4) Ajuste de refresco según volatilidad
        rel_vol = atr_val / mid_price
        if rel_vol > 0.002:
            self.dynamic_refresh_rate = REFRESH_MIN
        else:
            self.dynamic_refresh_rate = REFRESH_MAX

        logger.info(
            f"[{self.symbol}] ATR={atr_val:.4f}, Mid={mid_price:.2f}, "
            f"SpreadRel={self.dynamic_spread:.4f}, OrderSize={self.dynamic_order_size}, "
            f"Refresh={self.dynamic_refresh_rate}"
        )

    def round_price(self, price, side):
        """Redondea el precio dependiendo de si es BUY o SELL."""
        if side.upper() == "BUY":
            return round_to_tick(price, self.tick_size, mode="floor")
        else:
            return round_to_tick(price, self.tick_size, mode="ceil")

    def adjust_qty(self, qty, price):
        """Ajusta la cantidad para cumplir min_notional y lot_size."""
        return adjust_qty(qty, price, self.lot_size, MIN_NOTIONAL)

# ==================================
# Bot Market Maker
# ==================================
class MarketMakerBot:
    """
    Bot de Market Making que gestiona:
    - Conexión WS a Depth y User Data
    - Posición actual y órdenes abiertas
    - Estrategia de Market Making (Avellaneda-Stoikov simplificado)
    - Control de riesgos
    """
    def __init__(self, symbol, client: AsyncBinanceFuturesClient, risk_manager: RiskManager):
        self.symbol       = symbol
        self.client       = client
        self.risk_manager = risk_manager
        self.strategy     = StrategyManager(symbol, client)

        self.mid_price        = None
        self.current_position = 0.0
        self.avg_entry_price  = None
        self.total_pnl        = 0.0
        self.open_orders      = {}
        self.ws_depth_url     = f"{BINANCE_WS_BASE}/{self.symbol.lower()}@depth5@100ms"
        self.order_book_imbalance = None
        self._stop            = False

        # Variables para Trailing Stop Global
        self.initial_balance = None
        self.max_equity      = None

    def unrealized_pnl_estimate(self):
        """Cálculo simple del PnL no realizado basado en mid_price vs avg_entry_price."""
        if self.current_position == 0 or not self.mid_price or not self.avg_entry_price:
            return 0.0
        if self.current_position > 0:
            return (self.mid_price - self.avg_entry_price) * self.current_position
        else:
            return (self.avg_entry_price - self.mid_price) * abs(self.current_position)

    async def get_risk_adjusted_order_size(self):
        """
        Ajusta el tamaño de la orden según el balance actual y un factor lineal
        en relación al 'CAPITAL' definido.
        """
        balance = await self.client.get_account_balance()
        if balance is None:
            balance = CAPITAL

        # Inicializa los valores si es la primera vez
        if self.initial_balance is None:
            self.initial_balance = balance
            self.max_equity      = balance

        # Equity actual = balance + PnL no realizado
        current_equity = balance + self.unrealized_pnl_estimate()
        if current_equity > self.max_equity:
            self.max_equity = current_equity

        # Trailing stop global a nivel de equity
        if (self.max_equity - current_equity) / self.max_equity >= TRAILING_STOP_PCT:
            logger.warning(
                f"[{self.symbol}] Activando trailing stop global. Equity actual={current_equity:.2f}, pico={self.max_equity:.2f}"
            )
            await self.close_all_positions()
            return 0.0

        # Ajuste lineal por balance
        risk_factor = balance / CAPITAL
        size = self.strategy.dynamic_order_size * risk_factor
        return max(size, self.strategy.lot_size)

    async def close_all_positions(self):
        """Cierra la posición si la hay y cancela todas las órdenes abiertas."""
        if self.current_position != 0:
            await self.close_position()

        for order in list(self.open_orders.values()):
            await self.cancel_order(order["orderId"])
            self.open_orders.pop(order["orderId"], None)

    async def close_position(self):
        """Cierra la posición existente (si > 0 => SELL, si < 0 => BUY)."""
        if self.current_position == 0:
            return

        side = "SELL" if self.current_position > 0 else "BUY"
        qty  = abs(self.current_position)
        px   = self.mid_price or 0
        px   = self.strategy.round_price(px, side)

        # Cálculo rápido de PnL estimado (comisiones y slippage simplificados)
        fees = (self.avg_entry_price * qty + px * qty) * FEE_RATE
        if self.current_position > 0:
            pnl = (px - self.avg_entry_price) * qty - fees
        else:
            pnl = (self.avg_entry_price - px) * qty - fees
        self.total_pnl += pnl

        logger.info(
            f"[{self.symbol}] Cerrando posición: {side} {qty} @ {px} - PnL: {pnl:.2f}"
        )
        await self.place_order(side, qty, px, reduce_only=True)

        # Resetea los valores de posición
        self.current_position = 0.0
        self.avg_entry_price  = None

    async def ws_depth_loop(self):
        """Mantiene la conexión de profundidad (order book) vía WebSocket."""
        while not self._stop:
            try:
                async with websockets.connect(self.ws_depth_url) as ws:
                    logger.info(f"[{self.symbol}] WS Depth conectado.")
                    async for message in ws:
                        await self.on_depth_message(message)
            except Exception as e:
                logger.error(f"[{self.symbol}] Excepción en WS Depth: {e}")
            if not self._stop:
                logger.info(f"[{self.symbol}] Reintentando WS Depth en 5s...")
                await asyncio.sleep(5)

    async def on_depth_message(self, message):
        """Procesa cada actualización de profundidad."""
        try:
            msg = json.loads(message)
        except Exception as e:
            logger.error(f"[{self.symbol}] Error parseando WS Depth: {e}")
            return

        if "e" in msg and msg["e"] == "depthUpdate":
            await self.process_depth_update(msg)

    async def process_depth_update(self, msg):
        """Extrae best_bid, best_ask y calcula imbalance."""
        bids = msg.get("b", [])
        asks = msg.get("a", [])
        if not bids or not asks:
            return
        try:
            best_bid = max(float(b[0]) for b in bids if float(b[1]) > 0)
            best_ask = min(float(a[0]) for a in asks if float(a[1]) > 0)
            self.mid_price = (best_bid + best_ask) / 2

            best_bid_size = sum(float(b[1]) for b in bids if float(b[0]) == best_bid)
            best_ask_size = sum(float(a[1]) for a in asks if float(a[0]) == best_ask)
            self.strategy.best_bid_size = best_bid_size
            self.strategy.best_ask_size = best_ask_size

            total_bid = sum(float(b[1]) for b in bids if float(b[1]) > 0)
            total_ask = sum(float(a[1]) for a in asks if float(a[1]) > 0)
            if (total_bid + total_ask) > 0:
                self.order_book_imbalance = (total_bid - total_ask) / (total_bid + total_ask)
            else:
                self.order_book_imbalance = 0.0

        except Exception as e:
            logger.error(f"[{self.symbol}] Error al procesar profundidad: {e}")

    async def ws_user_data_loop(self, listen_key):
        """
        Mantiene la conexión al User Data Stream para recibir fills (ejecuciones) en tiempo real.
        """
        ws_url = f"{BINANCE_WS_BASE}/{listen_key}"
        while not self._stop:
            try:
                async with websockets.connect(ws_url) as ws:
                    logger.info(f"[{self.symbol}] WS UserData conectado.")
                    async for message in ws:
                        await self.on_user_data_message(message)
            except Exception as e:
                logger.error(f"[{self.symbol}] Excepción en WS UserData: {e}")
            if not self._stop:
                logger.info(f"[{self.symbol}] Reintentando WS UserData en 5s...")
                await asyncio.sleep(5)

    async def on_user_data_message(self, message):
        """Procesa fills de órdenes (executionReport) para actualizar posición promedio."""
        try:
            msg = json.loads(message)
        except Exception as e:
            logger.error(f"Error parseando UserData WS: {e}")
            return

        if msg.get("e") == "executionReport":
            side   = msg["S"]
            status = msg["X"]
            if status == "TRADE":
                fill_qty = float(msg["l"])  # Cantidad llenada en esta ejecución
                fill_px  = float(msg["L"])  # Precio de esta ejecución
                if fill_qty <= 0:
                    return

                signed_qty = fill_qty if side == "BUY" else -fill_qty
                old_pos = self.current_position
                new_pos = old_pos + signed_qty

                # Si la posición cambia de signo, se reinicia el precio promedio
                if old_pos == 0 or (old_pos * new_pos < 0):
                    self.avg_entry_price = fill_px
                    self.current_position = new_pos
                else:
                    total_qty = abs(old_pos) + abs(fill_qty)
                    weighted_price = (self.avg_entry_price * abs(old_pos)) + (fill_px * fill_qty)
                    new_avg_price = weighted_price / total_qty
                    self.avg_entry_price  = new_avg_price
                    self.current_position = new_pos

                logger.info(
                    f"[{self.symbol}] Fill parcial. side={side}, qty={fill_qty}, px={fill_px}, "
                    f"pos={self.current_position:.6f}, avgPx={self.avg_entry_price:.4f}"
                )

    async def update_orders(self, desired_orders):
        """
        Actualiza el libro de órdenes abiertas:
        - Cancela las que no están en `desired_orders`
        - Crea las que sí están pero no existen ya.
        """
        price_tolerance = self.strategy.tick_size
        qty_tolerance   = self.strategy.lot_size * 0.1

        existing_orders = list(self.open_orders.values())
        orders_to_create = []

        # 1) Determinar órdenes nuevas que NO existan ya
        for d_order in desired_orders:
            match_found = False
            for e_order in existing_orders:
                if (e_order["side"] == d_order["side"] and
                    abs(e_order["price"] - d_order["price"]) < price_tolerance and
                    abs(e_order["qty"] - d_order["qty"]) < qty_tolerance):
                    match_found = True
                    break
            if not match_found:
                orders_to_create.append(d_order)

        # 2) Cancelar órdenes que ya no están deseadas
        for e_order in existing_orders:
            match_found = any(
                (e_order["side"] == d_order["side"] and
                 abs(e_order["price"] - d_order["price"]) < price_tolerance and
                 abs(e_order["qty"] - d_order["qty"]) < qty_tolerance)
                for d_order in desired_orders
            )
            if not match_found:
                await self.cancel_order(e_order["orderId"])
                self.open_orders.pop(e_order["orderId"], None)

        # 3) Crear las órdenes nuevas
        for order in orders_to_create:
            placed_order = await self.place_order(
                order["side"], 
                order["qty"], 
                order["price"], 
                reduce_only=order.get("reduce_only", False)
            )
            if placed_order:
                self.open_orders[placed_order["orderId"]] = placed_order

    async def place_order(self, side, qty, price, reduce_only=False):
        """
        Envía una orden LIMIT (o LIMIT_MAKER si POST_ONLY=True).
        Ajusta la qty al lot_size y min_notional.
        """
        endpoint = "/fapi/v1/order"
        qty      = self.strategy.adjust_qty(qty, price)
        if qty <= 0:
            logger.warning(f"[{self.symbol}] Qty=0 => no se envía orden.")
            return None

        params = {
            "symbol": self.symbol,
            "side": side,
            "type": "LIMIT" if not POST_ONLY else "LIMIT_MAKER",
            "timeInForce": "GTC",
            "quantity": qty,
            "price": price
        }
        if reduce_only:
            params["reduceOnly"] = "true"

        resp = await self.client.send_request("POST", endpoint, params=params)
        if resp and "orderId" in resp:
            order_id = resp["orderId"]
            logger.info(
                f"[{self.symbol}] Orden {side} creada: ID={order_id}, price={price}, qty={qty}, reduce={reduce_only}"
            )
            return {"orderId": order_id, "side": side, "qty": qty, "price": price}
        else:
            logger.warning(f"[{self.symbol}] Error al crear orden {side}: {resp}")
        return None

    async def cancel_order(self, order_id):
        """Cancela una orden abierta específica."""
        endpoint = "/fapi/v1/order"
        params   = {"symbol": self.symbol, "orderId": order_id}
        resp     = await self.client.send_request("DELETE", endpoint, params=params)
        if resp:
            logger.debug(f"[{self.symbol}] Cancelación de orden: {resp}")

    async def main_loop(self):
        """Bucle principal de la estrategia, se repite según 'dynamic_refresh_rate'."""
        previous_mid_price = None
        while not self._stop:
            # 1) Límite de exposición absoluto
            if self.risk_manager.check_exposure_limit(self.current_position):
                logger.warning(
                    f"[{self.symbol}] Posición {self.current_position} excede el límite absoluto."
                )
                await self.close_all_positions()
                await asyncio.sleep(2)
                continue

            # 2) Verificar peso de cartera
            account_balance = await self.client.get_account_balance()
            if account_balance and await self.risk_manager.check_portfolio_limits(self, account_balance):
                await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                continue

            # 3) Verificar liquidez mínima en el order book (best_bid_size / best_ask_size)
            if (self.strategy.best_bid_size < self.strategy.lot_size * 3 or
                self.strategy.best_ask_size < self.strategy.lot_size * 3):
                logger.info(f"[{self.symbol}] Liquidez insuficiente. Esperamos.")
                await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                continue

            # 4) Recalcular indicadores
            try:
                await self.strategy.compute_indicators(self.mid_price or 0)
            except Exception as e:
                logger.error(f"[{self.symbol}] Error en compute_indicators: {e}")

            # 5) Filtro de volatilidad
            if self.strategy.last_atr and self.mid_price:
                rel_vol = self.strategy.last_atr / self.mid_price
                if rel_vol < MIN_VOLATILITY_RATIO:
                    logger.info(
                        f"[{self.symbol}] Volatilidad muy baja (rel_vol={rel_vol:.5f}). No se colocan órdenes."
                    )
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue

            # 6) Filtro de imbalance
            imbalance = self.order_book_imbalance if self.order_book_imbalance is not None else 0.0
            if abs(imbalance) < MIN_IMBALANCE:
                logger.info(
                    f"[{self.symbol}] Imbalance={imbalance:.3f} < MIN_IMBALANCE. No se colocan órdenes."
                )
                await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                continue

            # 7) Chequeo del spread dinámico
            spread = self.strategy.dynamic_spread
            if spread < MIN_DESIRED_SPREAD:
                logger.info(
                    f"[{self.symbol}] Spread ({spread:.4f}) < MIN_DESIRED_SPREAD. No se colocan órdenes."
                )
                await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                continue

            # 8) Reserva de precio según Avellaneda-Stoikov
            effective_atr  = self.strategy.last_atr or 0.0001
            reservation_px = self.mid_price - GAMMA * (effective_atr**2) * self.current_position * T_HORIZON

            # 9) Tamaño de orden ajustado al riesgo
            risk_order_size = await self.get_risk_adjusted_order_size()
            if risk_order_size <= 0:
                logger.warning(f"[{self.symbol}] risk_order_size=0 => No se opera.")
                await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                continue

            # 10) Múltiples capas de órdenes
            desired_orders = []
            base_bid_price = reservation_px - spread / 2
            base_ask_price = reservation_px + spread / 2

            # Ajuste por imbalance (skew)
            if imbalance > 0:
                bid_adjustment = 1 - imbalance
                ask_adjustment = 1 + imbalance
            else:
                bid_adjustment = 1 + abs(imbalance)
                ask_adjustment = 1 - abs(imbalance)

            # Skew por inventario
            if self.current_position > 0:
                base_bid = risk_order_size * 0.3
                base_ask = risk_order_size * 1.7
            elif self.current_position < 0:
                base_bid = risk_order_size * 1.7
                base_ask = risk_order_size * 0.3
            else:
                base_bid = risk_order_size
                base_ask = risk_order_size

            adjusted_bid_size = base_bid * bid_adjustment
            adjusted_ask_size = base_ask * ask_adjustment

            for layer_i in range(1, NUM_LAYERS + 1):
                offset = (layer_i - 1) * LAYER_DISTANCE
                layer_bid_px = self.strategy.round_price(base_bid_price - (PRICE_OFFSET + offset), "BUY")
                layer_ask_px = self.strategy.round_price(base_ask_price + (PRICE_OFFSET + offset), "SELL")

                layer_bid_qty = adjusted_bid_size / NUM_LAYERS
                layer_ask_qty = adjusted_ask_size / NUM_LAYERS

                if layer_bid_qty > 0:
                    desired_orders.append({
                        "side": "BUY",
                        "qty": layer_bid_qty,
                        "price": layer_bid_px
                    })
                if layer_ask_qty > 0:
                    desired_orders.append({
                        "side": "SELL",
                        "qty": layer_ask_qty,
                        "price": layer_ask_px
                    })

            logger.info(f"[{self.symbol}] Generando {NUM_LAYERS} capas de BID/ASK. Spread={spread:.4f}")
            await self.update_orders(desired_orders)

            # 11) Control SL/TP si existe posición abierta
            if self.current_position != 0:
                await self.risk_manager.evaluate_stop_take(self, self.strategy.last_atr)

            # 12) Verificar movimiento brusco del mercado
            if previous_mid_price is not None:
                await self.risk_manager.check_market_move(self, previous_mid_price)
            previous_mid_price = self.mid_price

            # Espera antes de la siguiente iteración
            await asyncio.sleep(self.strategy.dynamic_refresh_rate)

        # Al detener el bot, cierra posición y cancela órdenes.
        await self.close_all_positions()
        logger.info(f"[{self.symbol}] Bot detenido. Posición final: {self.current_position}")

    async def run(self):
        """
        Inicia las tareas principales:
        - WS de profundidad
        - WS de user data
        - Tarea de keepalive del listenKey
        - Bucle principal de trading
        """
        depth_task = asyncio.create_task(self.ws_depth_loop())
        listen_key = await self.client.start_user_data_stream()
        if listen_key:
            user_data_task = asyncio.create_task(self.ws_user_data_loop(listen_key))
        else:
            user_data_task = None

        async def keepalive_loop():
            while not self._stop:
                await asyncio.sleep(1800)
                if listen_key:
                    await self.client.keepalive_user_data_stream(listen_key)

        keepalive_task = asyncio.create_task(keepalive_loop())
        main_task = asyncio.create_task(self.main_loop())

        tasks = [depth_task, main_task, keepalive_task]
        if user_data_task:
            tasks.append(user_data_task)

        await asyncio.gather(*tasks)

    def stop(self):
        """Señal para detener el bot de forma ordenada."""
        self._stop = True
        logger.info(f"[{self.symbol}] Bot detenido (señal de stop activada).")

# ======================
# Función principal
# ======================
async def main():
    client = AsyncBinanceFuturesClient(API_KEY, API_SECRET, BINANCE_BASE_URL)
    bots   = []
    tasks  = []
    try:
        for sym in SYMBOLS:
            risk_manager = RiskManager(max_position=RISK_MAX_POSITION[sym])
            bot = MarketMakerBot(sym, client, risk_manager)
            bots.append(bot)
            tasks.append(asyncio.create_task(bot.run()))

        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.info("Deteniendo bots por KeyboardInterrupt...")
        for b in bots:
            b.stop()

    finally:
        total_pnl = sum(b.total_pnl for b in bots)
        logger.info(f"PnL total estimado: {total_pnl:.2f}")

        # Guarda el PnL en un CSV local para histórico
        file_exists = os.path.isfile("pnl_binance_log.csv")
        with open("pnl_binance_log.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "ATR_WINDOW", "ATR_MULTIPLIER", "VOLUME_MULTIPLIER", "PnL"])
            writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                ATR_WINDOW,
                ATR_MULTIPLIER,
                VOLUME_MULTIPLIER,
                total_pnl
            ])

        await client.close()
        logger.info("Finalizado.")

if __name__ == "__main__":
    asyncio.run(main())
