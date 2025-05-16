#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Binance Futures 保守型做市机器人示例，特点包括：
 - 使用 User Data Stream 处理实际成交 (fills) 和仓位更新
 - 多层订单下单
 - 根据 Gamma 和 ATR 进行调整
 - 资金管理 (最大仓位权重) 和应对市场剧烈波动控制
 - 基于 ATR 的止损和止盈管理
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
# 主要参数
# ============================

# --- ATR, 交易量和价差 ---
CAPITAL = 200  # 参考总资本 (以 USDT 计)
ATR_WINDOW = 14
ATR_MULTIPLIER = 1.6         # 根据 ATR 调整价差 (旨在覆盖费用并留有利润)
MIN_DESIRED_SPREAD = 0.003   # 最小期望价差 (例如 0.3%)

# --- 止损 / 止盈 ---
ATR_MULTIPLIER_STOP = 0.35
ATR_MULTIPLIER_PROFIT = 0.5
VOLUME_MULTIPLIER = 0.0000001

# --- Avellaneda-Stoikov 模型参数 ---
GAMMA = 0.013
T_HORIZON = 1.5

# --- 多层订单 ---
NUM_LAYERS = 3
PRICE_OFFSET = 1.6
LAYER_DISTANCE = 1.4
POST_ONLY = True  # 如果强制使用 POST_ONLY 订单类型 (limit maker)，请设置为 True

# --- 刷新频率和跟踪止损 ---
REFRESH_MIN = 10
REFRESH_MAX = 20
TRAILING_STOP_PCT = 0.05  # 如果账户权益从最高点下跌 5%，则平仓

# --- 费用和最小交易额 ---
FEE_RATE = 0.0001           # 0.01%
MIN_NOTIONAL = 5.0          # Binance 的最小名义价值要求 (在 adjust_qty 函数中使用)

# --- 连接设置 (此示例中为测试网) ---
BINANCE_BASE_URL = "https://testnet.binancefuture.com"
BINANCE_WS_BASE = "wss://stream.binancefuture.com/ws"

API_KEY = "c2593d9143f719cbd3b8487da2c4955814ca9aafc5e8f0f42e57aa51bd2feafa" 
API_SECRET = "af33187e862457359eb53022b994a13cad1c7646b761da117b5edf3f75522112" # <--- 在此处填写你的有效 API SECRET

# --- 要交易的交易对 ---
SYMBOLS = ["BTCUSDT"]

# 最大仓位风险 (以基础资产单位计)。
# 例如：如果是 BTCUSDT，设置为 0.01 表示 0.01 BTC。
RISK_MAX_POSITION = {
    "BTCUSDT": 0.001
}

# 交易对基本信息
INSTRUMENT_INFO = {
    "BTCUSDT": {"tick_size": 0.1, "lot_size": 0.001},
}

# --- 新增的过滤和资金管理常量 ---
MIN_VOLATILITY_RATIO = 0.0005  # 所需的最小波动率 (相对值)
MIN_IMBALANCE = 0.1            # 下单所需的最小订单簿失衡
MAX_POSITION_WEIGHT = 0.1     # 允许的最大投资组合权重 (占总资本的 5%)
MARKET_MOVE_THRESHOLD = 0.05   # 市场剧烈波动阈值: 5%

# =====================
# 日志配置
# =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

# =========================================
# 舍入辅助函数
# =========================================
def get_precision(value):
    """获取给定 tick_size 或 lot_size 的小数位数。"""
    d = Decimal(str(value))
    return abs(d.as_tuple().exponent)

def round_to_tick(value, tick_size, mode="floor"):
    """将价格舍入到最接近的 tick_size。"""
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
    调整数量以满足以下要求：
    - 最小步长 (lot_size)
    - 最小名义价值，例如 5 USDT
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
# Binance Futures 异步客户端
# ==================================
class AsyncBinanceFuturesClient:
    """
    用于与 Binance Futures API 交互的异步客户端。
    处理签名、重试、获取余额、listenKey 等。
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
        params['recvWindow'] = 10000 # Binance 建议 5000-60000

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
                        logger.warning(f"错误 {response.status}: {text}")
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"请求 {endpoint} 时发生异常: {e}")
                await asyncio.sleep(1)
        return None

    async def get_account_balance(self):
        """获取 Futures 账户中的 USDT 余额。"""
        endpoint = "/fapi/v2/balance"
        resp     = await self.send_request("GET", endpoint)
        if resp:
            for item in resp:
                if item.get("asset") == "USDT":
                    return float(item.get("balance", 0))
        return None

    async def start_user_data_stream(self):
        """获取 User Data Stream 的 listenKey。"""
        endpoint = "/fapi/v1/listenKey"
        params = {} # User-Data Stream 不需要额外参数，只需要 POST 方法
        resp = await self.send_request("POST", endpoint, params=params)
        if resp and "listenKey" in resp:
            return resp["listenKey"]
        else:
            logger.error(f"启动 User Data Stream 失败: {resp}")
            return None

    async def keepalive_user_data_stream(self, listen_key):
        """通过每隔约 30 分钟发送一个 PUT 请求来保持 Stream 连接。"""
        endpoint = "/fapi/v1/listenKey"
        params = {"listenKey": listen_key}
        resp = await self.send_request("PUT", endpoint, params=params)
        if resp is not None: # Binance API PUT listenKey 成功时返回空 JSON {}
            logger.info("User Data Stream 心跳发送成功。")
        else:
             # None 表示 send_request 发生了异常或重试失败
             logger.error("User Data Stream 心跳发送失败或发生异常。")


    async def close(self):
        """关闭客户端会话。"""
        await self.session.close()

# ============================
# 风险控制类
# ============================
class RiskManager:
    """
    控制最大敞口、评估止损止盈、
    投资组合权重和市场剧烈波动。
    """
    def __init__(self, max_position,
                 atr_multiplier_stop=ATR_MULTIPLIER_STOP,
                 atr_multiplier_profit=ATR_MULTIPLIER_PROFIT):
        self.max_position          = max_position
        self.atr_multiplier_stop   = atr_multiplier_stop
        self.atr_multiplier_profit = atr_multiplier_profit

    def check_exposure_limit(self, position):
        """检查仓位是否超过基础资产单位的绝对限制。"""
        return abs(position) > self.max_position

    async def evaluate_stop_take(self, bot, atr_value):
        """以 ATR 的倍数评估止损和止盈。"""
        if bot.avg_entry_price is None or bot.mid_price is None or atr_value is None:
            return

        current_price = bot.mid_price

        if bot.current_position > 0: # 多头仓位
            stop_level   = bot.avg_entry_price - self.atr_multiplier_stop * atr_value
            profit_level = bot.avg_entry_price + self.atr_multiplier_profit * atr_value
        else: # 空头仓位
            stop_level   = bot.avg_entry_price + self.atr_multiplier_stop * atr_value
            profit_level = bot.avg_entry_price - self.atr_multiplier_profit * atr_value

        # 止损
        if (bot.current_position > 0 and current_price <= stop_level) or \
           (bot.current_position < 0 and current_price >= stop_level):
            logger.warning(
                f"[{bot.symbol}] 触发止损。当前价格={current_price:.2f}, 止损水平={stop_level:.2f}"
            )
            await bot.close_position() # 调用 bot 的平仓方法

        # 止盈
        elif (bot.current_position > 0 and current_price >= profit_level) or \
             (bot.current_position < 0 and current_price <= profit_level):
            logger.info(
                f"[{bot.symbol}] 触发止盈。当前价格={current_price:.2f}, 止盈水平={profit_level:.2f}"
            )
            await bot.close_position() # 调用 bot 的平仓方法

    async def check_portfolio_limits(self, bot, account_balance):
        """
        验证当前仓位的价值不超过投资组合的最大允许百分比。
        """
        if bot.mid_price is None or account_balance <= 0:
            return False # 无法检查

        position_value = abs(bot.current_position * bot.mid_price)
        weight = position_value / account_balance
        if weight > MAX_POSITION_WEIGHT:
            logger.warning(
                f"[{bot.symbol}] 投资组合超重: {weight*100:.2f}% > {MAX_POSITION_WEIGHT*100:.2f}%"
            )
            await bot.close_position() # 超重则平仓
            return True # 触发限制
        return False # 未触发限制

    async def check_market_move(self, bot, previous_mid_price):
        """
        如果当前价格相对于前一个价格发生剧烈变动（超过定义的阈值），
        则平仓以规避风险。
        """
        if previous_mid_price and bot.mid_price:
            move = abs(bot.mid_price - previous_mid_price) / previous_mid_price
            if move > MARKET_MOVE_THRESHOLD:
                logger.warning(
                    f"[{bot.symbol}] 检测到市场剧烈变动: {move*100:.2f}%"
                )
                await bot.close_all_positions() # 紧急平仓所有持仓和订单
                return True # 触发剧烈变动
        return False # 未触发剧烈变动

# ==================================
# 策略 / 指标
# ==================================
class StrategyManager:
    """
    计算 ATR、动态价差、订单大小和基于波动率的刷新频率。
    同时获取/更新 Binance 的 tick_size 和 lot_size。
    """
    def __init__(self, symbol, client: AsyncBinanceFuturesClient):
        self.symbol = symbol
        self.client = client

        # 从预设或 exchangeInfo 获取精度信息
        if symbol in INSTRUMENT_INFO:
            self.tick_size = INSTRUMENT_INFO[symbol]["tick_size"]
            self.lot_size  = INSTRUMENT_INFO[symbol]["lot_size"]
        else:
             # 如果 SYMBOLS 中添加了新的交易对但 INSTRUMENT_INFO 中没有预设，则抛出错误
            raise ValueError(f"未找到 {symbol} 的配置信息。请在 INSTRUMENT_INFO 中添加。")

        self.pricePrecision    = get_precision(self.tick_size)
        self.quantityPrecision = get_precision(self.lot_size)

        self.atr_multiplier    = ATR_MULTIPLIER
        self.volume_multiplier = VOLUME_MULTIPLIER

        self.dynamic_spread       = 0.001 # 初始值
        self.dynamic_order_size   = 0     # 初始值
        self.dynamic_refresh_rate = REFRESH_MAX # 初始值
        self.last_atr             = None

        self.best_bid_size = 0
        self.best_ask_size = 0

        # 在后台异步更新仪器信息
        asyncio.create_task(self.update_instrument_info())

    async def update_instrument_info(self):
        """从 Binance 获取交易对的过滤规则并更新 tick_size 和 lot_size。"""
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
                        f"[{self.symbol}] 已更新: lotSize={self.lot_size}, tickSize={self.tick_size}"
                    )
                    return # 找到并更新后退出
        # 如果没有找到或者请求失败，使用预设值或初始值，并记录警告
        logger.warning(f"[{self.symbol}] 无法从 exchangeInfo 获取精确的精度信息。将使用预设/初始值。")


    async def get_candle_data(self, interval="1m", limit=ATR_WINDOW+1):
        """下载用于计算 ATR 的 K 线数据。"""
        endpoint = "/fapi/v1/klines"
        params   = {"symbol": self.symbol, "interval": interval, "limit": limit}
        resp     = await self.client.send_request("GET", endpoint, params=params)
        candles  = []
        if resp and isinstance(resp, list): # 确保响应是列表
            for entry in resp:
                 # 检查 K 线数据格式是否正确 (列表长度至少为 5)
                if isinstance(entry, list) and len(entry) > 4:
                    try:
                         candles.append({
                             "open":  float(entry[1]),
                             "high":  float(entry[2]),
                             "low":   float(entry[3]),
                             "close": float(entry[4])
                         })
                    except (ValueError, TypeError) as e:
                        logger.error(f"[{self.symbol}] 解析 K 线数据错误: {e}, 数据: {entry}")
                        continue # 跳过无效 K 线
                else:
                    logger.warning(f"[{self.symbol}] K 线数据格式无效: {entry}")
                    continue # 跳过无效 K 线
        elif resp is not None:
            logger.warning(f"[{self.symbol}] 获取 K 线数据响应格式无效: {resp}")
        return candles

    def calculate_atr(self, candles):
        """从 K 线列表中计算简单的 ATR。"""
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
        # 如果 TR 值列表为空（例如只有一条 K线），避免除以零
        if not tr_values:
             return None
        return sum(tr_values) / len(tr_values)

    async def compute_indicators(self, mid_price):
        """计算 ATR、动态价差、订单大小和刷新频率。"""
        if mid_price is None or mid_price <= 0:
             logger.warning(f"[{self.symbol}] 中间价无效，无法计算指标。")
             return

        # 1) ATR
        candles = await self.get_candle_data()
        if not candles or len(candles) < ATR_WINDOW + 1:
            logger.warning(f"[{self.symbol}] K 线数据不足以计算 ATR ({len(candles)}/{ATR_WINDOW+1} 需要)。")
            return # 数据不足，无法计算 ATR

        atr_val = self.calculate_atr(candles)
        if atr_val is None or atr_val <= 0:
            logger.warning(f"[{self.symbol}] 计算出的 ATR 无效 ({atr_val})。")
            return # ATR 无效

        self.last_atr = atr_val

        # 2) 基于 ATR 的价差
        base_spread = (atr_val / mid_price) * self.atr_multiplier
        # 确保一个最小价差
        self.dynamic_spread = max(base_spread, MIN_DESIRED_SPREAD)


        # 3) 订单基于 24h 交易量 (非常简单的启发式)
        # 注意：一个更稳健的方法应该使用实时订单簿深度或历史成交量
        endpoint = "/fapi/v1/ticker/24hr"
        params   = {"symbol": self.symbol}
        resp     = await self.client.send_request("GET", endpoint, params=params)
        # 安全地获取交易量，如果失败则使用默认值
        vol_24h  = float(resp.get("volume", 1_000_000)) if (resp and isinstance(resp, dict) and "volume" in resp) else 1_000_000
        size     = (vol_24h * self.volume_multiplier)
        # 将计算出的数量舍入到 lot_size 的整数倍
        size     = math.floor(size / self.lot_size) * self.lot_size
        # 确保订单量不小于最小 lot_size
        self.dynamic_order_size = max(size, self.lot_size)

        # 4) 根据波动率调整刷新频率
        rel_vol = atr_val / mid_price
        if rel_vol > 0.002: # 较高波动率
            self.dynamic_refresh_rate = REFRESH_MIN
        else: # 较低波动率
            self.dynamic_refresh_rate = REFRESH_MAX

        logger.info(
            f"[{self.symbol}] ATR={atr_val:.4f}, 中间价={mid_price:.2f}, "
            f"相对价差={self.dynamic_spread:.4f}, 订单大小={self.dynamic_order_size}, "
            f"刷新频率={self.dynamic_refresh_rate}s"
        )

    def round_price(self, price, side):
        """根据买入或卖出方向舍入价格。"""
        if side.upper() == "BUY":
            # 买入价格应向下或精确到 tick_size
            return round_to_tick(price, self.tick_size, mode="floor")
        else:
             # 卖出价格应向上或精确到 tick_size
            return round_to_tick(price, self.tick_size, mode="ceil")

    def adjust_qty(self, qty, price):
        """调整数量以满足 min_notional 和 lot_size 要求。"""
        return adjust_qty(qty, price, self.lot_size, MIN_NOTIONAL)

# ==================================
# 做市机器人
# ==================================
class MarketMakerBot:
    """
    Market Making Bot，负责管理：
    - 与深度和 User Data 的 WS 连接
    - 当前仓位和开放订单
    - 做市策略 (Avellaneda-Stoikov 简化版)
    - 风险控制
    """
    def __init__(self, symbol, client: AsyncBinanceFuturesClient, risk_manager: RiskManager):
        self.symbol       = symbol
        self.client       = client
        self.risk_manager = risk_manager
        self.strategy     = StrategyManager(symbol, client)

        # 机器人状态变量
        self.mid_price        = None # 从深度 WS 获取的最新中间价
        self.current_position = 0.0  # 从 User Data WS 更新的当前仓位
        self.avg_entry_price  = None # 从 User Data WS 更新的平均入场价格
        self.total_pnl        = 0.0  # 累积的总 PnL (简单估算)
        self.open_orders      = {}   # 机器人当前开放的订单 {orderId: {...}}
        self.ws_depth_url     = f"{BINANCE_WS_BASE}/{self.symbol.lower()}@depth5@100ms" # 深度 WS URL
        self.order_book_imbalance = None # 订单簿失衡 (Bid量 vs Ask量)
        self._stop            = False # 停止信号

        # 全局权益跟踪止损变量
        self.initial_balance = None # 机器人启动时的初始余额
        self.max_equity      = None # 记录历史最高账户权益

    def unrealized_pnl_estimate(self):
        """基于 mid_price 与 avg_entry_price 的简单未实现 PnL 计算。"""
        if self.current_position == 0 or not self.mid_price or not self.avg_entry_price:
            return 0.0
        # 根据仓位方向计算未实现 PnL
        if self.current_position > 0: # 多头
            return (self.mid_price - self.avg_entry_price) * self.current_position
        else: # 空头
            return (self.avg_entry_price - self.mid_price) * abs(self.current_position)

    async def get_risk_adjusted_order_size(self):
        """
        根据当前余额和相对于 'CAPITAL' 的线性因子调整订单大小。
        同时检查并执行全局权益跟踪止损。
        """
        balance = await self.client.get_account_balance()
        if balance is None:
            logger.warning(f"[{self.symbol}] 无法获取账户余额。使用默认资本 {CAPITAL} 计算订单大小。")
            balance = CAPITAL # 如果获取余额失败，使用默认资本

        # 首次运行时初始化跟踪止损变量
        if self.initial_balance is None:
            self.initial_balance = balance
            self.max_equity      = balance
            logger.info(f"[{self.symbol}] 初始化全局权益跟踪止损。初始余额/权益={self.initial_balance:.2f}")


        # 计算当前权益 (余额 + 未实现 PnL)
        current_equity = balance + self.unrealized_pnl_estimate()
        # 更新历史最高权益
        if current_equity > self.max_equity:
            self.max_equity = current_equity

        # 检查全局权益跟踪止损是否触发
        if self.max_equity > 0 and (self.max_equity - current_equity) / self.max_equity >= TRAILING_STOP_PCT:
            logger.warning(
                f"[{self.symbol}] 触发全局跟踪止损。当前权益={current_equity:.2f}, 最高点={self.max_equity:.2f}, 回撤比率={(self.max_equity - current_equity) / self.max_equity:.4f}"
            )
            await self.close_all_positions() # 触发则紧急平仓所有仓位和订单
            return 0.0 # 返回 0 意味着当前不应放置任何订单

        # 根据当前余额相对于 CAPITAL 的比例调整基础订单大小
        risk_factor = balance / CAPITAL
        size = self.strategy.dynamic_order_size * risk_factor
        # 确保调整后的订单大小不小于最小 lot_size
        return max(size, self.strategy.lot_size)

    async def close_all_positions(self):
        """如果存在仓位则平仓，并取消所有开放订单。"""
        logger.info(f"[{self.symbol}] 开始平仓并取消所有订单...")
        # 先平仓
        if self.current_position != 0:
            await self.close_position()

        # 取消所有机器人维护的开放订单
        # 使用 list() 创建副本，因为在循环中会修改 self.open_orders
        for order_id, order_info in list(self.open_orders.items()):
            await self.cancel_order(order_id)
            # 在 cancel_order 中会移除，此处不再需要 pop

    async def close_position(self):
        """平仓现有仓位 (如果 > 0 则 SELL，如果 < 0 则 BUY)。"""
        if self.current_position == 0:
            logger.info(f"[{self.symbol}] 没有仓位可平。")
            return

        # 确定平仓方向和数量
        side = "SELL" if self.current_position > 0 else "BUY"
        qty  = abs(self.current_position)
        px   = self.mid_price # 使用当前中间价作为参考价格
        if px is None:
             logger.error(f"[{self.symbol}] 中间价未知，无法平仓！")
             return

        # 根据平仓方向对价格进行舍入
        px   = self.strategy.round_price(px, side)
        # 调整数量以符合交易所要求
        qty  = self.strategy.adjust_qty(qty, px)

        if qty <= 0:
            logger.warning(f"[{self.symbol}] 计算出的平仓数量为 0。不发送平仓单。")
            # 可能需要在此处重置仓位状态，取决于实际需求
            # self.current_position = 0.0
            # self.avg_entry_price = None
            return # 不下数量为零的单

        # 简单估算本次平仓的 PnL (简化了费用和滑点)
        # 这是一个理论计算，实际 PnL 会在 User Data Stream 中确认
        fees = (self.avg_entry_price * qty + px * qty) * FEE_RATE
        if self.current_position > 0: # 多头平仓
            pnl = (px - self.avg_entry_price) * qty - fees
        else: # 空头平仓
            pnl = (self.avg_entry_price - px) * qty - fees
        self.total_pnl += pnl # 累加到总 PnL

        logger.info(
            f"[{self.symbol}] 平仓: {side} {qty} @ {px:.4f} - 估算本次 PnL: {pnl:.2f}. 累计 PnL: {self.total_pnl:.2f}"
        )
        # 下达平仓订单，使用 reduceOnly=True 确保只减少仓位
        await self.place_order(side, qty, px, reduce_only=True)

        # 在订单完全成交（User Data Stream 会通知）后，仓位信息会在 on_user_data_message 中更新。
        # 此处不立即重置仓位，等待 WS 通知确保准确性。
        # self.current_position = 0.0
        # self.avg_entry_price  = None

    async def ws_depth_loop(self):
        """通过 WebSocket 维护深度 (order book) 连接。"""
        logger.info(f"[{self.symbol}] 正在连接 WS 深度: {self.ws_depth_url}")
        while not self._stop:
            try:
                async with websockets.connect(self.ws_depth_url) as ws:
                    logger.info(f"[{self.symbol}] WS 深度连接成功。")
                    async for message in ws:
                        await self.on_depth_message(message)
            except websockets.exceptions.ConnectionClosedOK:
                 logger.info(f"[{self.symbol}] WS 深度连接正常关闭。")
            except Exception as e:
                logger.error(f"[{self.symbol}] WS 深度发生异常: {e}")
            # 如果不是因为停止信号而断开，则重试连接
            if not self._stop:
                logger.info(f"[{self.symbol}] 5 秒后重试 WS 深度连接...")
                await asyncio.sleep(5)

    async def on_depth_message(self, message):
        """处理每条深度更新消息。"""
        try:
            msg = json.loads(message)
        except Exception as e:
            logger.error(f"[{self.symbol}] 解析 WS 深度消息错误: {e}. 消息: {message[:100]}...")
            return

        # 检查消息类型是否为深度更新
        if "e" in msg and msg["e"] == "depthUpdate":
            await self.process_depth_update(msg)

    async def process_depth_update(self, msg):
        """提取 best_bid, best_ask 并计算 imbalance。"""
        # 获取买卖盘数据
        bids = msg.get("b", []) # "b" 字段是买盘 [price, quantity] 列表
        asks = msg.get("a", []) # "a" 字段是卖盘 [price, quantity] 列表

        if not bids or not asks:
            # 如果没有买盘或卖盘，无法计算中间价
            return

        try:
            # 获取最佳买价 (bids 中价格最高) 和最佳卖价 (asks 中价格最低)
            # 只考虑数量大于 0 的报价
            best_bid = max(float(b[0]) for b in bids if float(b[1]) > 0)
            best_ask = min(float(a[0]) for a in asks if float(a[1]) > 0)
            self.mid_price = (best_bid + best_ask) / 2 # 计算中间价

            # 计算最佳买卖单的总量
            self.strategy.best_bid_size = sum(float(b[1]) for b in bids if float(b[0]) == best_bid)
            self.strategy.best_ask_size = sum(float(a[1]) for a in asks if float(a[0]) == best_ask)

            # 计算总买盘量和总卖盘量以计算失衡度
            total_bid = sum(float(b[1]) for b in bids if float(b[1]) > 0)
            total_ask = sum(float(a[1]) for a in asks if float(a[1]) > 0)
            if (total_bid + total_ask) > 0:
                self.order_book_imbalance = (total_bid - total_ask) / (total_bid + total_ask)
            else:
                self.order_book_imbalance = 0.0 # 避免除以零
            # logger.debug(f"[{self.symbol}] 深度更新: Mid={self.mid_price:.2f}, BidSize={self.strategy.best_bid_size:.4f}, AskSize={self.strategy.best_ask_size:.4f}, Imbalance={self.order_book_imbalance:.3f}")


        except Exception as e:
            logger.error(f"[{self.symbol}] 处理深度信息错误: {e}. 消息: {msg}")

    async def ws_user_data_loop(self, listen_key):
        """
        维护 User Data Stream 连接，以接收实时 fills (执行报告)。
        """
        ws_url = f"{BINANCE_WS_BASE}/{listen_key}"
        logger.info(f"[{self.symbol}] 正在连接 WS UserData: {ws_url}")
        while not self._stop:
            try:
                async with websockets.connect(ws_url) as ws:
                    logger.info(f"[{self.symbol}] WS UserData 连接成功。")
                    # User Data Stream 需要定期 ping 以保持连接，虽然 keepalive 在 REST 层做
                    # 但 WS 本身也可能有心跳机制
                    async for message in ws:
                         #logger.debug(f"[{self.symbol}] UserData Msg: {message}") # 可以开启 debug 查看所有消息
                        await self.on_user_data_message(message)
            except websockets.exceptions.ConnectionClosedOK:
                 logger.info(f"[{self.symbol}] WS UserData 连接正常关闭。")
            except Exception as e:
                logger.error(f"[{self.symbol}] WS UserData 发生异常: {e}")
            # 如果不是因为停止信号而断开，则重试连接
            if not self._stop:
                logger.info(f"[{self.symbol}] 5 秒后重试 WS UserData 连接...")
                await asyncio.sleep(5)

    async def on_user_data_message(self, message):
        """处理订单 fills (executionReport) 以更新平均仓位价格。"""
        try:
            msg = json.loads(message)
        except Exception as e:
            logger.error(f"解析 UserData WS 消息错误: {e}. 消息: {message[:100]}...")
            return

        # 检查消息类型是否为执行报告 (订单状态更新)
        if msg.get("e") == "executionReport":
            order_id = msg.get("i") # 订单 ID
            status = msg.get("X") # 订单状态 (NEW, PARTIALLY_FILLED, FILLED, CANCELED, etc.)
            side   = msg.get("S") # 订单方向 (BUY/SELL)
            symbol = msg.get("s") # 交易对符号
            # 检查是否是当前交易对的成交报告
            if symbol != self.symbol:
                 return

            # 只有完全成交或部分成交才会包含 fills 信息
            if status in ["TRADE"]: # TRADE 状态表示本次是成交
                fill_qty = float(msg["l"])  # 本次成交的数量 (lastFilledQuantity)
                fill_px  = float(msg["L"])  # 本次成交的价格 (lastFilledPrice)
                order_status = msg["x"] # 本次事件的执行类型 (TRADE, NEW, CANCELED, etc.)
                cumulative_qty = float(msg["z"]) # 累计成交数量 (filledAccumulatedQuantity)

                if fill_qty <= 0:
                     # 本次成交数量为 0，可能不是实际 fill 事件，忽略
                    return

                # 更新机器人内部维护的仓位和平均入场价格
                signed_fill_qty = fill_qty if side == "BUY" else -fill_qty
                old_pos = self.current_position
                new_pos = old_pos + signed_fill_qty

                # 如果旧仓位为 0，或者仓位方向改变，则新的平均入场价格就是本次成交价格
                if old_pos == 0 or (old_pos > 0 and new_pos <= 0) or (old_pos < 0 and new_pos >= 0):
                    self.avg_entry_price = fill_px
                    self.current_position = new_pos # 更新仓位
                    logger.info(
                        f"[{self.symbol}] Fill (新仓位/反向平仓). side={side}, qty={fill_qty}, px={fill_px}, "
                        f"新仓位={self.current_position:.6f}, 新均价={self.avg_entry_price:.4f}"
                    )
                else:
                    # 如果仓位方向不变，则计算新的加权平均入场价格
                    total_qty = abs(old_pos) + abs(fill_qty)
                    # 避免除以零或接近零
                    if total_qty > 1e-9:
                        # 计算加权平均价
                        weighted_price = (self.avg_entry_price * abs(old_pos)) + (fill_px * fill_qty)
                        new_avg_price = weighted_price / total_qty
                        self.avg_entry_price  = new_avg_price
                        self.current_position = new_pos # 更新仓位
                        logger.info(
                           f"[{self.symbol}] Fill (加仓). side={side}, qty={fill_qty}, px={fill_px}, "
                           f"新仓位={self.current_position:.6f}, 新均价={self.avg_entry_price:.4f}"
                        )
                    else:
                         logger.error(f"[{self.symbol}] 计算新均价时总数量接近零. OldPos={old_pos}, FillQty={fill_qty}")
                         # 这种情况下，可能需要重新同步仓位信息，但此处代码未实现

            # 订单状态如果是 CANCELED 或 EXPIRED，从开放订单列表中移除
            elif status in ["CANCELED", "EXPIRED"]:
                if order_id in self.open_orders:
                    popped_order = self.open_orders.pop(order_id)
                    logger.info(f"[{self.symbol}] 订单 {order_id} 已从开放订单列表移除 (状态: {status})。")
                # else: # 订单可能不是机器人下的，或者之前就已移除
                #    logger.debug(f"[{self.symbol}] 收到非机器人订单 {order_id} 的状态 {status}。")
            # elif status in ["FILLED"]: # Fully filled
                 # 对于完全成交的订单，在处理完最后的 TRADE 事件后，其累计成交量会等于订单量
                 # 此时可以从 open_orders 移除，但因为 TRADE 事件已经更新了仓位，移除不是必须的
                 # 而且如果在 update_orders 循环中取消订单，也会触发 CANCELED 状态并移除
                 # 所以这里可以省略，依赖 CANCELED 或 EXPIRED 移除
                 # if order_id in self.open_orders:
                 #     popped_order = self.open_orders.pop(order_id)
                 #     logger.info(f"[{self.symbol}] 订单 {order_id} 已从开放订单列表移除 (状态: {status})。")


    async def update_orders(self, desired_orders):
        """
        更新开放订单簿：
        - 取消不在 `desired_orders` 中的订单
        - 创建在 `desired_orders` 中但不存在的订单。
        """
        # 定义价格和数量的容忍度，以应对浮点数精度问题
        price_tolerance = self.strategy.tick_size * 0.1 # 价格允许有微小差异
        qty_tolerance   = self.strategy.lot_size * 0.1  # 数量允许有微小差异

        # 获取机器人当前知道的开放订单列表
        existing_orders = list(self.open_orders.values())
        orders_to_create = []

        # 1) 确定需要创建的新订单：检查 desired_orders 中的每个订单是否已存在于 existing_orders
        for d_order in desired_orders:
            match_found = False
            for e_order in existing_orders:
                 # 检查方向、价格和数量是否在容忍范围内匹配
                if (e_order["side"] == d_order["side"] and
                    abs(e_order["price"] - d_order["price"]) < price_tolerance and
                    abs(e_order["qty"] - d_order["qty"]) < qty_tolerance):
                    match_found = True
                    break # 找到匹配项，不需要再检查 existing_orders
            if not match_found:
                # 没有找到匹配项，这个订单需要创建
                orders_to_create.append(d_order)

        # 2) 确定需要取消的订单：检查 existing_orders 中的每个订单是否仍然在 desired_orders 中
        for e_order in existing_orders:
            match_found = any(
                 # 检查 existing_order 是否与 desired_orders 中的任何订单匹配
                (e_order["side"] == d_order["side"] and
                 abs(e_order["price"] - d_order["price"]) < price_tolerance and
                 abs(e_order["qty"] - d_order["qty"]) < qty_tolerance)
                for d_order in desired_orders
            )
            if not match_found:
                # 没有在 desired_orders 中找到匹配项，这个订单需要取消
                await self.cancel_order(e_order["orderId"])
                # cancel_order 成功后会在 on_user_data_message 中处理 CANCELED 状态并移除，此处不再需要 pop

        # 3) 创建新的订单
        for order in orders_to_create:
            placed_order = await self.place_order(
                order["side"],
                order["qty"],
                order["price"],
                reduce_only=order.get("reduce_only", False) # 从 desired_order 中获取 reduce_only 标志
            )
            if placed_order:
                # 如果下单成功，将订单信息添加到机器人维护的 open_orders 字典中
                self.open_orders[placed_order["orderId"]] = placed_order

    async def place_order(self, side, qty, price, reduce_only=False):
        """
        发送一个 LIMIT 订单 (如果 POST_ONLY=True 则为 LIMIT_MAKER)。
        在发送前调整数量以符合 min_notional 和 lot_size。
        """
        endpoint = "/fapi/v1/order"
        # 先将价格舍入到符合交易所 tick_size 的精度
        # 注意：这里应该在计算 desired_orders 时就进行舍入，这里是二次确认
        # price = self.strategy.round_price(price, side)

        # 调整数量以符合 lot_size 和 min_notional
        qty      = self.strategy.adjust_qty(qty, price)
        # 如果调整后的数量小于等于零，不发送订单
        if qty <= 0:
            logger.warning(f"[{self.symbol}] 调整后数量为 0 ({side} @ {price:.4f}) => 不发送订单。")
            return None

        # 构建订单参数
        params = {
            "symbol": self.symbol,
            "side": side,
            "type": "LIMIT" if not POST_ONLY else "LIMIT_MAKER", # 根据配置选择订单类型
            "timeInForce": "GTC", # Good Till Cancel
            "quantity": qty,
            "price": price
        }
        # 如果需要 reduceOnly 属性，添加到参数中
        if reduce_only:
            params["reduceOnly"] = "true"

        # 发送下单请求
        resp = await self.client.send_request("POST", endpoint, params=params)
        # 检查响应是否包含 orderId 表示下单成功
        if resp and "orderId" in resp:
            order_id = resp["orderId"]
            logger.info(
                f"[{self.symbol}] 订单 {side} 创建成功: ID={order_id}, price={price:.4f}, qty={qty:.4f}, reduceOnly={reduce_only}, type={'LIMIT_MAKER' if POST_ONLY else 'LIMIT'}"
            )
            # 返回订单信息以便添加到 open_orders 字典中
            return {"orderId": order_id, "side": side, "qty": qty, "price": price}
        else:
            # 下单失败，记录警告
            logger.warning(f"[{self.symbol}] 创建订单 {side} 失败: {resp}. Params: {params}")
        return None # 下单失败

    async def cancel_order(self, order_id):
        """取消一个特定的开放订单。"""
        if order_id is None:
             return # 无效 orderId

        endpoint = "/fapi/v1/order"
        params   = {"symbol": self.symbol, "orderId": order_id}

        # 发送取消订单请求
        resp = await self.client.send_request("DELETE", endpoint, params=params)

        # Binance 取消订单成功通常会返回订单的最终状态信息
        if resp and "orderId" in resp and resp["orderId"] == order_id:
            logger.info(f"[{self.symbol}] 订单 {order_id} 取消请求发送成功。最终状态: {resp.get('status')}")
            # 注意：订单的最终状态（CANCELED 或 FILLED 等）会在 User Data Stream 中确认
            # open_orders 字典的移除会在 on_user_data_message 中处理 CANCELED 状态时完成
        else:
            # 取消请求失败
            logger.warning(f"[{self.symbol}] 取消订单 {order_id} 失败: {resp}")


    async def main_loop(self):
        """策略主循环，根据 'dynamic_refresh_rate' 重复执行。"""
        previous_mid_price = None # 用于检查市场剧烈波动

        # 在循环开始前先计算一次指标和动态参数，并尝试同步开放订单
        # 这样可以在启动时就放置初始订单
        logger.info(f"[{self.symbol}] 机器人启动，执行首次初始化和下单...")
        # 尝试获取并更新交易对精度信息 (已经在 StrategyManager 初始化时后台运行，但这里可以等待一下确保)
        # await self.strategy.update_instrument_info() # 不阻塞主循环，让后台任务完成
        # 等待 Order Book WS 第一次更新，获取初始中间价
        while self.mid_price is None and not self._stop:
             logger.info(f"[{self.symbol}] 等待接收初始中间价...")
             await asyncio.sleep(1)
        if self._stop:
            logger.info(f"[{self.symbol}] 启动过程中停止。")
            await self.close_all_positions()
            return # 停止标志已设置，退出

        # 首次计算动态参数和潜在的订单大小/价格
        try:
            await self.strategy.compute_indicators(self.mid_price)
        except Exception as e:
            logger.error(f"[{self.symbol}] 首次 compute_indicators 错误: {e}")
            # 即使计算失败，也尝试继续，可能使用默认值

        # 尝试同步一次开放订单 (虽然逻辑主要在 update_orders 里，但启动时清旧单可能有用)
        # 更好的启动流程可能包含从 API 获取现有开放订单，但这代码没有实现
        # 此处简化为直接进入循环并根据计算结果尝试下单

        logger.info(f"[{self.symbol}] 机器人主循环启动。")
        while not self._stop:
            try:
                # 1) 绝对仓位限制检查
                if self.risk_manager.check_exposure_limit(self.current_position):
                    logger.warning(
                        f"[{self.symbol}] 仓位 {self.current_position:.6f} EXCEDES ABSOLUTE LIMIT {self.risk_manager.max_position}. Closing position."
                    )
                    # await self.close_all_positions() # check_exposure_limit 内部调用 close_all_positions
                    await asyncio.sleep(2) # 等待平仓执行
                    continue # 跳过本次下单循环

                # 2) 检查投资组合权重限制
                account_balance = await self.client.get_account_balance()
                # check_portfolio_limits 内部调用 close_position 或 close_all_positions
                if account_balance is not None and await self.risk_manager.check_portfolio_limits(self, account_balance):
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate) # 等待平仓执行，然后按刷新率等待
                    continue # 跳过本次下单循环

                # 3) 检查订单簿最小流动性 (最佳买卖单量)
                # 这是一个简单的流动性指标
                if (self.strategy.best_bid_size < self.strategy.lot_size * 3 or
                    self.strategy.best_ask_size < self.strategy.lot_size * 3):
                    logger.info(f"[{self.symbol}] 订单簿流动性不足 (最佳买卖单量 < {self.strategy.lot_size * 3})。等待。")
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue # 跳过本次下单循环

                # 4) 重新计算指标 (ATR, Spread, Order Size, Refresh Rate)
                # 使用最新的中间价 self.mid_price
                if self.mid_price is not None:
                     await self.strategy.compute_indicators(self.mid_price)
                else:
                     logger.warning(f"[{self.symbol}] 中间价未知，跳过指标计算。")
                     await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                     continue # 没有中间价无法继续

                # 确保 ATR 已计算
                if self.strategy.last_atr is None or self.strategy.last_atr <= 0:
                    logger.warning(f"[{self.symbol}] ATR 未计算或无效 ({self.strategy.last_atr})。等待。")
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue

                # 5) 波动率过滤
                # 如果市场波动率过低，做市可能无利可图或难以成交
                if self.mid_price > 0: # 再次确认中间价有效
                    rel_vol = self.strategy.last_atr / self.mid_price
                    if rel_vol < MIN_VOLATILITY_RATIO:
                        logger.info(
                            f"[{self.symbol}] 波动率过低 (相对波动率={rel_vol:.5f} < {MIN_VOLATILITY_RATIO})。不放置订单。"
                        )
                        # 在波动率低时可以频繁检查，或者用动态刷新率
                        await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                        continue # 跳过本次下单循环

                # 6) 订单簿失衡过滤
                # 如果订单簿失衡不显著，可能意味着市场方向不明确，保守做市可能风险较高
                imbalance = self.order_book_imbalance if self.order_book_imbalance is not None else 0.0
                if abs(imbalance) < MIN_IMBALANCE:
                    logger.info(
                        f"[{self.symbol}] 订单簿失衡度={imbalance:.3f} < 最小失衡度 ({MIN_IMBALANCE})。不放置订单。"
                    )
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue # 跳过本次下单循环

                # 7) 检查动态价差是否满足最小期望
                # 如果计算出的动态价差过小，可能不足以覆盖费用和风险
                spread = self.strategy.dynamic_spread
                if spread < MIN_DESIRED_SPREAD:
                    logger.info(
                        f"[{self.symbol}] 计算价差 ({spread:.4f}) < 最小期望价差 ({MIN_DESIRED_SPREAD})。不放置订单。"
                    )
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue # 跳过本次下单循环

                # 8) 根据 Avellaneda-Stoikov 计算预留价格 (Reservation Price)
                # 这是机器人理想的无风险价格点，它考虑了当前中间价和库存风险
                # reservation_px = current_mid_price - gamma * sigma^2 * inventory * (T)
                # sigma (波动率) 可以用 ATR/sqrt(时间) 来近似，但这里直接用 ATR 的影响项简化
                # effective_volatility_term = GAMMA * (self.strategy.last_atr**2) * T_HORIZON
                # 调整后的预留价格，偏离中间价，偏离方向取决于持仓方向
                # 多头持仓(>0) -> 预留价向下偏离中间价 (想卖出更高)
                # 空头持仓(<0) -> 预留价向上偏离中间价 (想买入更低)
                # 注意：这里的实现 gamma * effective_atr**2 是一个常数影响项，而不是 sigma^2 * T
                # 这只是借用了 Avellaneda-Stoikov 的思想，简化了
                reservation_px = self.mid_price - GAMMA * self.strategy.last_atr * self.current_position * T_HORIZON # 简化为直接使用 ATR 乘以系数

                # 9) 计算风险调整后的订单大小
                # 这个方法内部会检查全局权益止损并可能返回 0
                risk_order_size = await self.get_risk_adjusted_order_size()
                if risk_order_size <= 0:
                    logger.warning(f"[{self.symbol}] risk_order_size=0 => 不进行操作 (可能触发了全局止损或余额不足)。")
                    # 如果 risk_order_size=0，表示触发了全局止损或其他资金限制，无需放置订单
                    await asyncio.sleep(self.strategy.dynamic_refresh_rate)
                    continue # 跳过本次下单循环

                # 10) 计算多层订单的价格和数量
                desired_orders = []
                # 基础买卖价，以预留价格为中心，加上/减去动态价差的一半
                base_bid_price = reservation_px - spread / 2
                base_ask_price = reservation_px + spread / 2

                # 根据订单簿失衡和自身持仓进行订单数量的偏斜 (Skew) 调整
                # 这个 Skew 逻辑可以根据实际需求和回测结果调整
                # 失衡度 > 0 表示买盘强，可以偏向多下卖单；失衡度 < 0 表示卖盘强，可以偏向多下买单
                # 持仓 > 0 (多头) 表示库存过剩，可以偏向多下卖单；持仓 < 0 (空头) 表示库存不足，可以偏向多下买单
                # 这里的实现是简单的乘法调整
                bid_qty_factor = 1.0
                ask_qty_factor = 1.0

                # 根据订单簿失衡调整
                # 失衡度越大 (越接近 1)，买单量因子越小，卖单量因子越大 (偏向卖)
                # 失衡度越小 (越接近 -1)，买单量因子越大，卖单量因子越小 (偏向买)
                bid_qty_factor *= (1 - imbalance) # 例如 imbalance=0.5, bid_factor=0.5
                ask_qty_factor *= (1 + imbalance) # 例如 imbalance=0.5, ask_factor=1.5

                # 根据自身持仓调整 (inventory skew)
                # 持仓 > 0，偏向卖 (bid_factor减小, ask_factor增大)
                # 持仓 < 0，偏向买 (bid_factor增大, ask_factor减小)
                # 这个实现将仓位简单映射到因子调整，可能需要更复杂的逻辑
                position_ratio = self.current_position / self.risk_manager.max_position # 仓位占最大允许仓位的比例
                bid_qty_factor *= (1 - position_ratio) # 多头时减小买单因子
                ask_qty_factor *= (1 + position_ratio) # 多头时增大卖单因子

                # 确保因子不为负或过小
                bid_qty_factor = max(0.1, bid_qty_factor) # 至少保留少量买单
                ask_qty_factor = max(0.1, ask_qty_factor) # 至少保留少量卖单


                # 计算总的买卖数量 (在所有层中分配的总量)
                total_bid_size = risk_order_size * bid_qty_factor
                total_ask_size = risk_order_size * ask_qty_factor

                # 将总数量分配到每一层订单
                qty_per_bid_layer = total_bid_size / NUM_LAYERS if NUM_LAYERS > 0 else 0
                qty_per_ask_layer = total_ask_size / NUM_LAYERS if NUM_LAYERS > 0 else 0


                for layer_i in range(1, NUM_LAYERS + 1):
                    # 计算每一层相对于基础价格的偏移
                    # 偏移量随着层数增加而增大
                    offset = (layer_i - 1) * LAYER_DISTANCE
                    # 计算每一层的买卖价格
                    layer_bid_px = base_bid_price - (PRICE_OFFSET + offset)
                    layer_ask_px = base_ask_price + (PRICE_OFFSET + offset)

                    # 将计算出的价格舍入到符合交易所 tick_size 的精度
                    layer_bid_px = self.strategy.round_price(layer_bid_px, "BUY")
                    layer_ask_px = self.strategy.round_price(layer_ask_px, "SELL")

                    # 检查价格是否有效 (例如不是负数，虽然在正常市场不会发生)
                    if layer_bid_px <= 0 or layer_ask_px <= 0:
                        logger.warning(f"[{self.symbol}] 计算出的订单价格无效 (层 {layer_i}): BID={layer_bid_px:.4f}, ASK={layer_ask_px:.4f}")
                        continue # 跳过此层

                    # 计算每一层的数量，并调整到符合交易所 lot_size 和 min_notional 要求
                    adjusted_bid_qty = self.strategy.adjust_qty(qty_per_bid_layer, layer_bid_px)
                    adjusted_ask_qty = self.strategy.adjust_qty(qty_per_ask_layer, layer_ask_px)

                    # 只有数量大于 0 才添加订单
                    if adjusted_bid_qty > 0:
                        desired_orders.append({
                            "side": "BUY",
                            "qty": adjusted_bid_qty,
                            "price": layer_bid_px
                        })
                    if adjusted_ask_qty > 0:
                        desired_orders.append({
                            "side": "SELL",
                            "qty": adjusted_ask_qty,
                            "price": layer_ask_px
                        })

                logger.info(f"[{self.symbol}] 生成 {len(desired_orders)} 个订单 ({NUM_LAYERS} 层买/卖). Spread={spread:.4f}, ImbalanceSkewed.")

                # 11) 更新交易所的订单：取消旧的，放置新的
                await self.update_orders(desired_orders)

                # 12) 如果存在开放仓位，检查是否触发基于 ATR 的止损或止盈
                if self.current_position != 0 and self.strategy.last_atr is not None:
                    await self.risk_manager.evaluate_stop_take(self, self.strategy.last_atr)
                # Note: 全局权益跟踪止损在 get_risk_adjusted_order_size 中检查

                # 13) 检查市场是否有剧烈变动 (基于中间价)
                if previous_mid_price is not None:
                    await self.risk_manager.check_market_move(self, previous_mid_price)
                # 更新前一个中间价
                previous_mid_price = self.mid_price

            except Exception as e:
                 # 捕获循环内的其他未处理异常，记录并继续
                 logger.error(f"[{self.symbol}] 主循环发生未处理异常: {e}", exc_info=True)


            # 等待动态计算出的刷新时间，然后进行下一轮迭代
            await asyncio.sleep(self.strategy.dynamic_refresh_rate)

        # 当停止标志 _stop 被设置为 True 时，主循环退出
        logger.info(f"[{self.symbol}] 主循环已停止。")
        # 在退出主循环后，执行清理工作：关闭仓位和取消订单
        # (这个清理工作已经在 main() 函数中的 finally 块或 KeyboardInterrupt 处理中完成)
        # await self.close_all_positions() # 放在这里也可以，但确保只执行一次


    async def run(self):
        """
        启动主要任务：
        - 深度 WS
        - 用户数据 WS
        - listenKey 心跳任务
        - 交易主循环
        """
        logger.info(f"[{self.symbol}] 启动 Bot 任务...")
        # 启动深度 WebSocket 连接任务
        depth_task = asyncio.create_task(self.ws_depth_loop())

        # 启动 User Data WebSocket 连接任务 (需要先获取 listenKey)
        listen_key = await self.client.start_user_data_stream()
        user_data_task = None
        keepalive_task = None
        if listen_key:
            user_data_task = asyncio.create_task(self.ws_user_data_loop(listen_key))

            # 启动 listenKey 心跳任务
            async def keepalive_loop():
                while not self._stop:
                    await asyncio.sleep(1800) # 每 30 分钟发送一次心跳
                    if not self._stop and listen_key: # 确保没有停止且 listenKey 有效
                        await self.client.keepalive_user_data_stream(listen_key)

            keepalive_task = asyncio.create_task(keepalive_loop())
        else:
            logger.error(f"[{self.symbol}] 无法获取 listenKey。User Data Stream 和 Keepalive 任务将不启动。将无法实时更新仓位和 fills！")


        # 启动策略主循环任务
        main_task = asyncio.create_task(self.main_loop())

        # 收集所有需要并发运行的任务
        tasks = [depth_task, main_task]
        if user_data_task:
            tasks.append(user_data_task)
        if keepalive_task:
             tasks.append(keepalive_task)


        # 等待所有任务完成 (如果其中一个任务失败或被取消)
        # 如果某个关键任务（如 WS 连接）异常退出，可能需要重新启动或处理
        # 目前简单的处理是如果任何一个任务异常退出，await asyncio.gather 会传播异常
        # 外层 main() 函数会捕获并处理
        await asyncio.gather(*tasks)
        logger.info(f"[{self.symbol}] Bot 任务结束。")


    def stop(self):
        """发送信号以有序停止机器人。"""
        logger.info(f"[{self.symbol}] 收到停止信号...")
        self._stop = True # 设置停止标志

# ======================
# 主函数
# ======================
async def main():
    # 创建 Binance Futures 客户端实例
    client = AsyncBinanceFuturesClient(API_KEY, API_SECRET, BINANCE_BASE_URL)
    bots   = [] # 存储每个交易对的 bot 实例
    tasks  = [] # 存储每个 bot 的运行任务

    try:
        # 为每个配置的交易对创建一个 MarketMakerBot 实例并启动其运行任务
        for sym in SYMBOLS:
            # 为每个交易对创建一个风险管理器实例
            if sym not in RISK_MAX_POSITION:
                 logger.error(f"交易对 {sym} 没有配置 RISK_MAX_POSITION！跳过。")
                 continue
            if sym not in INSTRUMENT_INFO:
                 logger.warning(f"交易对 {sym} 没有在 INSTRUMENT_INFO 中预设精度，将尝试从 exchangeInfo 获取。")
                 # continue # 即使没有预设，也尝试运行，依赖 update_instrument_info

            risk_manager = RiskManager(max_position=RISK_MAX_POSITION[sym])
            bot = MarketMakerBot(sym, client, risk_manager)
            bots.append(bot) # 添加到 bot 列表以便后续停止
            tasks.append(asyncio.create_task(bot.run())) # 创建并启动 bot 的运行任务

        # 等待所有 bot 任务完成
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.info("通过 KeyboardInterrupt (Ctrl+C) 接收到停止信号...")
        # 优雅地停止所有 bot
        for b in bots:
            b.stop()
        # 等待所有 bot 的任务真正结束
        await asyncio.gather(*tasks, return_exceptions=True) # 使用 return_exceptions=True 避免因任务取消而中断 gather

    except Exception as e:
         logger.error(f"主程序发生未处理异常: {e}", exc_info=True)

    finally:
        # 在程序结束时执行清理工作
        total_pnl = sum(b.total_pnl for b in bots) # 计算所有 bot 的总累积 PnL
        logger.info(f"所有交易对总估计 PnL: {total_pnl:.2f}")

        # 将总 PnL 保存到本地 CSV 文件以供历史记录
        csv_file_path = "pnl_binance_log.csv"
        file_exists = os.path.isfile(csv_file_path)
        try:
             with open(csv_file_path, "a", newline="", encoding="utf-8") as f: # 指定 utf-8 编码
                writer = csv.writer(f)
                if not file_exists:
                    # 如果文件不存在，写入表头
                    writer.writerow(["timestamp", "symbols", "ATR_WINDOW", "ATR_MULTIPLIER", "VOLUME_MULTIPLIER", "total_pnl"])
                # 写入当前记录
                writer.writerow([
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    ",".join(SYMBOLS), # 记录交易对列表
                    ATR_WINDOW,
                    ATR_MULTIPLIER,
                    VOLUME_MULTIPLIER,
                    total_pnl
                ])
             logger.info(f"PnL 记录已写入 {csv_file_path}")
        except Exception as e:
             logger.error(f"写入 PnL 日志文件失败: {e}")


        # 关闭 Binance 客户端的异步会话
        await client.close()
        logger.info("客户端会话已关闭。程序已完成。")

if __name__ == "__main__":
    # 运行 asyncio 主函数
    asyncio.run(main())