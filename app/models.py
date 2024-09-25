from tortoise.models import Model
from tortoise import fields
import datetime

class Trade(Model):
    id = fields.IntField(pk=True)
    symbol = fields.CharField(max_length=20, index=True)
    price = fields.FloatField()
    volume = fields.FloatField()
    timestamp = fields.DatetimeField(index=True)
    currency_pair = fields.ForeignKeyField('models.CurrencyPair', related_name='trades', null=True)

    class Meta:
        table = "trades"
        ordering = ["-timestamp"]

class HistoricalPrice(Model):
    id = fields.IntField(pk=True)
    symbol = fields.CharField(max_length=20, index=True)
    open = fields.FloatField()  # Precio de apertura
    high = fields.FloatField()  # Precio más alto
    low = fields.FloatField()  # Precio más bajo
    close = fields.FloatField()  # Precio de cierre
    volume = fields.FloatField()  # Volumen negociado
    timestamp = fields.DatetimeField(index=True)

    class Meta:
        table = "historical_prices"
        ordering = ["-timestamp"]

class Order(Model):
    id = fields.IntField(pk=True)
    symbol = fields.CharField(max_length=20, index=True)
    open = fields.FloatField() 
    type = fields.CharField(max_length=10)  # Tipo de orden (compra/venta)
    price = fields.FloatField()  # Precio de la orden
    volume = fields.FloatField()  # Volumen de la orden
    status = fields.CharField(max_length=10)  # Estado de la orden
    timestamp = fields.DatetimeField(index=True)

    class Meta:
        table = "orders"
        ordering = ["-timestamp"]

class StrategyResult(Model):
    id = fields.IntField(pk=True)
    strategy_name = fields.CharField(max_length=50)
    return_on_investment = fields.FloatField()  # ROI de la estrategia
    success_rate = fields.FloatField()  # Tasa de éxito de la estrategia
    timestamp = fields.DatetimeField(index=True)

    class Meta:
        table = "strategy_results"
        ordering = ["-timestamp"]

class CurrencyPair(Model):
    id = fields.IntField(pk=True)
    base_currency = fields.CharField(max_length=10)  # Moneda base
    quote_currency = fields.CharField(max_length=10)  # Moneda de cotización

    class Meta:
        table = "currency_pairs"
        unique_together = ("base_currency", "quote_currency")
class Signal(Model):
    id = fields.IntField(pk=True)  # Clave primaria
    symbol = fields.CharField(max_length=20)  # Par de trading, ej: BTCUSDT
    close = fields.FloatField()  # Precio de cierre
    ema_8 = fields.FloatField()  # EMA rápida (8 períodos)
    ema_23 = fields.FloatField()  # EMA lenta (23 períodos)
    signal_line = fields.FloatField()  # Línea de señal MACD
    adx = fields.FloatField()  # ADX (Índice de movimiento direccional promedio)
    volume = fields.FloatField()  # Volumen
    higher_trend = fields.CharField(max_length=10)  # Tendencia superior (bullish/bearish)
    signal = fields.IntField()  # Señal generada (1: compra, -1: venta, 0: sin señal)
    timestamp = fields.DatetimeField()  # Marca de tiempo del registro
    interval = fields.CharField(max_length=10)  # Intervalo de la vela, ej: 1h, 4h
    macd = fields.FloatField(null=True)  # Valor de MACD
    obv = fields.FloatField(null=True)  # Valor de On Balance Volume (OBV)
    rsi = fields.FloatField(null=True)  # Valor del RSI
    ema_fast = fields.FloatField(null=True)  # EMA rápida personalizada
    ema_slow = fields.FloatField(null=True)  # EMA lenta personalizada
    trailing_stop = fields.FloatField(null=True)  # Para el Trailing Stop
    return_anualizado = fields.FloatField(null=True)  # Retorno anualizado
    tasa_aciertos = fields.FloatField(null=True)  #
    # Campo único basado en el par (symbol) y el timestamp para evitar duplicados
    class Meta:
        unique_together = ("symbol", "timestamp", "interval")  # Evitar duplicados por par y tiempo

class BestParams(Model):
    symbol = fields.CharField(max_length=20)
    interval = fields.CharField(max_length=10)
    EMA_8 = fields.IntField()
    EMA_23 = fields.IntField()
    RSI_threshold = fields.FloatField()
    ADX_threshold = fields.FloatField()

    class Meta:
        table = "signals"
        unique_together = ("symbol", "timestamp")  # Evitar duplicados en DB
    
    class Meta:
        table = "signals"  # Especificar el nombre correcto de la tabla

    def __str__(self):
        return f"Signal(symbol={self.symbol}, timestamp={self.timestamp}, signal={self.signal})"
