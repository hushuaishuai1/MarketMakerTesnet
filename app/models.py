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
    symbol = fields.CharField(max_length=20)
    close = fields.FloatField()
    ema_8 = fields.FloatField()
    ema_23 = fields.FloatField()
    macd = fields.FloatField()
    signal_line = fields.FloatField()
    rsi = fields.FloatField()
    adx = fields.FloatField()
    volume = fields.FloatField()
    higher_trend = fields.CharField(max_length=10)
    signal = fields.IntField()
    timestamp = fields.DatetimeField()
    interval = fields.CharField(max_length=10)

    class Meta:
        table = "signals"
        unique_together = ("symbol", "timestamp")  # Evitar duplicados en DB
    
    class Meta:
        table = "signals"  # Especificar el nombre correcto de la tabla

    def __str__(self):
        return f"Signal(symbol={self.symbol}, timestamp={self.timestamp}, signal={self.signal})"
