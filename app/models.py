from tortoise import fields
from tortoise.models import Model

class CurrencyPair(Model):
    id = fields.IntField(pk=True)
    base_currency = fields.CharField(max_length=10)  # Moneda base
    quote_currency = fields.CharField(max_length=10)  # Moneda de cotización

    class Meta:
        table = "currency_pairs"
        unique_together = ("base_currency", "quote_currency")


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
        unique_together = ("symbol", "timestamp")


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
        unique_together = ("symbol", "timestamp")  # Evitar duplicados por símbolo y tiempo


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
        unique_together = ("symbol", "timestamp")  # Evitar duplicados por símbolo y tiempo


class StrategyResult(Model):
    id = fields.IntField(pk=True)
    strategy_name = fields.CharField(max_length=50)
    final_value = fields.FloatField()
    pnl = fields.FloatField()
    sharpe_ratio = fields.FloatField()
    timestamp = fields.DatetimeField()

    class Meta:
        table = "strategy_results"


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
    tasa_aciertos = fields.FloatField(null=True)  # Tasa de aciertos
    sharpe_ratio = fields.FloatField(null=True)  # Añadir Sharpe Ratio
    max_drawdown = fields.FloatField(null=True)  # Añadir Max Drawdown
    profit_factor = fields.FloatField(null=True)  # Añadir Profit Factor
    win_rate = fields.FloatField(null=True)  # Añadir Tasa de Aciertos

    class Meta:
        unique_together = ("symbol", "timestamp", "interval")  # Evitar duplicados por par y tiempo
        table = "signals"  # Especificar el nombre correcto de la tabla


class BestParams(Model):
    id = fields.IntField(pk=True)
    symbol = fields.CharField(max_length=20)
    interval = fields.CharField(max_length=10)
    EMA_8 = fields.IntField()
    EMA_23 = fields.IntField()
    RSI_threshold = fields.FloatField()
    ADX_threshold = fields.FloatField()

    class Meta:
        table = "best_params"
        unique_together = ("symbol", "interval")  # Evitar duplicados en DB


class TradingData(Model):
    id = fields.IntField(pk=True)
    timestamp = fields.DatetimeField()
    close = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    open = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    high = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    low = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    volume = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    target = fields.SmallIntField(null=True)  # Asumiendo que es una variable binaria para clasificación

    class Meta:
        table = "trading_data"  # Asegúrate de que coincida con el nombre de la tabla en la base de datos

    def __str__(self):
        return f"TradingData(timestamp={self.timestamp}, close={self.close})"
