# database_utils.py
import logging
from app.models import Signal
from tortoise.transactions import in_transaction

async def save_metrics_to_db(symbol, timestamp, sharpe_ratio, max_drawdown, profit_factor, win_rate, interval='1h'):
    """
    Guarda las métricas calculadas en la base de datos.

    Args:
        symbol (str): Símbolo del par de criptomonedas.
        timestamp (datetime): Marca de tiempo de las métricas.
        sharpe_ratio (float): Sharpe Ratio calculado.
        max_drawdown (float): Máximo drawdown.
        profit_factor (float): Factor de beneficio.
        win_rate (float): Tasa de éxito de las operaciones.
        interval (str): Intervalo de tiempo.
    """
    try:
        async with in_transaction():
            await Signal.create(
                symbol=symbol,
                timestamp=timestamp,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                profit_factor=profit_factor,
                win_rate=win_rate,
                interval=interval
            )
            logging.info(f"Métricas guardadas para {symbol} en {timestamp}")
    except Exception as e:
        logging.error(f"Error al guardar métricas en la base de datos: {e}", exc_info=True)
