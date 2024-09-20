# backtesting.py

import pandas as pd
from app.models import Signal

def backtest_momentum_strategy(df, initial_capital=100, commission=0.001, slippage=0.001, trailing_stop_percent=0.04):
    # Eliminar filas con valores faltantes en 'close' y 'signal'
    df.dropna(subset=['close', 'signal'], inplace=True)

    capital = initial_capital
    position = 0  # 0 = sin posición, 1 = posición larga
    position_entry_price = 0
    trade_log = []
    quantity = 0  # Cantidad de activos comprados
    stop_loss_price = 0

    for index, row in df.iterrows():
        signal = row['signal']
        price = float(row['close'])
        timestamp = row['timestamp']

        # Convertir timestamp a datetime sin zona horaria
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()
        if timestamp.tzinfo is not None:
            timestamp = timestamp.replace(tzinfo=None)

        # Formatear timestamp a cadena
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        slippage_adjustment = price * slippage
        adjusted_price = price + slippage_adjustment if position == 0 else price - slippage_adjustment

        if position == 1:
            # Actualizar Trailing Stop-Loss
            new_stop_loss_price = max(stop_loss_price, adjusted_price * (1 - trailing_stop_percent))
            stop_loss_price = new_stop_loss_price

            # Verificar si el precio alcanza el Trailing Stop-Loss
            if adjusted_price <= stop_loss_price:
                sale_amount = quantity * adjusted_price
                capital += sale_amount - (sale_amount * commission)
                profit_loss = sale_amount - (quantity * position_entry_price)
                trade_log.append(f"Trailing Stop-Loss ejecutado en {adjusted_price:.2f} en {timestamp_str}, P/L: {profit_loss:.2f}")
                position = 0
                quantity = 0
                continue

        if signal == 1 and position == 0:
            # Comprar
            position = 1
            position_entry_price = adjusted_price
            quantity = capital / (adjusted_price * (1 + commission))
            total_purchase = quantity * adjusted_price
            commission_cost = total_purchase * commission
            capital -= total_purchase + commission_cost
            stop_loss_price = adjusted_price * (1 - trailing_stop_percent)
            trade_log.append(f"Compra a {adjusted_price:.2f} en {timestamp_str}, cantidad: {quantity:.6f}")

        elif signal == -1 and position == 1:
            # Vender
            sale_amount = quantity * adjusted_price
            commission_cost = sale_amount * commission
            capital += sale_amount - commission_cost
            profit_loss = sale_amount - (quantity * position_entry_price)
            trade_log.append(f"Venta a {adjusted_price:.2f} en {timestamp_str}, P/L: {profit_loss:.2f}")
            position = 0
            quantity = 0

    # Cerrar posición al final si está abierta
    if position == 1:
        adjusted_price = price - price * slippage
        sale_amount = quantity * adjusted_price
        commission_cost = sale_amount * commission
        capital += sale_amount - commission_cost
        profit_loss = sale_amount - (quantity * position_entry_price)
        trade_log.append(f"Venta final a {adjusted_price:.2f} en {timestamp_str}, P/L: {profit_loss:.2f}")
        position = 0
        quantity = 0

    return capital, trade_log

def calculate_backtesting_metrics(trade_log, initial_capital, final_capital):
    num_trades = len([log for log in trade_log if 'P/L' in log])
    num_wins = sum(1 for log in trade_log if 'P/L' in log and float(log.split('P/L:')[-1]) > 0)
    num_losses = num_trades - num_wins
    win_rate = (num_wins / num_trades) * 100 if num_trades > 0 else 0
    roi = ((final_capital - initial_capital) / initial_capital) * 100

    # Calcular drawdown máximo
    peak = initial_capital
    max_drawdown = 0
    current_capital = initial_capital
    for log in trade_log:
        if 'P/L' in log:
            profit_loss = float(log.split('P/L:')[-1])
            current_capital += profit_loss
            if current_capital > peak:
                peak = current_capital
            drawdown = peak - current_capital
            if drawdown > max_drawdown:
                max_drawdown = drawdown

    # Evitar división por cero
    if num_wins > 0:
        avg_gain = sum(float(log.split('P/L:')[-1]) for log in trade_log if 'P/L' in log and float(log.split('P/L:')[-1]) > 0) / num_wins
    else:
        avg_gain = 0

    if num_losses > 0:
        avg_loss = sum(float(log.split('P/L:')[-1]) for log in trade_log if 'P/L' in log and float(log.split('P/L:')[-1]) < 0) / num_losses
    else:
        avg_loss = 0

    if avg_loss != 0:
        risk_reward_ratio = avg_gain / abs(avg_loss)
    else:
        risk_reward_ratio = None  # O asigna 0 o un valor específico

    return {
        "win_rate": win_rate,
        "roi": roi,
        "max_drawdown": max_drawdown,
        "risk_reward_ratio": risk_reward_ratio
    }

async def run_backtesting(symbol: str, interval: str = '1h'):
    # Obtener las señales de la base de datos
    signals = await Signal.filter(symbol=symbol, interval=interval).order_by('timestamp').values()

    # Crear el DataFrame directamente desde la lista de diccionarios
    df = pd.DataFrame(signals)

    if df.empty:
        raise ValueError(f"No se encontraron señales para el símbolo {symbol} en el intervalo {interval}")

    # Convertir 'timestamp' a datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Verificar que no haya valores NaT
    df.dropna(subset=['timestamp', 'close', 'signal'], inplace=True)

    # Ordenar por timestamp
    df.sort_values('timestamp', inplace=True)

    # Llamar a la función de backtesting
    initial_capital = 100
    final_capital, trade_log = backtest_momentum_strategy(df, initial_capital)
    metrics = calculate_backtesting_metrics(trade_log, initial_capital, final_capital)

    return {
        "final_capital": final_capital,
        "metrics": metrics,
        "trade_log": trade_log
    }
