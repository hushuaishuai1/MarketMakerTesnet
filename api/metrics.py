def calculate_additional_metrics(cerebro):
    # Retorno anualizado
    total_return = cerebro.broker.getvalue() - cerebro.broker.starting_cash
    annualized_return = (1 + total_return)**(252 / cerebro.data.num_bars()) - 1  # 252 días de trading en un año

    # Tasa de aciertos (Win Rate)
    winning_trades = sum(1 for trade in cerebro.trades if trade.pnl > 0)
    total_trades = len(cerebro.trades)
    win_rate = winning_trades / total_trades if total_trades > 0 else 0

    # Factor de recuperación
    max_drawdown = max([abs(trade.drawdown) for trade in cerebro.trades], default=0)
    recovery_factor = total_return / max_drawdown if max_drawdown != 0 else 0

    return {
        "annualized_return": annualized_return,
        "win_rate": win_rate,
        "recovery_factor": recovery_factor
    }
