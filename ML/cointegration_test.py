# ML/cointegration_test.py

import pandas as pd
import numpy as np
import logging
import sys
import os
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple
from datetime import datetime, timedelta
import warnings
from sklearn.model_selection import ParameterGrid, TimeSeriesSplit
from statsmodels.tsa.stattools import coint, adfuller
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from bayes_opt import BayesianOptimization  # Requiere instalación: pip install bayesian-optimization

warnings.filterwarnings("ignore")  # Ignorar warnings de estadísticas

# Configuración de logging
logger = logging.getLogger('pair_trading')
logger.setLevel(logging.INFO)

# Formato de log
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler para archivo de log
os.makedirs('ML/logs', exist_ok=True)
file_handler = logging.FileHandler('ML/logs/pair_trading.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Handler para consola
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def test_stationarity(series: pd.Series, significance_level: float = 0.05) -> bool:
    """
    Verificar si una serie temporal es estacionaria utilizando la prueba ADF.

    Args:
        series (pd.Series): Serie temporal a probar.
        significance_level (float): Nivel de significancia para la prueba.

    Returns:
        bool: True si la serie es estacionaria, False de lo contrario.
    """
    result = adfuller(series.dropna())
    p_value = result[1]
    return p_value < significance_level

def test_integration_order(series: pd.Series) -> int:
    """
    Determinar el orden de integración de una serie temporal.

    Args:
        series (pd.Series): Serie temporal a probar.

    Returns:
        int: Orden de integración de la serie.
    """
    order = 0
    test_series = series.copy()
    while not test_stationarity(test_series):
        test_series = test_series.diff().dropna()
        order += 1
        if order > 2:
            break
    return order

def test_cointegration_full_series(series1: pd.Series, series2: pd.Series, pvalue_threshold: float = 0.05) -> bool:
    """
    Realizar pruebas de cointegración utilizando Engle-Granger y Johansen.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        pvalue_threshold (float): Umbral de p-value para determinar cointegración.

    Returns:
        bool: True si las series son cointegradas, False de lo contrario.
    """
    # Prueba Engle-Granger
    _, pvalue, _ = coint(series1, series2)
    if pvalue >= pvalue_threshold:
        return False
    
    # Prueba Johansen
    model_data = pd.concat([series1, series2], axis=1).dropna()
    result = coint_johansen(model_data, det_order=0, k_ar_diff=1)
    trace_stat = result.lr1[0]  # Primer estadístico de traza
    critical_value = result.cvt[0, 1]  # Nivel de significancia 5%
    if trace_stat < critical_value:
        return False
    
    return True

def calculate_correlations(combined_df: pd.DataFrame, symbols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcular correlaciones de Pearson y Spearman entre todos los pares de símbolos.

    Args:
        combined_df (pd.DataFrame): DataFrame con precios de cierre de múltiples símbolos.
        symbols (List[str]): Lista de símbolos.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Correlaciones de Pearson y Spearman.
    """
    # Extraer los precios de cierre
    close_prices = combined_df.xs('close', level=1, axis=1)
    
    # Calcular correlaciones de Pearson
    pearson_corr = close_prices.corr(method='pearson')
    
    # Calcular correlaciones de Spearman
    spearman_corr = close_prices.corr(method='spearman')
    
    return pearson_corr, spearman_corr

def calculate_spread(series1: pd.Series, series2: pd.Series, lookback_window: int = 500) -> pd.Series:
    """
    Calcular el spread entre dos series utilizando regresión lineal.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        lookback_window (int): Ventana de lookback para recalcular beta.

    Returns:
        pd.Series: Spread calculado.
    """
    betas = []
    intercepts = []
    for i in range(len(series1)):
        if i < lookback_window:
            betas.append(np.nan)
            intercepts.append(np.nan)
        else:
            s1_window = series1.iloc[i - lookback_window:i]
            s2_window = series2.iloc[i - lookback_window:i]
            try:
                model = np.polyfit(s2_window, s1_window, 1)
                betas.append(model[0])
                intercepts.append(model[1])
            except np.RankWarning:
                betas.append(np.nan)
                intercepts.append(np.nan)
    betas = pd.Series(betas, index=series1.index)
    intercepts = pd.Series(intercepts, index=series1.index)
    spread = series1 - (betas * series2 + intercepts)
    return spread

def calculate_zscore(spread: pd.Series, spread_window: int = 500) -> pd.Series:
    """
    Calcular el Z-score de una serie.

    Args:
        spread (pd.Series): Serie temporal del spread.
        spread_window (int): Ventana para calcular media y desviación estándar.

    Returns:
        pd.Series: Z-score del spread.
    """
    mean = spread.rolling(window=spread_window).mean()
    std = spread.rolling(window=spread_window).std()
    zscore = (spread - mean) / std
    return zscore

def generate_trading_signals(spread: pd.Series, entry_threshold: float = 2.0, exit_threshold: float = 0.5, spread_window: int = 500) -> pd.DataFrame:
    """
    Generar señales de trading basadas en el Z-score del spread y otros indicadores técnicos.

    Args:
        spread (pd.Series): Serie temporal del spread entre dos activos.
        entry_threshold (float): Umbral de entrada para abrir posiciones.
        exit_threshold (float): Umbral de salida para cerrar posiciones.
        spread_window (int): Ventana para calcular Z-score.

    Returns:
        pd.DataFrame: DataFrame con señales de compra y venta.
    """
    zscore = calculate_zscore(spread, spread_window)
    signals = pd.DataFrame(index=spread.index)
    signals['zscore'] = zscore
    signals['long_entry'] = (zscore < -entry_threshold)
    signals['long_exit'] = (zscore > -exit_threshold)
    signals['short_entry'] = (zscore > entry_threshold)
    signals['short_exit'] = (zscore < exit_threshold)
    signals['positions'] = 0

    # Generar posiciones basadas en señales
    position = 0
    for i in range(len(signals)):
        if signals['long_entry'].iloc[i]:
            position = 1
        elif signals['short_entry'].iloc[i]:
            position = -1
        elif signals['long_exit'].iloc[i] and position == 1:
            position = 0
        elif signals['short_exit'].iloc[i] and position == -1:
            position = 0
        signals['positions'].iloc[i] = position

    return signals

def backtest_strategy(series1: pd.Series, series2: pd.Series, signals: pd.DataFrame, initial_capital: float = 10000, trading_fee: float = 0.0004, slippage: float = 0.0005, risk_per_trade: float = 0.01, stop_loss: float = 1.0, take_profit: float = 1.0) -> dict:
    """
    Backtesting de la estrategia de trading con Stop-Loss y Take-Profit.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        signals (pd.DataFrame): DataFrame con señales de trading.
        initial_capital (float): Capital inicial.
        trading_fee (float): Comisión por transacción.
        slippage (float): Slippage estimado.
        risk_per_trade (float): Porcentaje del capital a arriesgar por operación.
        stop_loss (float): Umbral de stop-loss en porcentaje.
        take_profit (float): Umbral de take-profit en porcentaje.

    Returns:
        dict: Métricas de rendimiento.
    """
    capital = initial_capital
    portfolio_values = [capital]
    trade_log = []
    positions = []  # Inicializar lista de posiciones

    position = 0  # 1: Long, -1: Short, 0: Neutral
    entry_price1 = 0
    entry_price2 = 0

    for i in range(1, len(signals)):
        date = signals.index[i]
        price1 = series1[date]
        price2 = series2[date]
        prev_position = position
        position = signals['positions'].iloc[i]

        fee = trading_fee
        slip = slippage

        if position != prev_position:
            if position == 1:
                # Abrir posición larga
                entry_price1 = price1 * (1 + slip)
                entry_price2 = price2 * (1 - slip)
                trade_amount = calculate_position_size(capital, series1.rolling(window=14).std().iloc[i], risk_per_trade)
                capital -= trade_amount * fee * 2
                trade_log.append({'date': date, 'type': 'Long Entry', 'price1': entry_price1, 'price2': entry_price2})
                logger.debug(f"Long Entry on {date} | Price1: {entry_price1}, Price2: {entry_price2}")
            elif position == -1:
                # Abrir posición corta
                entry_price1 = price1 * (1 - slip)
                entry_price2 = price2 * (1 + slip)
                trade_amount = calculate_position_size(capital, series1.rolling(window=14).std().iloc[i], risk_per_trade)
                capital -= trade_amount * fee * 2
                trade_log.append({'date': date, 'type': 'Short Entry', 'price1': entry_price1, 'price2': entry_price2})
                logger.debug(f"Short Entry on {date} | Price1: {entry_price1}, Price2: {entry_price2}")
            elif position == 0:
                # Cerrar posición
                if prev_position == 1:
                    exit_price1 = price1 * (1 - slip)
                    exit_price2 = price2 * (1 + slip)
                elif prev_position == -1:
                    exit_price1 = price1 * (1 + slip)
                    exit_price2 = price2 * (1 - slip)
                else:
                    continue

                # Calcular ganancias/pérdidas
                profit1 = (exit_price1 - entry_price1) * (-1 if prev_position == -1 else 1)
                profit2 = (entry_price2 - exit_price2) * (-1 if prev_position == -1 else 1)
                total_profit = profit1 + profit2

                # Aplicar Stop-Loss y Take-Profit
                if abs(total_profit) < 0.0:  # Placeholder para lógica de SL/TP
                    pass  # Implementar lógica si es necesario

                capital += total_profit
                capital -= (capital * fee * 2)  # Comisiones al cerrar
                portfolio_values.append(capital)
                trade_log.append({'date': date, 'type': 'Exit', 'price1': exit_price1, 'price2': exit_price2, 'profit': total_profit})
                logger.debug(f"Exit on {date} | Profit: {total_profit} | Capital: {capital}")
                positions.append(total_profit)
        else:
            portfolio_values.append(capital)

    # Calcular métricas
    total_return = capital - initial_capital
    returns_series = calculate_daily_returns(portfolio_values)
    sharpe_ratio = (returns_series.mean() / returns_series.std()) * np.sqrt(252) if returns_series.std() != 0 else np.nan
    max_drawdown = calculate_max_drawdown(portfolio_values)

    metrics = {
        'Initial Capital': initial_capital,
        'Final Capital': capital,
        'Total Return': total_return,
        'Sharpe Ratio': sharpe_ratio,
        'Max Drawdown': max_drawdown,
        'Total Trades': len(positions)
    }

    # Guardar el registro de operaciones
    trade_log_df = pd.DataFrame(trade_log)
    os.makedirs('ML/results', exist_ok=True)
    trade_log_df.to_csv('ML/results/trade_log.csv', index=False)

    return metrics

def calculate_daily_returns(portfolio_values: List[float]) -> pd.Series:
    """
    Calcular los retornos diarios del portafolio.
    """
    portfolio_series = pd.Series(portfolio_values)
    returns = portfolio_series.pct_change().dropna()
    return returns

def calculate_max_drawdown(portfolio_values: List[float]) -> float:
    """
    Calcular el máximo drawdown del portafolio.
    """
    cumulative = np.maximum.accumulate(portfolio_values)
    drawdowns = (cumulative - portfolio_values) / cumulative
    return np.max(drawdowns)

def calculate_position_size(capital: float, atr: float, risk_per_trade: float = 0.01) -> float:
    """
    Calcular el tamaño de la posición basado en el ATR y el riesgo por operación.

    Args:
        capital (float): Capital actual.
        atr (float): Average True Range o medida de volatilidad.
        risk_per_trade (float): Porcentaje del capital a arriesgar por operación.

    Returns:
        float: Tamaño de la posición.
    """
    if atr == 0:
        logger.warning("ATR es cero, evitando división por cero en el tamaño de la posición.")
        return 0
    return (capital * risk_per_trade) / atr

def plot_trading_signals(spread: pd.Series, signals: pd.DataFrame, pair: str):
    """
    Generar gráfica de señales de trading.

    Args:
        spread (pd.Series): Serie temporal del spread.
        signals (pd.DataFrame): DataFrame con señales de trading.
        pair (str): Nombre del par.
    """
    plt.figure(figsize=(14, 7))
    plt.plot(spread.index, spread.values, label='Spread')
    plt.plot(signals.loc[signals['long_entry']].index, spread.loc[signals['long_entry']], '^', markersize=10, color='g', label='Long Entry')
    plt.plot(signals.loc[signals['short_entry']].index, spread.loc[signals['short_entry']], 'v', markersize=10, color='r', label='Short Entry')
    plt.plot(signals.loc[signals['long_exit']].index, spread.loc[signals['long_exit']], 'o', markersize=5, color='b', label='Long Exit')
    plt.plot(signals.loc[signals['short_exit']].index, spread.loc[signals['short_exit']], 'o', markersize=5, color='orange', label='Short Exit')
    plt.title(f'Señales de Trading para el Par {pair}')
    plt.xlabel('Fecha')
    plt.ylabel('Spread')
    plt.legend()
    os.makedirs('ML/results', exist_ok=True)
    plt.savefig(f'ML/results/trading_signals_{pair}.png')
    plt.close()
    logger.info(f"Gráfica de señales de trading guardada para el par {pair}.")

def calculate_optimal_parameters(series1: pd.Series, series2: pd.Series, spread_window: int, param_grid: dict, cv_splits: int = 5) -> dict:
    """
    Optimizar los parámetros de la estrategia utilizando Grid Search con Validación Cruzada.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        spread_window (int): Ventana de spread.
        param_grid (dict): Diccionario con los parámetros a optimizar.
        cv_splits (int): Número de splits para la validación cruzada.

    Returns:
        dict: Mejor conjunto de parámetros encontrados.
    """
    best_sharpe = -np.inf
    best_params = None

    grid = ParameterGrid(param_grid)
    tscv = TimeSeriesSplit(n_splits=cv_splits)

    for params in grid:
        sharpe_scores = []
        for train_index, test_index in tscv.split(series1):
            train_series1, test_series1 = series1.iloc[train_index], series1.iloc[test_index]
            train_series2, test_series2 = series2.iloc[train_index], series2.iloc[test_index]

            # Calcular spread
            spread_train = calculate_spread(train_series1, train_series2, lookback_window=spread_window)
            spread_test = calculate_spread(test_series1, test_series2, lookback_window=spread_window)

            # Generar señales
            signals_test = generate_trading_signals(spread_test, entry_threshold=params['entry_threshold'], 
                                                   exit_threshold=params['exit_threshold'], spread_window=spread_window)

            # Backtesting en el conjunto de prueba
            metrics = backtest_strategy(test_series1, test_series2, signals_test, initial_capital=10000, 
                                        trading_fee=params.get('trading_fee', 0.0004), 
                                        slippage=params.get('slippage', 0.0005), 
                                        risk_per_trade=params.get('risk_per_trade', 0.01))
            sharpe_scores.append(metrics['Sharpe Ratio'])

        avg_sharpe = np.nanmean(sharpe_scores)
        if avg_sharpe > best_sharpe:
            best_sharpe = avg_sharpe
            best_params = params

    logger.info(f"Mejores parámetros encontrados: {best_params} con Sharpe Ratio: {best_sharpe}")
    return best_params

def optimize_parameters(series1: pd.Series, series2: pd.Series, spread_window: int, initial_capital: float = 10000):
    """
    Optimizar los parámetros de la estrategia utilizando Optimización Bayesiana.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        spread_window (int): Ventana de spread.
        initial_capital (float): Capital inicial para el backtest.

    Returns:
        dict: Mejor conjunto de parámetros encontrados.
    """
    spread = calculate_spread(series1, series2, lookback_window=spread_window)
    zscore = calculate_zscore(spread, spread_window)

    def sharpe_score(entry_threshold, exit_threshold, spread_window, trading_fee, slippage, risk_per_trade):
        signals = generate_trading_signals(spread, entry_threshold=entry_threshold, 
                                           exit_threshold=exit_threshold, spread_window=int(spread_window))
        metrics = backtest_strategy(series1, series2, signals, initial_capital=initial_capital, 
                                    trading_fee=trading_fee, 
                                    slippage=slippage, 
                                    risk_per_trade=risk_per_trade)
        return metrics['Sharpe Ratio']

    pbounds = {
        'entry_threshold': (1.0, 3.0),
        'exit_threshold': (0.3, 1.0),
        'spread_window': (300, 700),
        'trading_fee': (0.0002, 0.001),
        'slippage': (0.0003, 0.001),
        'risk_per_trade': (0.005, 0.02)
    }

    optimizer = BayesianOptimization(
        f=sharpe_score,
        pbounds=pbounds,
        random_state=1,
    )

    optimizer.maximize(
        init_points=10,
        n_iter=30,
    )

    best_params = optimizer.max['params']
    logger.info(f"Mejores parámetros encontrados con optimización bayesiana: {best_params}")
    return best_params

def plot_spread(spread: pd.Series, pair: Tuple[str, str]):
    """
    Graficar el spread de un par de activos.

    Args:
        spread (pd.Series): Serie temporal del spread.
        pair (Tuple[str, str]): Pares de activos.
    """
    plt.figure(figsize=(14, 7))
    plt.plot(spread.index, spread, label='Spread')
    plt.title(f'Spread para el Par {pair[0]}-{pair[1]}')
    plt.xlabel('Fecha')
    plt.ylabel('Spread')
    plt.legend()
    os.makedirs('ML/results', exist_ok=True)
    plt.savefig(f'ML/results/spread_{pair[0]}_{pair[1]}.png')
    plt.close()
    logger.info(f"Gráfica del spread guardada para el par {pair[0]}-{pair[1]}.")

def plot_spread_with_signals(spread: pd.Series, signals: pd.DataFrame, pair: Tuple[str, str]):
    """
    Graficar el spread junto con las señales de trading.

    Args:
        spread (pd.Series): Serie temporal del spread.
        signals (pd.DataFrame): DataFrame con señales de trading.
        pair (Tuple[str, str]): Pares de activos.
    """
    plt.figure(figsize=(14, 7))
    plt.plot(spread.index, spread, label='Spread')
    plt.plot(signals.loc[signals['long_entry']].index, spread.loc[signals['long_entry']], '^', markersize=10, color='g', label='Long Entry')
    plt.plot(signals.loc[signals['short_entry']].index, spread.loc[signals['short_entry']], 'v', markersize=10, color='r', label='Short Entry')
    plt.plot(signals.loc[signals['long_exit']].index, spread.loc[signals['long_exit']], 'o', markersize=5, color='b', label='Long Exit')
    plt.plot(signals.loc[signals['short_exit']].index, spread.loc[signals['short_exit']], 'o', markersize=5, color='orange', label='Short Exit')
    plt.title(f'Spread y Señales de Trading para el Par {pair[0]}-{pair[1]}')
    plt.xlabel('Fecha')
    plt.ylabel('Spread')
    plt.legend()
    os.makedirs('ML/results', exist_ok=True)
    plt.savefig(f'ML/results/spread_signals_{pair[0]}_{pair[1]}.png')
    plt.close()
    logger.info(f"Gráfica del spread con señales guardada para el par {pair[0]}-{pair[1]}.")

def plot_relative_trading_signals(relative_price: pd.Series, signals: pd.DataFrame, pair: Tuple[str, str]):
    """
    Generar gráfica de señales de trading basadas en la relación de precios relativa.

    Args:
        relative_price (pd.Series): Serie temporal de la relación de precios.
        signals (pd.DataFrame): DataFrame con señales de trading.
        pair (Tuple[str, str]): Nombre del par.
    """
    plt.figure(figsize=(14, 7))
    plt.plot(relative_price.index, relative_price, label='Relación de Precios (BTC/ETH)')
    plt.scatter(signals.loc[signals['long_entry']].index, relative_price.loc[signals['long_entry']], marker='^', color='g', label='Long Entry')
    plt.scatter(signals.loc[signals['short_entry']].index, relative_price.loc[signals['short_entry']], marker='v', color='r', label='Short Entry')
    plt.scatter(signals.loc[signals['long_exit']].index, relative_price.loc[signals['long_exit']], marker='o', color='b', label='Long Exit')
    plt.scatter(signals.loc[signals['short_exit']].index, relative_price.loc[signals['short_exit']], marker='o', color='orange', label='Short Exit')
    plt.title(f'Señales de Trading Relativas para el Par {pair[0]}-{pair[1]}')
    plt.xlabel('Fecha')
    plt.ylabel('Relación de Precios (BTC/ETH)')
    plt.legend()
    os.makedirs('ML/results', exist_ok=True)
    plt.savefig(f'ML/results/relative_trading_signals_{pair[0]}_{pair[1]}.png')
    plt.close()
    logger.info(f"Gráfica de señales de trading relativa guardada para el par {pair[0]}-{pair[1]}.")

def generate_simple_trading_signals(spread: pd.Series, zscore_threshold: float = 2.0) -> pd.DataFrame:
    """
    Generar señales de trading básicas basadas en el Z-score del spread.

    Args:
        spread (pd.Series): Serie temporal del spread.
        zscore_threshold (float): Umbral de Z-score para generar señales.

    Returns:
        pd.DataFrame: DataFrame con señales de trading.
    """
    zscore = calculate_zscore(spread)
    signals = pd.DataFrame(index=spread.index)
    signals['zscore'] = zscore
    signals['long_entry'] = zscore < -zscore_threshold
    signals['long_exit'] = zscore > 0
    signals['short_entry'] = zscore > zscore_threshold
    signals['short_exit'] = zscore < 0
    signals['positions'] = 0

    position = 0
    for i in range(len(signals)):
        if signals['long_entry'].iloc[i]:
            position = 1
        elif signals['short_entry'].iloc[i]:
            position = -1
        elif signals['long_exit'].iloc[i] and position == 1:
            position = 0
        elif signals['short_exit'].iloc[i] and position == -1:
            position = 0
        signals['positions'].iloc[i] = position

    return signals

def manual_cointegration_check(series1: pd.Series, series2: pd.Series, pair: str):
    """
    Realizar una verificación manual de la cointegración y graficar el spread.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.
        pair (str): Nombre del par.
    """
    spread = calculate_spread(series1, series2)
    zscore = calculate_zscore(spread)

    plt.figure(figsize=(14, 7))
    plt.plot(spread.index, spread, label='Spread')
    plt.plot(zscore.index, zscore, label='Z-score')
    plt.axhline(2, color='r', linestyle='--')
    plt.axhline(-2, color='g', linestyle='--')
    plt.title(f'Cointegración y Z-score para {pair}')
    plt.legend()
    os.makedirs('ML/results', exist_ok=True)
    plt.savefig(f'ML/results/manual_cointegration_check_{pair}.png')
    plt.close()
    logger.info(f"Cointegración manual y Z-score graficados para el par {pair}.")

def main():
    # Ruta del archivo de datos combinados
    data_path = 'ML/data/combined_data.csv'

    # Verificar si el archivo existe
    if not os.path.exists(data_path):
        logger.error(f"Archivo de datos combinados no encontrado en {data_path}. Ejecuta 'data_coin.py' primero.")
        return

    # Cargar datos combinados
    try:
        combined_df = pd.read_csv(data_path, header=[0, 1], index_col=0, parse_dates=True)
        logger.info(f"Datos combinados cargados correctamente desde {data_path}.")
    except Exception as e:
        logger.error(f"Error al cargar datos combinados: {e}")
        return

    # Asegurar que los datos están ordenados por fecha
    combined_df.sort_index(inplace=True)

    # Asegurar que los tipos de datos son numéricos
    combined_df = combined_df.apply(pd.to_numeric, errors='coerce')
    initial_len = len(combined_df)
    combined_df.dropna(inplace=True)
    final_len = len(combined_df)
    if final_len < initial_len:
        logger.warning(f"Se eliminaron {initial_len - final_len} filas con valores faltantes tras convertir a numérico.")

    # Verificar y sincronizar fechas
    try:
        combined_df = combined_df.resample('15T').mean().interpolate()
        logger.info("Datos resampleados a 15 minutos y interpolados.")
    except Exception as e:
        logger.error(f"Error al resamplear e interpolar datos: {e}")
        return

    # Extraer lista de símbolos
    symbols = combined_df.columns.get_level_values(0).unique().tolist()
    logger.info(f"Símbolos encontrados en los datos: {symbols}")

    # Calcular correlaciones de Pearson y Spearman
    pearson_corr, spearman_corr = calculate_correlations(combined_df, symbols)
    logger.info("Correlaciones de Pearson y Spearman calculadas.")

    # Visualizar correlaciones con un mapa de calor
    try:
        plt.figure(figsize=(12, 8))
        plt.title('Correlación de Pearson entre activos')
        sns.heatmap(pearson_corr, annot=True, cmap='coolwarm')
        os.makedirs('ML/results', exist_ok=True)
        plt.savefig('ML/results/pearson_correlation_heatmap.png')
        plt.close()
        logger.info("Mapa de calor de correlación de Pearson guardado.")
    except Exception as e:
        logger.error(f"Error al generar mapa de calor de Pearson: {e}")

    try:
        plt.figure(figsize=(12, 8))
        plt.title('Correlación de Spearman entre activos')
        sns.heatmap(spearman_corr, annot=True, cmap='coolwarm')
        plt.savefig('ML/results/spearman_correlation_heatmap.png')
        plt.close()
        logger.info("Mapa de calor de correlación de Spearman guardado.")
    except Exception as e:
        logger.error(f"Error al generar mapa de calor de Spearman: {e}")

    # Seleccionar pares con alta correlación
    highly_correlated_pairs = []
    threshold = 0.8
    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            s1 = symbols[i]
            s2 = symbols[j]
            corr = pearson_corr.loc[s1, s2]
            if abs(corr) >= threshold:
                highly_correlated_pairs.append((s1, s2))

    logger.info(f"Pares con correlación superior a {threshold}: {highly_correlated_pairs}")

    if not highly_correlated_pairs:
        logger.warning("No se encontraron pares altamente correlacionados.")
        return

    # Pruebas de Cointegración sin ventanas móviles
    cointegrated_pairs = []
    pvalue_threshold = 0.05  # Ajustado de 0.1 a 0.05

    for pair in highly_correlated_pairs:
        s1, s2 = pair
        series1 = combined_df[(s1, 'close')]
        series2 = combined_df[(s2, 'close')]

        # Asegurarse de que las series no tienen NaN
        combined_series = pd.concat([series1, series2], axis=1).dropna()
        if len(combined_series) < 1000:
            logger.info(f"Pares: {s1}-{s2} | Datos insuficientes para pruebas de cointegración.")
            continue

        series1 = combined_series.iloc[:, 0]
        series2 = combined_series.iloc[:, 1]

        # Verificar estacionariedad y orden de integración
        order1 = test_integration_order(series1)
        order2 = test_integration_order(series2)
        if order1 != 1 or order2 != 1:
            logger.info(f"Pares: {s1}-{s2} | Las series no son I(1), se omite el par.")
            continue

        # Prueba de cointegración con múltiples pruebas
        try:
            if test_cointegration_full_series(series1, series2, pvalue_threshold):
                cointegrated_pairs.append((s1, s2))
                logger.info(f"--> Los pares {s1}-{s2} son cointegrados.")
            else:
                logger.info(f"Pares: {s1}-{s2} | No son cointegrados.")
        except Exception as e:
            logger.error(f"Error al realizar pruebas de cointegración para {s1}-{s2}: {e}")

    if not cointegrated_pairs:
        logger.warning("No se encontraron pares cointegrados con los umbrales especificados.")
        return

    logger.info(f"Pares cointegrados seleccionados: {cointegrated_pairs}")

    # Implementar estrategia para los pares cointegrados
    for pair in cointegrated_pairs:
        s1, s2 = pair
        logger.info(f"Implementando estrategia para el par {s1}-{s2}")
        series1 = combined_df[(s1, 'close')]
        series2 = combined_df[(s2, 'close')]

        # Asegurarse de que las series no tienen NaN
        combined_series = pd.concat([series1, series2], axis=1).dropna()
        if len(combined_series) < 1000:
            logger.info(f"Pares: {s1}-{s2} | Datos insuficientes para backtesting.")
            continue

        series1 = combined_series.iloc[:, 0]
        series2 = combined_series.iloc[:, 1]

        # Calcular spread
        spread = calculate_spread(series1, series2, lookback_window=500)
        plot_spread(spread, pair)

        # Generar señales de trading con umbrales ajustados
        signals = generate_trading_signals(spread, entry_threshold=1.5, exit_threshold=0.5, spread_window=500)
        plot_spread_with_signals(spread, signals, pair)

        # Verificar que se generaron señales
        if signals.empty:
            logger.warning(f"No se generaron señales de trading para el par {s1}-{s2}.")
            continue

        # Backtesting de la estrategia
        try:
            metrics = backtest_strategy(series1, series2, signals)
            logger.info(f"Rendimiento de la estrategia para pares {s1}-{s2}: {metrics}")
        except Exception as e:
            logger.error(f"Error en backtesting para el par {s1}-{s2}: {e}")
            continue

        # Guardar métricas de rendimiento
        try:
            with open('ML/results/performance_metrics.txt', 'a') as f:
                f.write(f"Pares: {s1}-{s2}\n")
                for key, value in metrics.items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
            logger.info(f"Métricas de rendimiento guardadas para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al guardar métricas para el par {s1}-{s2}: {e}")

        # Visualizar las señales de trading
        try:
            plot_trading_signals(spread, signals, f"{s1}-{s2}")
            logger.info(f"Gráfica de señales de trading guardada para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al generar gráfica para el par {s1}-{s2}: {e}")

    # Optimización de Parámetros (Opcional)
    param_grid = {
        'entry_threshold': [1.5, 2.0, 2.5],
        'exit_threshold': [0.5, 1.0],
        'spread_window': [300, 500, 700],
        'trading_fee': [0.0004, 0.0006],
        'slippage': [0.0005, 0.001],
        'risk_per_trade': [0.005, 0.01]
    }

    for pair in cointegrated_pairs:
        s1, s2 = pair
        logger.info(f"Optimización de parámetros para el par {s1}-{s2}")
        series1 = combined_df[(s1, 'close')]
        series2 = combined_df[(s2, 'close')]
        combined_series = pd.concat([series1, series2], axis=1).dropna()
        series1 = combined_series.iloc[:, 0]
        series2 = combined_series.iloc[:, 1]

        # Calcular spread
        spread = calculate_spread(series1, series2, lookback_window=500)
        plot_spread(spread, pair)

        # Optimizar parámetros
        try:
            best_params = calculate_optimal_parameters(series1, series2, spread_window=500, param_grid=param_grid, cv_splits=3)
            logger.info(f"Mejores parámetros encontrados para el par {s1}-{s2}: {best_params}")
        except Exception as e:
            logger.error(f"Error durante la optimización de parámetros para el par {s1}-{s2}: {e}")
            continue

        # Generar señales con los mejores parámetros
        try:
            signals = generate_trading_signals(spread, entry_threshold=best_params['entry_threshold'], 
                                               exit_threshold=best_params['exit_threshold'], spread_window=int(best_params['spread_window']))
            plot_spread_with_signals(spread, signals, pair)
            logger.info(f"Señales generadas con los mejores parámetros para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al generar señales con parámetros optimizados para el par {s1}-{s2}: {e}")
            continue

        # Backtesting con los mejores parámetros
        try:
            metrics = backtest_strategy(series1, series2, signals, 
                                        trading_fee=best_params.get('trading_fee', 0.0004), 
                                        slippage=best_params.get('slippage', 0.0005), 
                                        risk_per_trade=best_params.get('risk_per_trade', 0.01))
            logger.info(f"Optimización - Rendimiento de la estrategia para pares {s1}-{s2}: {metrics}")
        except Exception as e:
            logger.error(f"Error en backtesting optimizado para el par {s1}-{s2}: {e}")
            continue

        # Guardar métricas de rendimiento optimizadas
        try:
            with open('ML/results/performance_metrics_optimized.txt', 'a') as f:
                f.write(f"Pares: {s1}-{s2}\n")
                for key, value in metrics.items():
                    f.write(f"{key}: {value}\n")
                f.write(f"Mejores Parámetros: {best_params}\n\n")
            logger.info(f"Métricas de rendimiento optimizadas guardadas para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al guardar métricas optimizadas para el par {s1}-{s2}: {e}")

    # Implementar estrategia relativa para BTC-ETH si no hay resultados positivos
    relative_pair = ('BTCUSDT', 'ETHUSDT')
    logger.info(f"Implementando estrategia relativa para el par {relative_pair}")

    s1, s2 = relative_pair
    series1 = combined_df[(s1, 'close')]
    series2 = combined_df[(s2, 'close')]

    # Calcular relación de precios relativa
    relative_price = calculate_relative_price(series1, series2)

    # Generar señales de trading relativa con umbrales ajustados
    relative_signals = generate_relative_trading_signals(relative_price, entry_threshold=1.5, exit_threshold=1.0)
    plot_relative_trading_signals(relative_price, relative_signals, relative_pair)

    # Verificar que se generaron señales
    if relative_signals.empty:
        logger.warning(f"No se generaron señales de trading para el par {s1}-{s2}.")
    else:
        # Backtesting de la estrategia relativa
        try:
            relative_metrics = backtest_strategy(series1, series2, relative_signals)
            logger.info(f"Rendimiento de la estrategia relativa para pares {s1}-{s2}: {relative_metrics}")
        except Exception as e:
            logger.error(f"Error en backtesting de estrategia relativa para el par {s1}-{s2}: {e}")

        # Guardar métricas de rendimiento relativa
        try:
            with open('ML/results/performance_metrics_relative.txt', 'a') as f:
                f.write(f"Pares: {s1}-{s2}\n")
                for key, value in relative_metrics.items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
            logger.info(f"Métricas de rendimiento guardadas para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al guardar métricas para el par {s1}-{s2}: {e}")

        # Visualizar las señales de trading relativa
        try:
            plot_relative_trading_signals(relative_price, relative_signals, relative_pair)
            logger.info(f"Gráfica de señales de trading relativa guardada para el par {s1}-{s2}.")
        except Exception as e:
            logger.error(f"Error al generar gráfica para el par {s1}-{s2}: {e}")

def calculate_relative_price(series1: pd.Series, series2: pd.Series) -> pd.Series:
    """
    Calcular la relación de precios relativa entre dos activos.

    Args:
        series1 (pd.Series): Serie temporal del primer activo.
        series2 (pd.Series): Serie temporal del segundo activo.

    Returns:
        pd.Series: Relación de precios relativa.
    """
    return series1 / series2

def generate_relative_trading_signals(relative_price: pd.Series, entry_threshold: float = 1.5, exit_threshold: float = 1.0) -> pd.DataFrame:
    """
    Generar señales de trading basadas en la relación de precios relativa.

    Args:
        relative_price (pd.Series): Serie temporal de la relación de precios.
        entry_threshold (float): Umbral de entrada para abrir posiciones.
        exit_threshold (float): Umbral de salida para cerrar posiciones.

    Returns:
        pd.DataFrame: DataFrame con señales de trading.
    """
    signals = pd.DataFrame(index=relative_price.index)
    signals['relative_price'] = relative_price
    signals['long_entry'] = relative_price < (1 / entry_threshold)
    signals['long_exit'] = relative_price > (1 / exit_threshold)
    signals['short_entry'] = relative_price > entry_threshold
    signals['short_exit'] = relative_price < exit_threshold
    signals['positions'] = 0

    position = 0
    for i in range(len(signals)):
        if signals['long_entry'].iloc[i]:
            position = 1
        elif signals['short_entry'].iloc[i]:
            position = -1
        elif signals['long_exit'].iloc[i] and position == 1:
            position = 0
        elif signals['short_exit'].iloc[i] and position == -1:
            position = 0
        signals['positions'].iloc[i] = position

    return signals

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.critical(f"Error crítico en la ejecución del script: {e}")
