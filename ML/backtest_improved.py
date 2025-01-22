import os
import sys
import logging
import numpy as np
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt

from datetime import datetime
from sklearn.impute import SimpleImputer
import joblib

# ====================== CONFIGURACIÓN GENERAL ======================
CSV_PATH = "ML/data/processed/BTCUSDT_15m_processed.csv"
MODEL_FILE = "./results/XGBoost_Binario_trained_pipeline.joblib"
RESULTS_PATH = "./backtest_results_improved"
os.makedirs(RESULTS_PATH, exist_ok=True)

LOG_FILE = os.path.join(RESULTS_PATH, "backtest_improved_log.log")

# Parámetros de estrategia y riesgo
ATR_PERIOD = 14
ATR_STOP_MULT = 2.5
ATR_TP_MULT = 3.5
# Disminuimos el riesgo por trade a 1%:
RISK_PER_TRADE = 0.015    # 1% del capital
# Elevamos el umbral para filtrar señales de baja calidad:
THRESHOLD_HIGH = 0.7      # En vez de 0.6
INITIAL_CAPITAL = 10000.0
COMMISSION_RATE = 0.001   # 0.1% en cada operación

# Parámetros extra para mejoras:
MAX_DRAWDOWN_LIMIT = 0.3         # 30% de DD máximo; si se excede, no se opera más
MAX_CONSECUTIVE_LOSSES = 5       # Si hay 5 pérdidas seguidas, paramos temporalmente
PARTIAL_EXIT_RATIO = 0.3        # Salir de un 50% de la posición en la toma de ganancia parcial
PARTIAL_EXIT_TRIGGER = 1.0       # Activar la salida parcial cuando el trade vaya +1.0 ATR a favor
TRAILING_STOP_BUFFER = 1.0       # Ajustar el SL a (precio actual - 1.0 * ATR) tras la salida parcial

# Columnas que se usaron en el entrenamiento
FIXED_FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'RSI', 'MACD', 'MACDs', 'MACDh',
    'SMA_10', 'SMA_50', 'EMA_10', 'EMA_50',
    'OBV', 'BBL', 'BBM', 'BBU',
    'STOCHk', 'STOCHd', 'CCI', 'SMA_200'
]

# ====================== CONFIGURACIÓN DE LOGGING ======================
logger = logging.getLogger("backtest_improved_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ====================== FUNCIONES DE PREPROCESAMIENTO ======================
def load_data(csv_path: str) -> pd.DataFrame:
    """Carga el CSV, valida columnas y configura el índice como DatetimeIndex."""
    required_columns = {"open_time", "open", "high", "low", "close", "volume"}
    if not os.path.exists(csv_path):
        logger.error(f"No se encontró el archivo: {csv_path}")
        sys.exit(1)
    df = pd.read_csv(csv_path, parse_dates=["open_time"])
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        logger.error(f"Faltan columnas en el CSV: {missing_columns}")
        sys.exit(1)
        
    df.drop_duplicates(subset=["open_time"], inplace=True)
    df.sort_values(by="open_time", inplace=True)
    df.set_index("open_time", inplace=True)
    
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df.sort_index(inplace=True)
    
    logger.info(f"Datos cargados desde {csv_path} (filas: {len(df)})")
    return df

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula indicadores técnicos y ATR, y organiza el DataFrame según FIXED_FEATURES."""
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.error("El índice debe ser DatetimeIndex para calcular indicadores.")
        sys.exit(1)
    df = df.copy()
    df.sort_index(inplace=True)

    # Eliminar indicadores previos si existen
    indicators_to_drop = [
        'RSI', 'MACD', 'MACDs', 'MACDh', 'OBV', 'BBL', 'BBM', 'BBU',
        'STOCHk', 'STOCHd', 'CCI', 'SMA_10', 'SMA_50', 'SMA_200',
        'EMA_10', 'EMA_50', 'ATR'
    ]
    for col in indicators_to_drop:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Calcular indicadores técnicos usando pandas_ta
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.sma(length=10, append=True)
    df.ta.sma(length=50, append=True)
    df.ta.sma(length=200, append=True)
    df.ta.ema(length=10, append=True)
    df.ta.ema(length=50, append=True)
    df.ta.obv(append=True)
    df.ta.bbands(length=20, std=2, append=True)
    df.ta.stoch(append=True)
    df.ta.cci(length=14, append=True)
    
    # Convertir a numérico y eliminar filas sin datos esenciales
    df = df.apply(pd.to_numeric, errors='coerce')
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    
    # Calcular ATR y llenar valores faltantes
    df["ATR"] = ta.atr(high=df["high"], low=df["low"], close=df["close"], length=ATR_PERIOD)
    df["ATR"] = df["ATR"].ffill().bfill()
    if df["ATR"].isna().sum() > 0:
        logger.error("Existen valores nulos en la columna ATR después del fill.")
        sys.exit(1)

    # Renombrar columnas para que coincidan con FIXED_FEATURES
    rename_dict = {
        "RSI_14": "RSI",
        "MACD_12_26_9": "MACD",
        "MACDs_12_26_9": "MACDs",
        "MACDh_12_26_9": "MACDh",
        "BBL_20_2.0": "BBL",
        "BBM_20_2.0": "BBM",
        "BBU_20_2.0": "BBU",
        "STOCHk_14_3_3": "STOCHk",
        "STOCHd_14_3_3": "STOCHd",
        "CCI_14_0.015": "CCI",
        "SMA_10": "SMA_10",
        "SMA_50": "SMA_50",
        "SMA_200": "SMA_200",
        "EMA_10": "EMA_10",
        "EMA_50": "EMA_50"
    }
    df.rename(columns=rename_dict, inplace=True)

    # Mantener solo las columnas esenciales (más ATR)
    final_columns = list(set(FIXED_FEATURES + ["open", "high", "low", "close", "volume", "ATR"]))
    extra_cols = [col for col in df.columns if col not in final_columns]
    if extra_cols:
        df.drop(columns=extra_cols, inplace=True)
    return df

def prepare_dataset(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """Prepara el DataFrame para predicción, dejando solo las columnas requeridas y aplicando imputación."""
    df = df.copy()
    columns_to_use = [c for c in feature_cols if c in df.columns]
    df = df[columns_to_use]
    imputer = SimpleImputer(strategy='mean')
    X_array = imputer.fit_transform(df)
    X = pd.DataFrame(X_array, columns=df.columns, index=df.index)
    return X

# ====================== GENERAR SEÑALES ======================
def generate_signals(model, X, threshold=THRESHOLD_HIGH):
    """
    Genera señales de compra (1) basándose en la probabilidad de la clase 1, 
    usando el threshold establecido.
    """
    probabilities = model.predict_proba(X)[:, 1]
    signals = np.where(probabilities > threshold, 1, 0)
    return signals

# ====================== BACKTEST CON MANEJO INTRABAR (MEJORADO) ======================
def backtest_trades_risk_intrabar(df: pd.DataFrame,
                                  signals: np.ndarray,
                                  atr_stop_mult: float,
                                  atr_tp_mult: float,
                                  initial_capital: float,
                                  risk_per_trade: float,
                                  commission_rate: float,
                                  max_dd_limit: float = 1.0,
                                  max_consecutive_losses: int = 9999) -> (pd.DataFrame, pd.DataFrame):
    """
    Ejecuta un backtest 'long only' con mejoras:
      - Se abre el trade al cierre de la vela donde aparece la señal.
      - En la siguiente vela se revisa si se alcanza SL, TP parcial, trailing stop, etc.
      - Si se supera el drawdown global 'max_dd_limit' o la racha de pérdidas 'max_consecutive_losses', 
        se deja de abrir nuevas posiciones.
    """
    df_bt = df.copy()
    df_bt["signal"] = signals
    df_bt["capital"] = np.nan

    capital = initial_capital
    in_position = False
    entry_price = 0.0
    position_size = 0.0
    consecutive_losses = 0

    # Lista donde se almacenarán los trades
    trades = []

    idx_list = df_bt.index.to_list()

    # Para trackear drawdown en vivo
    peak_capital = initial_capital

    for i in range(len(idx_list) - 1):
        idx_current = idx_list[i]
        idx_next = idx_list[i + 1]

        current_close = df_bt.at[idx_current, "close"]
        signal = df_bt.at[idx_current, "signal"]
        df_bt.at[idx_current, "capital"] = capital
        
        # Actualizar máximo de capital y drawdown
        if capital > peak_capital:
            peak_capital = capital
        current_dd = 1.0 - (capital / peak_capital)  # drawdown actual
        # Si la cuenta supera el DD límite, no abrimos más posiciones
        global_stop_active = (current_dd >= max_dd_limit)

        # Chequear racha de pérdidas
        if consecutive_losses >= max_consecutive_losses:
            # No abrir más operaciones hasta que reiniciemos manual o alguna condición.
            pass_open_trades = True
        else:
            pass_open_trades = False

        # Si NO estamos en posición, evaluar si abrimos:
        if (not in_position) and (not pass_open_trades) and (not global_stop_active):
            if signal == 1:
                # Abrir trade al cierre de esta vela
                entry_price = current_close
                atr_value = df_bt.at[idx_current, "ATR"]
                if pd.isna(atr_value) or atr_value <= 0:
                    continue

                # Definir Stop Loss y Take Profit
                stop_loss_price = entry_price - (atr_stop_mult * atr_value)
                take_profit_price = entry_price + (atr_tp_mult * atr_value)

                # Calcular tamaño de la posición basado en riesgo
                loss_per_unit = (entry_price - stop_loss_price)
                risk_amount = capital * risk_per_trade
                position_size = risk_amount / loss_per_unit
                max_size = capital / entry_price
                if position_size > max_size:
                    position_size = max_size

                # Comisión de entrada
                commission_entry = entry_price * position_size * commission_rate
                capital -= commission_entry

                in_position = True

                current_trade = {
                    "entry_time": idx_current,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss_price,
                    "take_profit": take_profit_price,
                    "size": position_size,
                    "commission_entry": commission_entry,
                    "partial_exit_done": False  # Para controlar si ya hicimos la salida parcial
                }
                trades.append(current_trade)

        else:
            # Si estamos en posición, revisar la vela siguiente (datos intrabar)
            if in_position:
                bar_low = df_bt.at[idx_next, "low"]
                bar_high = df_bt.at[idx_next, "high"]

                current_trade = trades[-1]
                stop_loss_price = current_trade["stop_loss"]
                take_profit_price = current_trade["take_profit"]
                partial_exit_done = current_trade["partial_exit_done"]

                exit_price = None
                exit_time = None
                trade_closed = False

                # 1) Revisión de STOP LOSS
                if bar_low <= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_time = idx_next
                    trade_closed = True

                # 2) Revisión de TAKE PROFIT PARCIAL (si no hemos salido y no se ha hecho)
                if (not trade_closed) and (not partial_exit_done):
                    # Si el precio toca al menos "entry_price + PARTIAL_EXIT_TRIGGER * ATR"
                    # hacemos salida parcial y movemos el stop a trailing
                    partial_exit_price = current_trade["entry_price"] + (PARTIAL_EXIT_TRIGGER * df_bt.at[idx_current, "ATR"])
                    if bar_high >= partial_exit_price:
                        # Salimos de una fracción de la posición
                        size_to_close = current_trade["size"] * PARTIAL_EXIT_RATIO
                        commission_exit_partial = partial_exit_price * size_to_close * commission_rate
                        pnl_partial = (partial_exit_price - current_trade["entry_price"]) * size_to_close
                        pnl_partial -= commission_exit_partial
                        capital += pnl_partial

                        # Marcar que ya hicimos salida parcial
                        trades[-1]["partial_exit_done"] = True

                        # Ajustar stop al trailing stop
                        new_trailing_stop = bar_high - (TRAILING_STOP_BUFFER * df_bt.at[idx_current, "ATR"])
                        # Si el trailing queda por debajo del stop original, mantenemos el más alto (protege más)
                        trades[-1]["stop_loss"] = max(new_trailing_stop, stop_loss_price)

                        # Reducir la size viva en el trade
                        trades[-1]["size"] -= size_to_close

                # 3) Revisión de TAKE PROFIT TOTAL (si no hemos cerrado)
                if (not trade_closed) and (bar_high >= take_profit_price):
                    exit_price = take_profit_price
                    exit_time = idx_next
                    trade_closed = True

                # Si determinamos un cierre (SL o TP)
                if trade_closed and exit_price is not None:
                    final_size = trades[-1]["size"]
                    commission_exit = exit_price * final_size * commission_rate
                    trade_pnl = (exit_price - current_trade["entry_price"]) * final_size
                    trade_pnl -= commission_exit
                    capital += trade_pnl

                    trades[-1]["exit_time"] = exit_time
                    trades[-1]["exit_price"] = exit_price
                    trades[-1]["commission_exit"] = commission_exit
                    trades[-1]["pnl_$"] = trade_pnl
                    trades[-1]["pnl_%"] = (exit_price - current_trade["entry_price"]) / current_trade["entry_price"]

                    # Actualizar consecutivos
                    if trade_pnl < 0:
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    in_position = False

    # Si queda una posición abierta, cerrarla en la última vela
    if in_position:
        last_idx = idx_list[-1]
        last_close = df_bt.at[last_idx, "close"]
        current_trade = trades[-1]
        final_size = current_trade["size"]

        exit_price = last_close
        commission_exit = exit_price * final_size * commission_rate
        trade_pnl = (exit_price - current_trade["entry_price"]) * final_size
        trade_pnl -= commission_exit
        capital += trade_pnl

        trades[-1]["exit_time"] = last_idx
        trades[-1]["exit_price"] = exit_price
        trades[-1]["commission_exit"] = commission_exit
        trades[-1]["pnl_$"] = trade_pnl
        trades[-1]["pnl_%"] = (exit_price - current_trade["entry_price"]) / current_trade["entry_price"]

        if trade_pnl < 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0

        in_position = False

    df_bt.at[idx_list[-1], "capital"] = capital

    # Equity curve y cálculo de drawdown
    df_bt["equity_curve"] = df_bt["capital"].ffill()
    df_bt["peak"] = df_bt["equity_curve"].cummax()
    df_bt["drawdown"] = df_bt["equity_curve"] / df_bt["peak"] - 1

    trades_df = pd.DataFrame(trades)
    return df_bt, trades_df

def calculate_metrics(df_bt: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float) -> dict:
    """Calcula métricas clave a partir de la equity curve y el registro de trades."""
    closed_trades = trades_df.dropna(subset=["exit_price"])
    total_trades = len(closed_trades)

    if total_trades == 0:
        return {
            "Total Trades": 0,
            "Wins": 0,
            "Losses": 0,
            "Win Rate": 0.0,
            "Final Capital": initial_capital,
            "Profit Factor": 0.0,
            "Max Drawdown": 0.0
        }

    wins = closed_trades[closed_trades["pnl_$"] > 0].shape[0]
    losses = total_trades - wins
    win_rate = wins / total_trades if total_trades > 0 else 0.0

    sum_wins = closed_trades[closed_trades["pnl_$"] > 0]["pnl_$"].sum()
    sum_losses = abs(closed_trades[closed_trades["pnl_$"] <= 0]["pnl_$"].sum())

    profit_factor = sum_wins / sum_losses if sum_losses > 0 else 0.0
    final_capital = df_bt["capital"].dropna().iloc[-1]
    max_drawdown = df_bt["drawdown"].min()

    metrics = {
        "Total Trades": total_trades,
        "Wins": wins,
        "Losses": losses,
        "Win Rate": round(win_rate, 3),
        "Final Capital": round(final_capital, 2),
        "Profit Factor": round(profit_factor, 3),
        "Max Drawdown": round(max_drawdown, 3)
    }
    return metrics

def main():
    try:
        logger.info("=== Iniciando Backtest Mejorado (con intrabar + mejoras) ===")
        
        # 1) Cargar y preparar datos
        df = load_data(CSV_PATH)
        df = add_technical_indicators(df)
        X = prepare_dataset(df, FIXED_FEATURES)
        
        # 2) Cargar modelo entrenado
        if not os.path.exists(MODEL_FILE):
            logger.error(f"No se encontró el modelo entrenado: {MODEL_FILE}")
            sys.exit(1)
        model = joblib.load(MODEL_FILE)
        logger.info("Modelo cargado exitosamente.")

        # 3) Generar señales con el modelo (umbral más estricto = 0.7)
        signals = generate_signals(model, X, threshold=THRESHOLD_HIGH)
        df["signals"] = signals

        # 4) Ejecutar backtest con nuevas mejoras
        df_bt, trades_df = backtest_trades_risk_intrabar(
            df,
            signals,
            atr_stop_mult=ATR_STOP_MULT,
            atr_tp_mult=ATR_TP_MULT,
            initial_capital=INITIAL_CAPITAL,
            risk_per_trade=RISK_PER_TRADE,
            commission_rate=COMMISSION_RATE,
            max_dd_limit=MAX_DRAWDOWN_LIMIT,
            max_consecutive_losses=MAX_CONSECUTIVE_LOSSES
        )

        # 5) Calcular métricas y guardar resultados
        metrics = calculate_metrics(df_bt, trades_df, INITIAL_CAPITAL)
        logger.info(f"Métricas del Backtest con mejoras:\n{metrics}")

        out_csv = os.path.join(RESULTS_PATH, "backtest_intrabar_results.csv")
        df_bt.to_csv(out_csv)
        trades_csv = os.path.join(RESULTS_PATH, "backtest_intrabar_trades.csv")
        trades_df.to_csv(trades_csv, index=False)
        logger.info(f"Resultados guardados en {out_csv} y {trades_csv}")

        # 6) Graficar Equity Curve
        plt.figure(figsize=(10, 6))
        plt.plot(df_bt.index, df_bt["equity_curve"], label="Equity Curve")
        plt.title("Curva de Capital - Backtest Mejorado (Intrabar + mejoras)")
        plt.xlabel("Fecha")
        plt.ylabel("Capital (USD)")
        plt.legend()
        plt.tight_layout()
        eq_file = os.path.join(RESULTS_PATH, "equity_curve_intrabar.png")
        plt.savefig(eq_file)
        plt.close()
        logger.info(f"Curva de capital guardada en {eq_file}")

        # 7) Graficar Drawdown
        plt.figure(figsize=(10, 4))
        plt.plot(df_bt.index, df_bt["drawdown"], label="Drawdown", color="red")
        plt.title("Drawdown - Backtest Mejorado (Intrabar + mejoras)")
        plt.xlabel("Fecha")
        plt.ylabel("Drawdown")
        plt.legend()
        plt.tight_layout()
        dd_file = os.path.join(RESULTS_PATH, "drawdown_intrabar.png")
        plt.savefig(dd_file)
        plt.close()
        logger.info(f"Gráfica de Drawdown guardada en {dd_file}")

        logger.info("=== Backtest Mejorado Finalizado con Éxito ===")
    except Exception as e:
        logger.error(f"Error en main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
