# ====================================================================================
# BACKTESTING WALK-FORWARD CON XGBOOST + ROBUST SCALER + INDICADORES MOMENTUM
# ====================================================================================
# Este script muestra un flujo completo para entrenar y backtestear un modelo ML
# (XGBoost) con un pipeline que incluye RobustScaler. Añade lógica de:
#   - Generación de un target binario basado en el cambio futuro de precio.
#   - Indicadores técnicos, incluyendo momentum (ROC) y otros.
#   - Stops basados en ATR (stop-loss, take-profit y trailing-stop).
#   - Filtrado por tendencia (SMA_50 vs. SMA_200).
#   - Walk-Forward Analysis para evaluar robustez en múltiples ventanas temporales.
#
# AJUSTA ESTOS PARÁMETROS A TU PROPIO CASO (features, horizonte, min_change, etc.).
# Usa Optuna o la librería de tu preferencia para optimizar hiperparámetros.
# ====================================================================================

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

# Para métricas avanzadas (Sharpe, Sortino, etc.) con quantstats:
# pip install quantstats
# import quantstats as qs

# =============== CONFIGURACIÓN DE LOGGING ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# =============== PARÁMETROS GLOBALES ===============
TRAIN_SIZE_BARS   = 25280   # Aprox. 6 meses si el timeframe es 15 min
TEST_SIZE_BARS    = 5880    # Aprox. 1 mes
STEP_SIZE         = 1880    # Se avanza 1 mes en cada ciclo
INITIAL_CAPITAL   = 1000.0
COMMISSION_RATE   = 0.0003  # 0.03% por trade (ejemplo)
RISK_PER_TRADE    = 0.01    # 1% del capital por operación
MAX_BARS_IN_TRADE = 14    # Máx. velas manteniendo una posición

# Parámetros ATR y stops
ATR_PERIOD     = 14
ATR_SL_MULT    = 0.2   # stop-loss = precio entrada - (ATR * 0.5)
ATR_TP_MULT    = 0.6   # take-profit = precio entrada + (ATR * 1.0)
TRAILING_MULT  = 0.2   # trailing-stop dinámico

# Filtrado de tendencia
SHORT_MA_COL = "SMA_50"
LONG_MA_COL  = "SMA_200"

# Umbral de probabilidad para señal de compra
SIGNAL_THRESHOLD = 0.82

# =============== FUNCIONES AUXILIARES ===============

def handle_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia NaN e infinitos, aplicando forward/backward fill.
    """
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.ffill(inplace=True)
    df.bfill(inplace=True)
    df.dropna(inplace=True)
    return df

def compute_sma(series: pd.Series, window: int) -> pd.Series:
    """Media móvil simple."""
    return series.rolling(window).mean()

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Cálculo simple de ATR (Average True Range).
    ATR = media del True Range en 'period' velas.
    """
    df = df.copy()
    df["prev_close"] = df["close"].shift(1)
    df["high_low"]   = df["high"] - df["low"]
    df["high_close"] = (df["high"] - df["prev_close"]).abs()
    df["low_close"]  = (df["low"]  - df["prev_close"]).abs()

    df["TR"]  = df[["high_low","high_close","low_close"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(period).mean()

    df.drop(columns=["prev_close","high_low","high_close","low_close","TR"], inplace=True)
    df.dropna(subset=["ATR"], inplace=True)
    return df

def compute_roc(series: pd.Series, window: int = 14) -> pd.Series:
    """
    Rate Of Change (ROC) como ejemplo de indicador de momentum:
      ROC = (precio_actual - precio_(n velas atrás)) / precio_(n velas atrás) * 100
    """
    shifted = series.shift(window)
    roc = ((series - shifted) / shifted) * 100.0
    return roc

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade indicadores básicos: SMA_50, SMA_200, ATR, ROC (momentum).
    Ajusta según tus necesidades de features.
    """
    df = df.copy()
    df["SMA_50"]  = compute_sma(df["close"], 50)
    df["SMA_200"] = compute_sma(df["close"], 200)
    df = compute_atr(df, ATR_PERIOD)
    df["ROC_14"]  = compute_roc(df["close"], 14)  # momentum
    return df

def generate_target_binary(df: pd.DataFrame, look_ahead: int = 3, min_change: float = 0.03) -> pd.DataFrame:
    """
    Genera un target binario basado en cambio futuro:
      1 => si el precio sube al menos min_change% en 'look_ahead' velas,
      0 => si cae al menos min_change% en 'look_ahead' velas.
    Se ignoran (NaN) casos intermedios.
    """
    df = df.copy()
    df["future_close"] = df["close"].shift(-look_ahead)
    df.dropna(subset=["future_close"], inplace=True)

    change_pct = (df["future_close"] - df["close"]) / df["close"]
    df["target"] = np.nan
    df.loc[ change_pct >=  min_change, "target"] = 1
    df.loc[ change_pct <= -min_change, "target"] = 0

    # Elimina los casos "grises" donde la variación no llegó a ±min_change
    df.dropna(subset=["target"], inplace=True)
    return df

def get_feature_columns() -> list:
    """
    Devuelve la lista de columnas a usar como features para el modelo.
    Ajusta según tus indicadores reales.
    """
    return [
        "open", "high", "low", "close", "volume",
        "SMA_50", "SMA_200", "ATR", "ROC_14"
    ]

def prepare_dataset(df: pd.DataFrame):
    """
    Prepara X, y para entrenamiento. Aplica un pipeline con RobustScaler a las features.
    """
    # 1) Extraer features y label
    features = get_feature_columns()
    X = df[features].copy()
    y = df["target"].copy()

    # 2) Manejar NaN
    X = handle_missing_data(X)
    y = y.reindex(X.index).fillna(0)

    return X, y

def create_pipeline_xgb() -> Pipeline:
    """
    Crea un pipeline con RobustScaler + XGBoost (hiperparámetros ajustados).
    Ajusta según tus necesidades o tu búsqueda con Optuna.
    """
    numeric_features = get_feature_columns()

    # Preprocesamiento (ejemplo con RobustScaler)
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", RobustScaler(), numeric_features)
        ],
        remainder="passthrough"  # Deja intactas otras columnas, si las hubiera
    )

    # Modelo XGBoost con parámetros "razonables" (ajusta a tu gusto)
    xgb_params = {
        "objective": "binary:logistic",
        "n_estimators": 150,
        "learning_rate": 0.05,
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "eval_metric": "logloss",
        "use_label_encoder": False
    }

    model = XGBClassifier(**xgb_params)

    # Pipeline final
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", model)
    ])

    return pipeline

def train_model_on_window(df_train: pd.DataFrame) -> Pipeline:
    """
    Entrena el pipeline en la ventana de entrenamiento df_train.
    Retorna el pipeline entrenado.
    """
    df_train = df_train.copy()

    # 1) Añadir indicadores y target
    df_train = add_technical_indicators(df_train)
    df_train = generate_target_binary(df_train, look_ahead=3, min_change=0.03)
    if len(df_train) < 50:
        # Evitar entrenar con datos insuficientes
        return None

    # 2) Preparar X, y
    X_train, y_train = prepare_dataset(df_train)

    # 3) Crear pipeline y entrenar
    pipeline = create_pipeline_xgb()
    pipeline.fit(X_train, y_train)
    return pipeline

def generate_signals(df: pd.DataFrame, pipeline: Pipeline) -> pd.Series:
    """
    Genera señales de compra (1) o no-trade (0) a partir de la prob. predicha por el modelo.
    """
    if pipeline is None or len(df) == 0:
        return pd.Series([0]*len(df), index=df.index)

    # Extraer X
    X = df[get_feature_columns()].copy()
    X = handle_missing_data(X)

    if len(X) == 0:
        return pd.Series([0]*len(df), index=df.index)

    # Probabilidad de ser clase=1
    probs = pipeline.predict_proba(X)[:, 1]
    signals = (probs > SIGNAL_THRESHOLD).astype(int)

    return pd.Series(signals, index=X.index)

def filter_signals_by_trend(df: pd.DataFrame, signals: pd.Series) -> pd.Series:
    """
    Filtra señales en largo sólo si SMA_50 > SMA_200.
    """
    if SHORT_MA_COL not in df.columns or LONG_MA_COL not in df.columns:
        logger.warning("Faltan columnas de SMA_50 / SMA_200 para filtrar tendencia.")
        return signals

    trend_mask = (df[SHORT_MA_COL] > df[LONG_MA_COL])
    filtered_signals = signals.where(trend_mask, other=0)
    return filtered_signals

def volatility_position_size(capital, entry_price, stop_loss, consecutive_losses=0) -> float:
    """
    Calcula tamaño de posición basado en RISK_PER_TRADE y distancia stop_loss.
    Reduce el riesgo gradualmente si hay pérdidas consecutivas.
    """
    reduce_factor = 0.9 ** consecutive_losses
    effective_risk = RISK_PER_TRADE * reduce_factor
    if effective_risk < 0.002:
        effective_risk = 0.002

    risk_amount = capital * effective_risk
    distance_sl = (entry_price - stop_loss)
    if distance_sl <= 0:
        return 0.0

    size = risk_amount / distance_sl
    # Evitar posiciones más grandes que todo el capital
    max_size = capital / entry_price
    size = min(size, max_size)
    return size

def backtest_on_period(df: pd.DataFrame, pipeline: Pipeline, initial_capital: float):
    """
    Backtest en la ventana df (con pipeline entrenado).
    - Genera señales
    - Aplica stop-loss, take-profit, trailing-stop
    - Devuelve:
        equity_df: DataFrame [time, equity_curve, drawdown]
        trades_df: registro de operaciones
        final_capital
    """
    df_bt = df.copy()
    df_bt = add_technical_indicators(df_bt)
    df_bt.dropna(subset=["close","high","low","SMA_50","SMA_200","ATR"], inplace=True)

    # Generar señales y filtrar por tendencia
    raw_signals = generate_signals(df_bt, pipeline)
    signals = filter_signals_by_trend(df_bt, raw_signals)
    df_bt["signal"] = signals

    capital = initial_capital
    peak_capital = capital
    in_position = False
    trades = []
    equity_curve = []
    drawdowns = []
    idxs = df_bt.index.to_list()
    bars_in_trade = 0
    consecutive_losses = 0

    for i in range(len(idxs) - 1):
        idx_now = idxs[i]
        idx_next = idxs[i+1]

        current_close = df_bt.at[idx_now, "close"]
        df_bt.at[idx_now, "equity_curve"] = capital
        if capital > peak_capital:
            peak_capital = capital
        current_dd = 1.0 - (capital / peak_capital)
        df_bt.at[idx_now, "drawdown"] = current_dd

        # Si hay posición abierta:
        if in_position:
            bars_in_trade += 1
            trade_open = trades[-1]
            stop_loss    = trade_open["stop_loss"]
            take_profit  = trade_open["take_profit"]
            trailing_sl  = trade_open.get("trailing_stop", stop_loss)

            next_low     = df_bt.at[idx_next, "low"]
            next_high    = df_bt.at[idx_next, "high"]
            next_close   = df_bt.at[idx_next, "close"]
            atr_value    = df_bt.at[idx_now, "ATR"]

            # Ajustar trailing-stop si el precio subió
            new_trailing = next_close - (atr_value * TRAILING_MULT)
            if new_trailing > trailing_sl:
                trailing_sl = new_trailing

            final_stop = max(stop_loss, trailing_sl)
            trade_closed = False
            exit_price = None

            # 1) Stop-loss o trailing-stop
            if next_low <= final_stop:
                exit_price = final_stop
                trade_closed = True

            # 2) Take-profit
            if (not trade_closed) and (next_high >= take_profit):
                exit_price = take_profit
                trade_closed = True

            # 3) Cierre por tiempo máximo
            if (not trade_closed) and bars_in_trade >= MAX_BARS_IN_TRADE:
                exit_price = next_close
                trade_closed = True

            if trade_closed and exit_price is not None:
                size = trade_open["size"]
                gross_pnl = (exit_price - trade_open["entry_price"]) * size
                commission_exit = exit_price * size * COMMISSION_RATE
                net_pnl = gross_pnl - commission_exit
                capital += net_pnl

                trades[-1]["exit_time"]       = idx_next
                trades[-1]["exit_price"]      = exit_price
                trades[-1]["commission_exit"] = commission_exit
                trades[-1]["pnl_$"]           = net_pnl
                trades[-1]["pnl_%"]           = (exit_price / trade_open["entry_price"] - 1.0) * 100

                if net_pnl < 0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0

                in_position = False
                bars_in_trade = 0
            else:
                # Actualizar trailing stop si sigue abierto
                trades[-1]["trailing_stop"] = trailing_sl

        else:
            # Checar si abrimos posición
            signal = df_bt.at[idx_now, "signal"]
            if signal == 1:
                entry_price = current_close
                atr_value   = df_bt.at[idx_now, "ATR"]
                stop_loss   = entry_price - (ATR_SL_MULT * atr_value)
                take_profit = entry_price + (ATR_TP_MULT * atr_value)

                if stop_loss >= entry_price:
                    continue

                size = volatility_position_size(capital, entry_price, stop_loss, consecutive_losses)
                if size <= 0:
                    continue

                # Comisión de entrada
                commission_entry = entry_price * size * COMMISSION_RATE
                if capital < commission_entry:
                    continue

                capital -= commission_entry
                trade_data = {
                    "entry_time": idx_now,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "size": size,
                    "commission_entry": commission_entry,
                    "trailing_stop": stop_loss
                }
                trades.append(trade_data)
                in_position = True
                bars_in_trade = 0

        equity_curve.append(capital)
        drawdowns.append(current_dd)

    final_capital = capital
    # Construir DF de equity
    equity_df = pd.DataFrame({
        "time": df_bt.index[:len(equity_curve)],
        "equity_curve": equity_curve,
        "drawdown": drawdowns
    }).set_index("time")

    trades_df = pd.DataFrame(trades)
    return equity_df, trades_df, final_capital

def walk_forward_analysis(df: pd.DataFrame):
    """
    Aplica Walk-Forward:
      - Toma TRAIN_SIZE_BARS de entrenamiento.
      - Toma TEST_SIZE_BARS para backtest.
      - Repite avanzando STEP_SIZE barras cada vez.
      - Mantiene y arrastra capital final a la siguiente ventana.
    """
    start_index = 0
    capital = INITIAL_CAPITAL

    all_equity_dfs = []
    all_trades_dfs = []

    while True:
        train_end = start_index + TRAIN_SIZE_BARS
        test_end  = train_end + TEST_SIZE_BARS
        if test_end > len(df):
            logger.info("No hay más datos para la siguiente ventana. Fin WFA.")
            break

        df_train = df.iloc[start_index:train_end].copy()
        df_test  = df.iloc[train_end:test_end].copy()
        if len(df_train) < 200:
            logger.warning(f"Pocos datos en train: {len(df_train)}. Se detiene.")
            break

        # ENTRENAR MODELO
        pipeline = train_model_on_window(df_train)
        if pipeline is None:
            logger.warning("No se pudo entrenar el modelo. Ventana con datos insuficientes.")
            break

        # BACKTEST
        equity_df, trades_df, final_capital = backtest_on_period(df_test, pipeline, capital)
        logger.info(f"WFA Ventana Train=({start_index}:{train_end}), "
                    f"Test=({train_end}:{test_end}) => Capital final: {final_capital:.2f} "
                    f"Trades: {len(trades_df)}")

        all_equity_dfs.append(equity_df)
        trades_df["train_range"] = f"{start_index}-{train_end}"
        trades_df["test_range"]  = f"{train_end}-{test_end}"
        all_trades_dfs.append(trades_df)

        # Actualizar capital para la siguiente iteración
        capital = final_capital
        start_index += STEP_SIZE

    # Combinar resultados
    if all_equity_dfs:
        all_equity = pd.concat(all_equity_dfs)
    else:
        all_equity = pd.DataFrame(columns=["equity_curve","drawdown"])

    if all_trades_dfs:
        all_trades = pd.concat(all_trades_dfs, ignore_index=True)
    else:
        all_trades = pd.DataFrame()

    return all_equity, all_trades

def main():
    # Ajusta la ruta del CSV a tu proyecto
    CSV_PATH = "ML/data/processed/BTCUSDT_15m_processed.csv"
    if not os.path.exists(CSV_PATH):
        logger.error(f"No existe el archivo {CSV_PATH}")
        sys.exit(1)

    # Cargar y ordenar
    df = pd.read_csv(CSV_PATH, parse_dates=["open_time"])
    df.set_index("open_time", inplace=True)
    df.sort_index(inplace=True)

    # Limpieza preliminar
    df = handle_missing_data(df)

    # Iniciar Walk-Forward
    equity_curve, trades_df = walk_forward_analysis(df)

    # Guardar resultados
    os.makedirs("results_wfa", exist_ok=True)
    equity_curve.to_csv("results_wfa/equity_curve.csv")
    trades_df.to_csv("results_wfa/trades.csv", index=False)

    # Graficar Equity
    if not equity_curve.empty:
        plt.figure(figsize=(10,6))
        plt.plot(equity_curve.index, equity_curve["equity_curve"], label="Equity Curve")
        plt.title("Equity Curve - Walk Forward Analysis")
        plt.xlabel("Fecha")
        plt.ylabel("Capital")
        plt.legend()
        plt.tight_layout()
        plt.savefig("results_wfa/equity_curve.png")
        plt.close()
        logger.info("Equity curve graficada en results_wfa/equity_curve.png")
    else:
        logger.warning("No se generó equity curve para graficar.")

    # Opcional: métricas de performance con quantstats
    # try:
    #     returns = equity_curve["equity_curve"].pct_change().fillna(0)
    #     qs.reports.html(returns, output="results_wfa/wfa_quantstats.html")
    #     logger.info("Reporte QuantStats generado en results_wfa/wfa_quantstats.html")
    # except Exception as e:
    #     logger.error(f"Error al generar reporte quantstats: {e}")

    logger.info("Proceso completado. Revisa la carpeta 'results_wfa' para resultados.")

if __name__ == "__main__":
    main()
