import os
import sys
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pandas_ta as ta  # Para indicadores técnicos

from xgboost import XGBClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    confusion_matrix,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_curve,
    roc_auc_score,
    balanced_accuracy_score,
    matthews_corrcoef
)
from sklearn.preprocessing import RobustScaler, OneHotEncoder
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.utils.class_weight import compute_class_weight

import joblib
import optuna
from imblearn.combine import SMOTETomek

from datetime import datetime

# =========================== PARÁMETROS FIJOS ================================
CSV_PATH = "ML/data/processed/BTCUSDT_15m_processed.csv"
TEST_SIZE = 0.35
RANDOM_SEED = 62

# Tamaño de la muestra del train para búsqueda de hiperparámetros (Optuna)
SAMPLE_FRAC = 0.9

# Cantidad de trials de Optuna
N_TRIALS = 12
N_JOBS_OPTUNA = 1

# ¿Usar PCA?
USE_PCA = False
PCA_N_COMPONENTS = 8

# Directorio para resultados
RESULTS_PATH = ".ML/results"
MODEL_NAME = "XGBoost_Binario"

# Parámetros fijos del target binario
FIXED_LOOK_AHEAD = 3
FIXED_MIN_CHANGE = 0.031

# Umbral para señal de compra
PROB_THRESHOLD_HIGH = 0.61

# Lista fija de características (indicadores + OHLC + volumen)
# === Se eliminaron VWAP y ATR para evitar problemas ===
FIXED_FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'RSI', 'MACD', 'MACDs', 'MACDh',
    'SMA_10', 'SMA_50', 'EMA_10', 'EMA_50',
    'OBV', 'BBL', 'BBM', 'BBU',
    'STOCHk', 'STOCHd', 'CCI', 'SMA_200'
]

# ============================ CONFIGURACIÓN DE LOGGING ============================
def setup_logging(log_path="logs"):
    os.makedirs(log_path, exist_ok=True)
    logger = logging.getLogger("trading_ml_binario")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_path, f"training_{timestamp}.log")
    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

# ============================ CARGA DE DATOS ============================
def load_data(csv_path: str) -> pd.DataFrame:
    required_columns = {"open_time", "close", "high", "low", "volume", "open"}
    if not os.path.exists(csv_path):
        logger.error(f"No se encontró el archivo: {csv_path}")
        sys.exit(1)
    try:
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
        df = df.sort_index()
        logger.info(f"Datos cargados desde {csv_path}, filas: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar datos: {e}")
        sys.exit(1)

# ============================ GENERACIÓN DE TARGET BINARIO ============================
def generate_target_binary(df: pd.DataFrame, look_ahead: int = 4, min_change: float = 0.035) -> pd.DataFrame:
    """
    Genera un target binario:
    1 = 'comprar' si (future_close - close)/close >= min_change
    0 = 'vender' si (future_close - close)/close <= -min_change
    Excluye casos donde el cambio está entre -min_change y min_change (NaN).
    """
    if look_ahead <= 0:
        logger.error("look_ahead debe ser positivo.")
        sys.exit(1)
    if not (0 < min_change):
        logger.error("min_change debe ser positivo.")
        sys.exit(1)
    df = df.copy()
    df["future_close"] = df["close"].shift(-look_ahead)
    df.dropna(subset=["future_close"], inplace=True)
    df["change_pct"] = (df["future_close"] - df["close"]) / df["close"]
    conditions = [df["change_pct"] >= min_change, df["change_pct"] <= -min_change]
    choices = [1, 0]
    df["target"] = np.select(conditions, choices, default=np.nan)
    df.dropna(subset=["change_pct", "target"], inplace=True)
    logger.info(f"Target binario con look_ahead={look_ahead}, min_change={min_change}")
    logger.info(f"Distribución de clases (0=Vender, 1=Comprar):\n{df['target'].value_counts()}")
    sample = df[['close', 'future_close', 'change_pct', 'target']].sample(10)
    logger.info(f"Muestra de datos con target:\n{sample}")
    return df

# ============================ AGREGAR INDICADORES TÉCNICOS ============================
def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores técnicos con pandas_ta.
    Se han eliminado VWAP y ATR para evitar problemas.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.error("El índice debe ser DatetimeIndex para calcular indicadores.")
        sys.exit(1)
    df = df.copy()
    df.sort_index(inplace=True)
    # Elimina indicadores previos si existen
    existing_indicator_cols = [
        'RSI', 'MACD', 'MACDs', 'MACDh', 'OBV',
        'BBL', 'BBM', 'BBU', 'STOCHk', 'STOCHd', 'CCI',
        'SMA_10', 'SMA_50', 'SMA_200', 'EMA_10', 'EMA_50'
    ]
    cols_to_drop = [col for col in existing_indicator_cols if col in df.columns]
    if cols_to_drop:
        logger.info(f"Columnas de indicadores existentes eliminadas: {cols_to_drop}")
        df.drop(columns=cols_to_drop, inplace=True)
    # Calcular indicadores técnicos
    df.ta.rsi(length=14, append=True)            # RSI_14
    df.ta.macd(append=True)                      # MACD_12_26_9, MACDs_12_26_9, MACDh_12_26_9
    df.ta.sma(length=10, append=True)            # SMA_10
    df.ta.sma(length=50, append=True)            # SMA_50
    df.ta.ema(length=10, append=True)            # EMA_10
    df.ta.ema(length=50, append=True)            # EMA_50
    df.ta.obv(append=True)                       # OBV
    df.ta.bbands(length=20, std=2, append=True)  # BBL_20_2.0, BBM_20_2.0, BBU_20_2.0
    df.ta.stoch(append=True)                     # STOCHk_14_3_3, STOCHd_14_3_3
    df.ta.cci(length=14, append=True)            # CCI_14_0.015
    df.ta.sma(length=200, append=True)           # SMA_200
    df = df.apply(pd.to_numeric, errors='coerce')
    initial_length = len(df)
    df.dropna(subset=['open','high','low','close','volume'], how='any', inplace=True)
    df.dropna(inplace=True)
    final_length = len(df)
    logger.info(f"Indicadores técnicos agregados. Filas antes: {initial_length}, después: {final_length} (descartadas: {initial_length - final_length})")
    rename_dict = {
        'RSI_14': 'RSI',
        'MACD_12_26_9': 'MACD',
        'MACDs_12_26_9': 'MACDs',
        'MACDh_12_26_9': 'MACDh',
        'BBL_20_2.0': 'BBL',
        'BBM_20_2.0': 'BBM',
        'BBU_20_2.0': 'BBU',
        'STOCHk_14_3_3': 'STOCHk',
        'STOCHd_14_3_3': 'STOCHd',
        'CCI_14_0.015': 'CCI',
        'SMA_10': 'SMA_10',
        'SMA_50': 'SMA_50',
        'SMA_200': 'SMA_200',
        'EMA_10': 'EMA_10',
        'EMA_50': 'EMA_50'
    }
    df.rename(columns=rename_dict, inplace=True)
    logger.info(f"Columnas renombradas: {rename_dict}")
    for col in ['CCI','SMA_200','STOCHk','STOCHd']:
        if col in df.columns and df[col].isna().all():
            logger.warning(f"El indicador '{col}' quedó 100% NaN. Se eliminará.")
            df.drop(columns=[col], inplace=True)
    for col in FIXED_FEATURES:
        if col not in df.columns:
            logger.warning(f"Columna faltante '{col}' añadida con NaN.")
            df[col] = np.nan
    keep_cols = FIXED_FEATURES + ['target', 'change_pct', 'future_close']
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    return df

# ============================ PREPARAR DATASET ============================
def prepare_dataset(df: pd.DataFrame, feature_cols: list, label_col="target"):
    df = df.copy()
    columns_to_keep = feature_cols + [label_col]
    extra_cols = [col for col in df.columns if col not in columns_to_keep]
    if extra_cols:
        logger.info(f"Columnas extra eliminadas: {extra_cols}")
        df.drop(columns=extra_cols, inplace=True)
    for col in feature_cols:
        if col not in df.columns:
            df[col] = np.nan
            logger.warning(f"Columna faltante '{col}' añadida como NaN.")
    X = df[feature_cols].copy()
    if label_col in df.columns:
        y = df[label_col].copy()
    else:
        logger.warning("No se encontró 'label_col' en df. Se usará y=0.")
        y = pd.Series([0]*len(X), index=X.index)
    all_nan_cols = [c for c in X.columns if X[c].isna().all()]
    if all_nan_cols:
        logger.warning(f"Estas columnas están 100% NaN y se eliminarán antes de imputar: {all_nan_cols}")
        X.drop(columns=all_nan_cols, inplace=True)
    imputer = SimpleImputer(strategy='mean')
    X_array = imputer.fit_transform(X)
    X = pd.DataFrame(X_array, columns=X.columns, index=X.index)
    y.fillna(0, inplace=True)
    if not all(np.issubdtype(dtype, np.number) for dtype in X.dtypes):
        logger.error("No todas las características son numéricas.")
        sys.exit(1)
    logger.info(f"Dataset preparado: {X.shape[0]} filas, {X.shape[1]} features (después de imputación).")
    return X, y

# ============================ BALANCE DE CLASES CON SMOTETOMEK ============================
def balance_classes_smote(X, y):
    smote_tomek = SMOTETomek(random_state=RANDOM_SEED)
    X_res, y_res = smote_tomek.fit_resample(X, y)
    logger.info(f"Balance de clases tras SMOTETomek:\n{pd.Series(y_res).value_counts()}")
    return X_res, y_res

# =============== FUNCIÓN OBJETIVO DE OPTUNA (OPTIMIZA PRECISIÓN CLASE 1) ================
def objective_precision_class1(trial, X, y, cv, categorical_features):
    """
    Métrica a optimizar: Precisión de la clase '1' (comprar).
    """
    param = {
        "max_depth": trial.suggest_int("max_depth", 1, 35),
        "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 100, 700),
        "subsample": trial.suggest_float("subsample", 0.4, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "gamma": trial.suggest_float("gamma", 0, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 0, 1.0),
        "reg_lambda": trial.suggest_float("reg_lambda", 0, 1.0),
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "random_state": RANDOM_SEED,
        "use_label_encoder": False,
        "verbosity": 0
    }
    if categorical_features:
        preprocessor = ColumnTransformer(
            transformers=[
                ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features)
            ],
            remainder="passthrough",
        )
    else:
        preprocessor = "passthrough"
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("scaler", RobustScaler()),
        ("model", XGBClassifier(**param))
    ])
    precision_scores = []
    for train_idx, valid_idx in cv.split(X, y):
        X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
        y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
        X_train_bal, y_train_bal = balance_classes_smote(X_train, y_train)
        pipeline.fit(X_train_bal, y_train_bal)
        y_pred = pipeline.predict(X_valid)
        prec_clase1 = precision_score(y_valid, y_pred, pos_label=1)
        precision_scores.append(prec_clase1)
    return np.mean(precision_scores)

def train_models_optuna_precision(X, y, cv, categorical_features, n_trials=50, n_jobs=2):
    logger.info("Iniciando optimización en Optuna (precisión clase=1)...")
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=48))
    func = lambda trial: objective_precision_class1(trial, X, y, cv, categorical_features)
    study.optimize(func, n_trials=n_trials, n_jobs=n_jobs)
    logger.info(f"Mejor Precisión (clase=1): {study.best_value:.4f}")
    logger.info(f"Mejores parámetros: {study.best_params}")
    return study.best_params

# ============================ ENTRENAR PIPELINE FINAL ============================
def train_final_pipeline_with_params(X, y, best_params, use_pca=False, pca_n_components=7, categorical_features=[], factor_pos_weight=1.78):
    """
    Entrena el pipeline final con todos los datos,
    enfocándose en la precisión de la clase 1.
    Ajusta scale_pos_weight usando factor_pos_weight si es necesario.
    """
    try:
        if categorical_features:
            preprocessor = ColumnTransformer(
                transformers=[
                    ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features)
                ],
                remainder="passthrough",
            )
        else:
            preprocessor = "passthrough"
        steps = [("preprocessor", preprocessor), ("scaler", RobustScaler())]
        if use_pca:
            steps.append(("pca", PCA(n_components=pca_n_components, random_state=RANDOM_SEED)))
        classes = np.unique(y)
        cw = compute_class_weight(class_weight='balanced', classes=classes, y=y)
        cw_dict = {cls: w for cls, w in zip(classes, cw)}
        if 0 in cw_dict and 1 in cw_dict:
            scale_pos_weight = (cw_dict[0] / cw_dict[1]) * factor_pos_weight
        else:
            scale_pos_weight = 1.0
        model = XGBClassifier(
            objective="binary:logistic",
            random_state=RANDOM_SEED,
            eval_metric="logloss",
            use_label_encoder=False,
            verbosity=0,
            max_depth=best_params["max_depth"],
            learning_rate=best_params["learning_rate"],
            n_estimators=best_params["n_estimators"],
            subsample=best_params["subsample"],
            colsample_bytree=best_params["colsample_bytree"],
            gamma=best_params["gamma"],
            reg_alpha=best_params["reg_alpha"],
            reg_lambda=best_params["reg_lambda"],
            scale_pos_weight=scale_pos_weight
        )
        steps.append(("model", model))
        pipeline = Pipeline(steps)
        pipeline.fit(X, y)
        logger.info(f"Pipeline final entrenado con scale_pos_weight={scale_pos_weight:.2f}")
        return pipeline
    except Exception as e:
        logger.error(f"Error al entrenar el pipeline final: {e}")
        sys.exit(1)

# ============================ EVALUACIÓN DEL MODELO ============================
def evaluate_model(model, X, y):
    try:
        y_pred = model.predict(X).astype(int)
        y_prob = model.predict_proba(X)[:, 1]
    except Exception as e:
        logger.error(f"Error en la predicción: {e}")
        return 0.0
    y_int = y.astype(int).values
    cm = confusion_matrix(y_int, y_pred, labels=[0, 1])
    rep = classification_report(y_int, y_pred, labels=[0, 1], zero_division=0,
                                  target_names=['Vender', 'Comprar'])
    prec_clase1 = precision_score(y_int, y_pred, pos_label=1)
    f1_clase1 = f1_score(y_int, y_pred, pos_label=1)
    recall_clase1 = recall_score(y_int, y_pred, pos_label=1)
    balanced_acc = balanced_accuracy_score(y_int, y_pred)
    mcc = matthews_corrcoef(y_int, y_pred)
    try:
        auc_score = roc_auc_score(y_int, y_prob)
    except:
        auc_score = np.nan
    logger.info(f"Matriz de confusión [0=Vender, 1=Comprar]:\n{cm}")
    logger.info(f"Reporte de Clasificación:\n{rep}")
    logger.info(f"Precision (clase=1): {prec_clase1:.4f}")
    logger.info(f"F1 (clase=1): {f1_clase1:.4f}")
    logger.info(f"Recall (clase=1): {recall_clase1:.4f}")
    logger.info(f"Balanced Accuracy: {balanced_acc:.4f}")
    logger.info(f"MCC: {mcc:.4f}")
    logger.info(f"AUC: {auc_score:.4f}")
    os.makedirs(RESULTS_PATH, exist_ok=True)
    # Matriz de confusión
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=['Vender', 'Comprar'],
                yticklabels=['Vender', 'Comprar'])
    plt.title("Matriz de Confusión (Binaria)")
    plt.xlabel("Predicho")
    plt.ylabel("Real")
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_PATH, "confusion_matrix_binary.png"))
    plt.close()
    # Curva ROC
    try:
        fpr, tpr, _ = roc_curve(y_int, y_prob)
        plt.figure()
        plt.plot(fpr, tpr, label=f"AUC = {auc_score:.2f}")
        plt.plot([0, 1], [0, 1], "k--")
        plt.legend()
        plt.title("Curva ROC (Binaria)")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_PATH, "roc_curve_binary.png"))
        plt.close()
    except Exception as e:
        logger.error(f"Error al generar curva ROC: {e}")
    # Balanced Accuracy y MCC
    plt.figure(figsize=(8, 6))
    metrics = ['Balanced Acc', 'MCC']
    values = [balanced_acc, mcc]
    sns.barplot(x=metrics, y=values)
    plt.ylim(0, 1)
    plt.title("Balanced Accuracy y MCC")
    plt.ylabel("Score")
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_PATH, "balanced_accuracy_mcc_binary.png"))
    plt.close()
    return prec_clase1

# ============================ GENERAR SEÑALES BASADAS EN PROBABILIDAD ============================
def generate_signals(model, X, threshold_high=0.75):
    probabilities = model.predict_proba(X)[:, 1]
    signals = np.zeros(len(probabilities))
    signals[probabilities > threshold_high] = 1
    logger.info(f"Señales generadas con umbral={threshold_high}")
    return signals

# ============================ FILTRAR SEÑALES POR TENDENCIA ============================
def filter_signals_by_trend(df: pd.DataFrame, raw_signals: np.ndarray, short_ma_col: str = "SMA_50", long_ma_col: str = "SMA_200") -> np.ndarray:
    filtered_signals = raw_signals.copy()
    if short_ma_col not in df.columns or long_ma_col not in df.columns:
        logger.warning("No se pueden filtrar señales por tendencia: faltan columnas de medias.")
        return filtered_signals
    short_ma = df[short_ma_col].values
    long_ma = df[long_ma_col].values
    for i in range(len(filtered_signals)):
        if short_ma[i] <= long_ma[i]:
            filtered_signals[i] = 0
    logger.info("Señales filtradas por tendencia.")
    return filtered_signals

# ============================ IMPORTANCIA DE FEATURES ============================
def analyze_feature_importance(model, feature_cols):
    try:
        importances = model.named_steps["model"].feature_importances_
        if len(importances) > len(feature_cols):
            logger.warning("Más columnas en pipeline que en feature_cols.")
        importances = importances[:len(feature_cols)]
        fi_df = pd.DataFrame({
            "feature": feature_cols[:len(importances)],
            "importance": importances
        })
        fi_df.sort_values(by="importance", ascending=False, inplace=True)
        out_csv = os.path.join(RESULTS_PATH, "feature_importances_binary.csv")
        fi_df.to_csv(out_csv, index=False)
        logger.info(f"Importancia de características guardada en {out_csv}.")
        plt.figure(figsize=(12, 10))
        sns.barplot(x="importance", y="feature", data=fi_df.head(20))
        plt.title("Importancia de las Características (Top 20) - Binaria")
        plt.xlabel("Importancia")
        plt.ylabel("Características")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_PATH, "feature_importances_binary.png"))
        plt.close()
        logger.info("Gráfico de importancia de características guardado.")
    except Exception as e:
        logger.error(f"Error al analizar importancia de características: {e}")

def analyze_feature_importance_shap(model, X):
    try:
        import shap
        logger.info("Calculando valores SHAP...")
        explainer = shap.Explainer(model.named_steps["model"])
        shap_values = explainer(X)
        shap.summary_plot(shap_values, X, show=False)
        plt.title("SHAP Summary Plot")
        plt.savefig(os.path.join(RESULTS_PATH, "shap_summary_plot.png"))
        plt.close()
        shap_df = pd.DataFrame(shap_values.values, columns=X.columns)
        shap_df.to_csv(os.path.join(RESULTS_PATH, "shap_values.csv"), index=False)
        logger.info("SHAP analysis completado y guardado.")
    except ImportError:
        logger.error("SHAP no está instalado. Ejecuta 'pip install shap'.")
    except Exception as e:
        logger.error(f"Error al analizar SHAP: {e}")

# ============================ GUARDAR MODELO Y RESULTADOS ============================
def save_model_and_results(model, best_params, feature_cols, model_name="XGBoost_Binario", results_path="./results", alternative_save_path=None):
    os.makedirs(results_path, exist_ok=True)
    model_file = os.path.join(results_path, f"{model_name}_trained_pipeline.joblib")
    try:
        joblib.dump(model, model_file)
        logger.info(f"Pipeline completo guardado en: {model_file}")
    except OSError as e:
        if e.errno == 28:
            logger.error("No hay suficiente espacio en disco.")
            if alternative_save_path:
                try:
                    os.makedirs(alternative_save_path, exist_ok=True)
                    alt_file = os.path.join(alternative_save_path, f"{model_name}_trained_pipeline.joblib")
                    joblib.dump(model, alt_file)
                    logger.info(f"Modelo guardado en ubicación alternativa: {alt_file}")
                except Exception as ex:
                    logger.error(f"No se pudo guardar en la ubicación alternativa: {ex}")
            else:
                logger.error("No se proporcionó ubicación alternativa.")
        else:
            logger.error(f"Error al guardar el modelo: {e}")
    params_file = os.path.join(results_path, f"{model_name}_model_params.txt")
    try:
        with open(params_file, "w") as f:
            for k, v in best_params.items():
                f.write(f"{k}: {v}\n")
        logger.info(f"Parámetros guardados en: {params_file}")
    except Exception as e:
        logger.error(f"Error al guardar los parámetros: {e}")
    feats_file = os.path.join(results_path, f"{model_name}_feature_cols.txt")
    try:
        with open(feats_file, "w") as f:
            for col in feature_cols:
                f.write(f"{col}\n")
        logger.info(f"Lista de features guardada en: {feats_file}")
    except Exception as e:
        logger.error(f"Error al guardar la lista de features: {e}")

# ============================ FUNCIÓN PRINCIPAL ============================
def main():
    try:
        # 1) Cargar
        df = load_data(CSV_PATH)
        # 2) Indicadores técnicos
        df = add_technical_indicators(df)
        # 3) Generar target binario
        df = generate_target_binary(df, look_ahead=FIXED_LOOK_AHEAD, min_change=FIXED_MIN_CHANGE)
        # 4) Split train / test
        split_idx = int(len(df) * (1 - TEST_SIZE))
        df_train = df.iloc[:split_idx].copy()
        df_test = df.iloc[split_idx:].copy()
        logger.info("División de datos en Train/Test completada.")
        # 5) StratifiedKFold
        tscv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_SEED)
        # 6) Muestra para Optuna
        logger.info("=== Búsqueda de hiperparámetros (Optuna) en muestra de Training ===")
        train_sample_size = int(len(df_train) * SAMPLE_FRAC)
        df_train_sample = df_train.iloc[:train_sample_size].copy()
        # 7) Features
        feature_cols = FIXED_FEATURES.copy()
        # 8) Detectar características categóricas
        categorical_features = []
        for col in feature_cols:
            if str(df_train_sample[col].dtype) in ["object", "category"]:
                categorical_features.append(col)
        if categorical_features:
            logger.warning(f"Características categóricas detectadas: {categorical_features}")
        else:
            logger.info("No se han identificado características categóricas.")
        # 9) Preparar dataset (muestra)
        X_train_sample, y_train_sample = prepare_dataset(df_train_sample, feature_cols, label_col="target")
        # 10) Optimización con Optuna (precisión clase 1)
        best_params = train_models_optuna_precision(
            X_train_sample, y_train_sample, tscv, categorical_features,
            n_trials=N_TRIALS, n_jobs=N_JOBS_OPTUNA
        )
        # 11) Entrenar pipeline final con TODO el training
        logger.info("=== Entrenando Pipeline Final con TODO el Training ===")
        X_train_full, y_train_full = prepare_dataset(df_train, feature_cols, label_col="target")
        X_train_full_bal, y_train_full_bal = balance_classes_smote(X_train_full, y_train_full)
        final_pipeline = train_final_pipeline_with_params(
            X_train_full_bal, y_train_full_bal,
            best_params,
            use_pca=USE_PCA,
            pca_n_components=PCA_N_COMPONENTS,
            categorical_features=categorical_features,
            factor_pos_weight=1.0  # Ajusta aquí si deseas favorecer más la clase 1
        )
        # 12) Guardar modelo y resultados
        save_model_and_results(
            final_pipeline, best_params, feature_cols,
            model_name=MODEL_NAME, results_path=RESULTS_PATH,
            alternative_save_path="./backup_results"
        )
        # 13) Preparar dataset para Test (mismo formato)
        if not isinstance(df_test.index, pd.DatetimeIndex):
            logger.info("Convirtiendo df_test a DatetimeIndex...")
            df_test = df_test.copy()
            if "open_time" in df_test.columns:
                df_test.set_index("open_time", inplace=True)
            else:
                logger.error("No se encuentra 'open_time' en df_test.")
                sys.exit(1)
            df_test.sort_index(inplace=True)
        df_test = add_technical_indicators(df_test)
        df_test = generate_target_binary(df_test, look_ahead=FIXED_LOOK_AHEAD, min_change=FIXED_MIN_CHANGE)
        X_test_final, y_test_final = prepare_dataset(df_test, feature_cols, label_col="target")
        # 14) Evaluar
        logger.info("=== Evaluación en Test ===")
        precision_test = evaluate_model(final_pipeline, X_test_final, y_test_final)
        logger.info(f"Precisión final en Test (clase=1): {precision_test:.4f}")
        # 15) Analizar importancia de features
        analyze_feature_importance(final_pipeline, feature_cols)
        # 16) SHAP (opcional)
        analyze_feature_importance_shap(final_pipeline, X_test_final)
        # 17) Generar señales
        raw_signals = generate_signals(final_pipeline, X_test_final, threshold_high=PROB_THRESHOLD_HIGH)
        # 18) Filtrar señales por tendencia (usando SMA_50 y SMA_200, según tu código actual)
        filtered_signals = filter_signals_by_trend(df_test, raw_signals)
        # Guardar señales en CSV
        signals_df = pd.DataFrame({
            'raw_signal': raw_signals,
            'filtered_signal': filtered_signals
        }, index=df_test.index)
        signals_file = os.path.join(RESULTS_PATH, "signals_binary.csv")
        signals_df.to_csv(signals_file)
        logger.info(f"Señales guardadas en: {signals_file}")
        logger.info("=== Proceso Completado con Éxito ===")
    except Exception as e:
        logger.error(f"Error en main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
