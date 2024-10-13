from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
import joblib
import pandas as pd
from ML.data_processing import prepare_dataset, label_data

def train_model(symbol: str, interval: str, limit: int = 1000):
    # Preparar los datos
    df = prepare_dataset(symbol, interval, limit)
    df = label_data(df)

    feature_cols = ['ema_12', 'ema_26', 'rsi', 'macd', 'bollinger_hband', 'bollinger_lband']
    X = df[feature_cols]
    y = df['signal']

    # Dividir en entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # Evitar el Look-Ahead Bias al no mezclar los datos temporalmente
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Validación cruzada
    scores = cross_val_score(model, X_train, y_train, cv=5)
    print(f"Cross-validation accuracy scores: {scores}")
    print(f"Mean cross-validation accuracy: {scores.mean()}")

    # Guardar el modelo entrenado
    joblib.dump(model, 'ML/trained_model.pkl')
    print("Modelo entrenado y guardado como 'ML/trained_model.pkl'")

    return model

if __name__ == "__main__":
    # Ejemplo de entrenamiento
    train_model('BTCUSDT', '5m', 5000)
