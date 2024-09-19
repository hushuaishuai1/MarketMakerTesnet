from api.binance_connector import get_historical_data

if __name__ == "__main__":
    symbol = 'BTCUSDT'  # Define el par de criptomonedas que deseas
    interval = '15m'  # Define el intervalo de tiempo (ej. '15m', '1h')
    limit = 100  # Número de velas que quieres obtener

    # Llamar a la función para obtener datos históricos
    df = get_historical_data(symbol, interval, limit)

    # Mostrar las últimas filas del DataFrame
    print(df.tail())
