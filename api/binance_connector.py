from binance.client import Client
import json
import pandas as pd
import os
import requests
# Cargar las claves de API desde config.json
with open('config.json', 'r') as f:
    config = json.load(f)

client = Client(config['BINANCE_API_KEY'], config['BINANCE_API_SECRET'])

def get_historical_data(symbol, interval, limit=1000):
    # Verificar que el símbolo esté en el formato correcto
    if not symbol.isupper() or not isinstance(symbol, str):
        raise ValueError(f"Símbolo inválido: {symbol}")
    
    # URL para la API de Binance
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    
    # Llamada a la API
    response = requests.get(url)
    
    # Verificar que la respuesta es exitosa
    if response.status_code != 200:
        raise ValueError(f"Error al obtener datos de Binance: {response.status_code} - {response.text}")
    
    data = response.json()

    # Verificar que los datos se recibieron correctamente
    if not isinstance(data, list):
        raise ValueError("Error al recibir los datos de Binance")

    # Convertir los datos a un DataFrame
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'quote_asset_volume', 'number_of_trades', 
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])

    # Convertir el timestamp a datetime para facilitar el análisis
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Filtrar columnas no necesarias
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

    return df
