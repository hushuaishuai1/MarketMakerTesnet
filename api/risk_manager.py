import talib as ta

def calculate_atr(df, period=14):
    """
    Calcula el Average True Range (ATR) para el manejo de riesgo dinámico.
    """
    df['ATR'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=period)
    return df['ATR'].iloc[-1]

def set_stop_loss(price, atr_value):
    """
    Define un stop-loss dinámico basado en el valor de ATR.
    """
    return price - (1.5 * atr_value)

