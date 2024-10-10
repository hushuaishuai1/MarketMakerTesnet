import backtrader as bt
import pandas as pd
import joblib

class MLStrategy(bt.Strategy):
    params = (
        ('model_filename', 'random_forest_model.joblib'),  # Modelo de ML entrenado
        ('scaler_filename', 'scaler.joblib'),  # Escalador para normalización
        ('features', []),  # Lista de características esperadas por el modelo
    )
    
    def __init__(self):
        # Cargar el modelo entrenado y el scaler
        self.model = joblib.load(self.p.model_filename)
        self.scaler = joblib.load(self.p.scaler_filename)
        self.dataclose = self.datas[0].close
        
        # Inicializar indicadores que coincidan con las características del modelo
        self.ema_fast = bt.indicators.EMA(self.datas[0], period=8)
        self.ema_slow = bt.indicators.EMA(self.datas[0], period=35)
        self.rsi = bt.indicators.RSI(self.datas[0], period=14)
        self.atr = bt.indicators.ATR(self.datas[0], period=14)
        self.macd = bt.indicators.MACD(self.datas[0])
        self.boll = bt.indicators.BollingerBands(self.datas[0], period=20)
        self.momentum = bt.indicators.Momentum(self.datas[0], period=10)
        self.volatility = bt.indicators.StdDev(self.datas[0], period=10)
    
    def next(self):
        # Recolectar características en un solo punto de datos
        data_point = {
            'close': self.dataclose[0],
            'open': self.datas[0].open[0],
            'high': self.datas[0].high[0],
            'low': self.datas[0].low[0],
            'volume': self.datas[0].volume[0],
            'ema_fast': self.ema_fast[0],
            'ema_slow': self.ema_slow[0],
            'rsi': self.rsi[0],
            'atr': self.atr[0],
            'macd': self.macd.macd[0],
            'macd_signal': self.macd.signal[0],
            'macd_hist': self.macd.hist[0],
            'upper_band': self.boll.top[0],
            'middle_band': self.boll.mid[0],
            'lower_band': self.boll.bot[0],
            'momentum': self.momentum[0],
            'volatility': self.volatility[0],
        }
        
        # Crear un DataFrame con las características y columnas esperadas por el modelo
        df_live = pd.DataFrame([data_point], columns=self.p.features)
        
        # Normalizar las características usando el escalador
        X_scaled = self.scaler.transform(df_live)
        
        # Hacer predicción
        prediction = self.model.predict(X_scaled)
        
        # Generar señales de trading en base a la predicción
        if prediction[0] == 1 and not self.position:
            self.buy()  # Compra si predice subida y no hay posición abierta
        elif prediction[0] == 0 and self.position:
            self.sell()  # Vende si predice baja y hay una posición abierta
