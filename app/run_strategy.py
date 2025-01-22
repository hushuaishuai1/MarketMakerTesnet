import time
import logging
from ML.ml_strategy import run_ml_backtesting

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_strategy_every_5_minutes():
    """
    Ejecuta la estrategia cada 5 minutos de forma continua.
    """
    try:
        while True:
            logging.info("Ejecutando estrategia de trading para BTCUSDT con intervalo de 5 minutos.")
            run_ml_backtesting('BTCUSDT', '5m')  # Ejecuta tu estrategia de trading
            logging.info("Estrategia ejecutada, esperando 5 minutos antes de la próxima ejecución.")
            time.sleep(300)  # Espera 5 minutos (300 segundos)
    except Exception as e:
        logging.error(f"Error durante la ejecución de la estrategia: {e}")

if __name__ == "__main__":
    run_strategy_every_5_minutes()

