import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
import os

# Configuración para Binance Testnet
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "osHkEG6MAGsJNFj6XzBcdz3eiR7okPbxuM6YUvtGWNM9oYDdIicywOc1b6osyQB1")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "0jhitqOcBeOnebUiNJNyw9Gx5EXp9X4PffUFHomiViANGk6ZOWgtNPh0LofQ3BzH")

# Endpoint para Binance Testnet
BASE_URL = "https://testnet.binance.vision/api"

# Otras configuraciones
LOG_LEVEL = "INFO"  # Cambiar a DEBUG para más detalles
