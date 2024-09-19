import pandas as pd
from dotenv import load_dotenv
load_dotenv()
# rm -rf migrations/
#  aerich init-db
#  aerich init -t database.DATABASE_CONFIG
#  python init_db.py
# conda activate ApiBinance3.10
# uvicorn app.main:app --reload


# curl "http://localhost:8000/momentum/BTCUSDT?interval=1h&limit=1000"
# curl "http://localhost:8000/backtesting/BTCUSDT?interval=1h"
# curl "http://localhost:8000/historical-data/BTCUSDT?interval=1h&limit=1000"
# 