from pydantic import BaseModel
from datetime import datetime

class TradeBase(BaseModel):
    symbol: str
    price: float
    volume: float

class TradeInDB(TradeBase):
    id: int
    timestamp: datetime

    class Config:
        orm_mode = True
