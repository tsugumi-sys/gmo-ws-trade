from pydantic import BaseModel
from typing import List


class BidsAsks(BaseModel):
    price: str
    size: str


class BoardResponseItem(BaseModel):
    asks: List[BidsAsks]
    bids: List[BidsAsks]
    symbol: str
    timestamp: str
