import sys
from typing import List
from datetime import timedelta

import pandas as pd

sys.path.append(".")
from gmo_hft_bot.db import models


def get_ohlcv_df(ohlcv_data: List[models.OHLCV], time_span: int) -> pd.DataFrame:
    """Get ohlcv data as pd.Dataframe

    Args:
        ohlcv_data (_type_): return of query.
        time_span (int): seconds.

    Returns:
        pd.DataFrame: _description_
    """
    data = {"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
    for item in ohlcv_data:
        data["open"].append(item.open)
        data["high"].append(item.high)
        data["low"].append(item.low)
        data["close"].append(item.close)
        data["volume"].append(item.volume)
        data["timestamp"].append(item.timestamp)

    df = pd.DataFrame(data)
    df["timestamp"] *= time_span
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] += timedelta(hours=9)
    df = df.set_index("timestamp")
    df = df.sort_index()
    print(df.head())
    return df


def get_predict_df(predict_data: List[models.PREDICT]):
    data = {"timestamp": [], "side": [], "price": [], "size": [], "predict_value": [], "symbol": []}
    for item in predict_data:
        data["timestamp"].append(item.timestamp)
        data["side"].append(item.side)
        data["price"].append(item.price)
        data["size"].append(item.size)
        data["predict_value"].append(item.predict_value)
        data["symbol"].append(item.symbol)

    df = pd.DataFrame(data)
    df["timestamp"] /= 1000
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] += timedelta(hours=9)
    df = df.set_index("timestamp")
    df = df.sort_index()
    print(df.head())
    return df
