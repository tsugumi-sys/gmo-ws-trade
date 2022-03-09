import sys
import matplotlib.pyplot as plt

# Avoid AttributeError: module 'sqlalchemy' has no attribute 'orm'
import sqlalchemy.orm  # noqa: F401

sys.path.append(".")
from gmo_hft_bot.db.database import SessionLocal
from gmo_hft_bot.db import crud
from backtest.visualize.ohlcv import ohlcv_plot
from backtest.utils.utils import get_ohlcv_df, get_predict_df


def main():
    symbol = "BTC_JPY"

    with SessionLocal() as db:
        ohlcv_data = crud.get_ohlcv_with_symbol(db=db, symbol=symbol)
        predict_data = crud.get_predict_items(db=db, symbol=symbol)

    # fig, axes = plt.subplots(2, 1, figsize=(16, 8))
    # axes = axes.flatten()

    ohlcv_df = get_ohlcv_df(ohlcv_data, time_span=5)
    predict_df = get_predict_df(predict_data)

    _, ax = plt.subplots(figsize=(16, 8))
    ohlcv_plot(ax, ohlcv_df)
    plt.show()


if __name__ == "__main__":
    main()
