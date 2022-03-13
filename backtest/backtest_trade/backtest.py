import numpy as np
import numba


@numba.njit
def backtest(
    close: np.ndarray,
    predict_buy_entry: np.ndarray,
    predict_sell_entry: np.ndarray,
    priority_buy_entry: np.ndarray,
    buy_executed: np.ndarray,
    sell_executed: np.ndarray,
    buy_price: np.ndarray,
    sell_price: np.ndarray,
):
    n = close.size
    y = close.copy() * 0.0
    poss = close.copy() * 0.0
    ret = 0.0
    pos = 0.0
    buy_entry_price = 1.0
    sell_entry_price = 1.0
    buy_entry_prices = np.full((n), np.nan)
    sell_entry_prices = np.full((n), np.nan)
    buy_exit_prices = np.full((n), np.nan)
    sell_exit_prices = np.full((n), np.nan)
    for i in range(n):
        prev_pos = pos

        # exit
        # Exit of short
        if buy_executed[i] and predict_buy_entry[i]:
            vol = np.maximum(0, -prev_pos)
            if vol == 1:
                buy_exit_price = buy_price[i]
                buy_exit_prices[i] = buy_price[i]
                ret -= (buy_exit_price / sell_entry_price - 1) * vol
                pos += vol

        # Exit of long
        if sell_executed[i] and predict_sell_entry[i]:
            vol = np.maximum(0, prev_pos)
            if vol == 1:
                sell_exit_price = sell_price[i]
                sell_exit_prices[i] = sell_price[i]
                ret += (sell_exit_price / buy_entry_price - 1) * vol
                pos -= vol

        # entry
        if priority_buy_entry[i] and predict_buy_entry[i] and buy_executed[i]:
            vol = np.minimum(1.0, 1 - prev_pos) * predict_buy_entry[i]
            # ret -= buy_cost[i] * vol
            pos += vol
            if vol == 1:
                buy_entry_price = buy_price[i]
                buy_entry_prices[i] = buy_price[i]

        if not priority_buy_entry[i] and predict_sell_entry[i] and sell_executed[i]:
            vol = np.minimum(1.0, prev_pos + 1) * predict_sell_entry[i]
            pos -= vol
            if vol == 1:
                sell_entry_price = sell_price[i]
                sell_entry_prices[i] = sell_price[i]

        y[i] = ret
        poss[i] = pos

    return y, poss, buy_entry_prices, sell_entry_prices, buy_exit_prices, sell_exit_prices
