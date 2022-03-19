import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def cum_return_plot(ax: plt.Axes, cum_return: pd.Series, backtest_type: str) -> None:
    sns.lineplot(y=cum_return.to_numpy(), x=cum_return.index, ax=ax)
    ax.set_title(f"{backtest_type}  Cumulative Return")


def position_change_plot(ax: plt.Axes, position: pd.Series, backtest_type: str) -> None:
    sns.lineplot(y=position.to_numpy(), x=position.index, ax=ax)
    ax.set_title(f"{backtest_type}  Position Change")


def position_change_average_plot(ax: plt.Axes, position: pd.Series, backtest_type: str, rolling_period: int = 10):
    sns.lineplot(y=position.rolling(rolling_period).mean().to_numpy(), x=position.index, ax=ax)
    ax.set_title(f"{backtest_type}  Position Change Average")
