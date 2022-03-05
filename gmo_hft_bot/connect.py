import logging
from typing import Tuple, Optional
import multiprocessing
from dotenv import load_dotenv

from gmo_hft_bot.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot import websocket_threads
from gmo_hft_bot import queue_and_trade_threads
from gmo_hft_bot.utils.logger_utils import LOGGER_FORMAT, listener_configurer, listener_process

# Load .env file
load_dotenv()


def main_process(
    symbol: str,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Tuple[str, int],
    logging_queue: Optional[multiprocessing.Queue] = None,
):
    """Main process

    Args:
        symbol (str): Name of symbol
        queue_and_trade_manager (QueueAndTradeManager): gmo websockets
        logging_level (Tuple[str, int]): Logging level
        logging_queue (multiprocessing.Queue): Logging queue for multiprocessing. Default is None
    """
    websocket_threads.main(symbol=symbol, queue_and_trade_manager=queue_and_trade_manager, logging_level=logging_level, logging_queue=logging_queue)


def sub_processes(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Tuple[str, int],
    logging_queue: multiprocessing.Queue,
) -> Tuple[multiprocessing.Process, multiprocessing.Process]:
    """Sub processes. Logging process and manage_queue_and_trade process.

    Args:
        symbol (str): Name of symbol.
        time_span (int): Time span (seconds).
        max_orderbook_table_rows (int): Number of max orderbook table rows.
        max_tick_table_rows (int): Number of max tick table rows.
        max_ohlcv_table_rows (int): Number of max ohlcv table rows.
        queue_and_trade_manager (QueueAndTradeManager): Manage queue class.
        logging_level (Tuple[str, int]): Logging level.
        logging_queue (multiprocessing.Queue): Queue of multiprocessing.

    Return:
        (logging_process, queue_and_trade_process)
    """
    logging_process = multiprocessing.Process(
        target=listener_process,
        args=(
            logging_queue,
            listener_configurer,
        ),
    )

    queue_and_trade_process = multiprocessing.Process(
        target=queue_and_trade_threads.main,
        args=(
            symbol,
            time_span,
            max_orderbook_table_rows,
            max_tick_table_rows,
            max_ohlcv_table_rows,
            queue_and_trade_manager,
            None,  # SessionLocal
            logging_level,
            logging_queue,
        ),
    )

    return logging_process, queue_and_trade_process


if __name__ == "__main__":
    logging_level = logging.DEBUG
    logging_queue = multiprocessing.Queue(-1)
    logging.basicConfig(level=logging_level, format=LOGGER_FORMAT)

    queue_and_trade_manager = QueueAndTradeManager()
    symbol = "BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000

    logging_process, queue_and_trade_process = sub_processes(
        symbol=symbol,
        time_span=time_span,
        max_orderbook_table_rows=max_orderbook_table_rows,
        max_tick_table_rows=max_tick_table_rows,
        max_ohlcv_table_rows=max_ohlcv_table_rows,
        queue_and_trade_manager=queue_and_trade_manager,
        logging_level=logging_level,
        logging_queue=logging_queue,
    )

    logging_process.start()
    queue_and_trade_process.start()

    main_process(symbol=symbol, queue_and_trade_manager=queue_and_trade_manager, logging_level=logging_level)

    logging_process.join()
    queue_and_trade_process.join()
