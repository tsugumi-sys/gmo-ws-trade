import sys
import os
from typing import Optional, Tuple
import multiprocessing
import asyncio
import logging
from dotenv import load_dotenv

sys.path.append(".")
from gmo_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot.utils.custom_exceptions import ConnectionFailedError
from gmo_hft_bot.utils.logger_utils import LOGGER_FORMAT, worker_configurer
from gmo_hft_bot.threads.connect_orderbook_ws import ConnectOrderbookWs
from gmo_hft_bot.threads.connect_tick_ws import ConnectTickWs


async def run_multiple_websockets(symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
    connect_orderbook_ws = ConnectOrderbookWs()
    connect_tick_ws = ConnectTickWs()
    await asyncio.gather(
        connect_orderbook_ws.run(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager),
        connect_tick_ws.run(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager),
    )


def main(
    symbol: str,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Optional[Tuple[str, int]] = None,
    logging_queue: Optional[multiprocessing.Queue] = None,
):
    """Websocket Threads

    Args:
        symbol (str): Name of symbol
        queue_and_trade_manager (QueueAndTradeManager): Queue manager of gmo websocket
        logging_level (Optional[Tuple[str, int]], optional): Logging level. Defaults to None.
        logging_queue (Optional[multiprocessing.Queue], optional): Logger Queue. Defaults to None.

        [TODO]: Raise error if you use logging queue.
    """
    if logging_queue is None:
        logger = logging.getLogger("WebsocketThredsLogger")
        formatter = logging.Formatter(LOGGER_FORMAT)
        file_handler = logging.FileHandler("./log/main_process.log", "a")
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)

        if logging_level is not None:
            logger.setLevel(logging_level)

    else:
        logger = logging.getLogger("WebsocketThredsLogger")
        worker_configurer(logging_queue, logger.getEffectiveLevel())

        if logging_level is not None:
            logger.setLevel(logging_level)

    try:
        asyncio.run(run_multiple_websockets(symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager))
    except ConnectionFailedError:
        logger.debug("Reconnect websockets")
        asyncio.run(main(symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager))


if __name__ == "__main__":
    load_dotenv()
    queue_and_trade_manager = QueueAndTradeManager(api_key=os.environ["EXCHANGE_API_KEY"], api_secret=os.environ["EXCHANGE_API_SECRET"])
    logging_level = logging.DEBUG
    logging.basicConfig(level=logging_level, format=LOGGER_FORMAT)
    logger = logging.getLogger(__name__)
    symbol = "BTC_JPY"
    main(symbol=symbol, queue_and_trade_manager=queue_and_trade_manager, logging_level=logging_level)
