from typing import Optional, Tuple
import multiprocessing
import websockets
import asyncio
import json
import logging

from gmo_hft_bot.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot.utils.custom_exceptions import ConnectionFailedError
from gmo_hft_bot.utils.logger_utils import LOGGER_FORMAT, worker_configurer


async def orderbook_ws(ws_url: str, symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        ws.logger.info("Start orderbook")
        # Subscribe board topic
        message = {"command": "subscribe", "channel": "orderbooks", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)
        ws.logger.info("Orderbook subscribed!")

        while True:
            ws.logger.debug("Running Orderbook websockets")
            ws.logger.debug(f"Orderbook queue count: {queue_and_trade_manager.get_orderbook_queue_size()}")
            try:
                # Get data
                res = await ws.recv()
                res = json.loads(res)
                if "error" in list(res.keys()):
                    if "Invalid request parameter" in res["error"]:
                        raise ValueError(f"Invalid request parameter sybol={symbol}")
                    else:
                        ws.logger.error(f"Error response: {res}")
                else:
                    queue_and_trade_manager.add_orderbook_queue(res)

                await asyncio.sleep(0.0)
            except websockets.exceptions.ConnectionClosed:
                ws.logger.error("Public websocket connection has been closed.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError

            except asyncio.TimeoutError:
                ws.logger.error("Time out for sending to pubic websocket api.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError


async def tick_ws(ws_url: str, symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        ws.logger.info("Start Ticks")
        # Subscribe board topic
        await asyncio.sleep(1.0)  # Avoid too many requests.

        message = {"command": "subscribe", "channel": "trades", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)

        ws.logger.info("Ticks subsribed!!")

        while True:
            ws.logger.debug("Running Tick websockets")
            ws.logger.debug(f"Ticks queue count: {queue_and_trade_manager.get_ticks_queue_size()}")
            try:
                # Get data
                res = await ws.recv()
                res = json.loads(res)
                if "error" in list(res.keys()):
                    if "Invalid request parameter" in res["error"]:
                        raise ValueError(f"Invalid request parameter sybol={symbol}")
                    else:
                        ws.logger.error(f"Error response: {res}")
                        continue
                else:
                    queue_and_trade_manager.add_ticks_queue(res)

                await asyncio.sleep(0.0)
            except websockets.exceptions.ConnectionClosed:
                ws.logger.error("Public websocket connection has been closed.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError

            except asyncio.TimeoutError:
                ws.logger.error("Time out for sending to pubic websocket api.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError


async def run_multiple_websockets(symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
    await asyncio.gather(
        orderbook_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager),
        tick_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager),
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
        logging.getLogger().addHandler(logging.FileHandler("./log/main_process.log", "a"))

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
    queue_and_trade_manager = QueueAndTradeManager()
    logging_level = logging.DEBUG
    logging.basicConfig(level=logging_level, format=LOGGER_FORMAT)
    logger = logging.getLogger(__name__)
    symbol = "BTC_JPY"
    main(symbol=symbol, queue_and_trade_manager=queue_and_trade_manager, logging_level=logging_level)
