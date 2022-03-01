from typing import Optional
import websockets
import asyncio
import json
import logging

from gmo_ws import GmoWebsocket
from utils.custom_exceptions import ConnectionFailedError
from utils.logger_utils import LOGGER_FORMAT


async def orderbook_ws(ws_url: str, symbol: str, logger: logging.Logger, gmo_ws: GmoWebsocket):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        ws.logger.info("Start orderbook")
        # Subscribe board topic
        message = {"command": "subscribe", "channel": "orderbooks", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)
        ws.logger.info("Orderbook subscribed!")

        while True:
            ws.logger.debug("Running Orderbook websockets")
            ws.logger.debug(f"Orderbook queue count: {gmo_ws.get_orderbook_queue_size()}")
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
                    gmo_ws.add_orderbook_queue(res)
                    await asyncio.sleep(0.0)
            except websockets.exceptions.ConnectionClosed:
                ws.logger.error("Public websocket connection has been closed.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError

            except asyncio.TimeoutError:
                ws.logger.error("Time out for sending to pubic websocket api.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError


async def tick_ws(ws_url: str, symbol: str, logger: logging.Logger, gmo_ws: GmoWebsocket):
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
            ws.logger.debug(f"Ticks queue count: {gmo_ws.get_ticks_queue_size()}")
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
                    gmo_ws.add_ticks_queue(res)
                await asyncio.sleep(0.0)
            except websockets.exceptions.ConnectionClosed:
                ws.logger.error("Public websocket connection has been closed.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError

            except asyncio.TimeoutError:
                ws.logger.error("Time out for sending to pubic websocket api.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError


async def run_multiple_websockets(symbol: str, logger: logging.Logger, gmo_ws: GmoWebsocket):
    await asyncio.gather(
        orderbook_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, gmo_ws=gmo_ws),
        tick_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol, logger=logger, gmo_ws=gmo_ws),
    )


def main(
    symbol: str,
    gmo_ws: GmoWebsocket,
    logger: Optional[logging.Logger] = None,
):
    """Websockets thread

    Args:
        symbol (str): Name of symbol
        gmo_ws (GmoWebsocket): GmoWebsocket class.
        logger (Optional[logging.Logger], optional): Pass logger. Defaults to None.
    """
    if logger is None:
        logger = logging.getLogger("WebsocketThreadsLogger")

    try:
        asyncio.run(run_multiple_websockets(symbol=symbol, logger=logger, gmo_ws=gmo_ws))
    except ConnectionFailedError:
        logger.info("Reconnect websockets")
        asyncio.run(main(symbol=symbol, logger=logger, gmo_ws=gmo_ws))


if __name__ == "__main__":
    gmo_ws = GmoWebsocket()
    logging.basicConfig(level=logging.DEBUG, format=LOGGER_FORMAT)
    logger = logging.getLogger(__name__)
    symbol = "BTC_JPY"
    # while True:
    main(symbol=symbol, logger=logger, gmo_ws=gmo_ws)
