import websockets
import asyncio
import json
import logging
from dotenv import load_dotenv

from gmo_ws import GmoWebsocket
from utils.custom_exceptions import ConnectionFailedError

# Load .env file
load_dotenv()

# Set logging.basicConfig
logging.basicConfig(
    format="%(asctime)s %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

gmo_ws = GmoWebsocket()


async def orderbook_ws(ws_url: str, symbol: str):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        # Subscribe board topic
        message = {"command": "subscribe", "channel": "orderbooks", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)

        while True:
            print("Hey Orderook")
            try:
                # Get data
                res = await ws.recv()
                res = json.loads(res)
                # {'error': 'ERR-5003 Request too many.'}
                if "error" in list(res.keys()):
                    ws.logger.error(f"Error response: {res}")
                    # raise ConnectionFailedError
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


async def tick_ws(ws_url: str, symbol: str):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        # Subscribe board topic
        message = {"command": "subscribe", "channel": "trades", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)

        while True:
            try:
                # Get data
                res = await ws.recv()
                res = json.loads(res)
                print("Hey Ticks")
                if "error" in list(res.keys()):
                    ws.logger.error(f"Error response: {res}")
                    # raise ConnectionFailedError
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


async def run_multiple_websockets(symbol: str):
    await asyncio.gather(
        orderbook_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol),
        tick_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol),
    )


def main(symbol: str):
    try:
        asyncio.run(run_multiple_websockets(symbol=symbol))
    except ConnectionFailedError:
        logger.info("Reconnect websockets")
        asyncio.run(main(symbol=symbol))


if __name__ == "__main__":
    symbol = "BTC_JPY"
    while True:
        main(symbol=symbol)
