import websockets
import asyncio
import json
import logging
from dotenv import load_dotenv

# from bybit_ws import BybitWebSocket
from db import crud, models
from db.database import SessionLocal, engine
from utils.custom_exceptions import ConnectionFailedError

# Load .env file
load_dotenv()

# Set logging.basicConfig
logging.basicConfig(
    format="%(asctime)s %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# bybit_ws = BybitWebSocket(api_key=os.environ["BYBIT_API_KEY"], api_secret=os.environ["BYBIT_SECRET_KEY"])


async def orderbook_ws(ws_url: str, symbol: str):
    async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
        # Subscribe board topic
        message = {"command": "subscribe", "channel": "orderbooks", "symbol": symbol}
        topic = json.dumps(message)
        await asyncio.wait_for(ws.send(topic), timeout=1.0)

        while True:
            try:
                # Get data
                res = await ws.recv()
                res = json.loads(res)
                with SessionLocal() as db:
                    # Insert data
                    crud.insert_board_items(db=db, insert_items=res, max_board_counts=10)

                    # Get data
                    buy_board_items, sell_board_items = crud.get_current_board(db=db, symbol=symbol)
                    best_bid, best_ask = buy_board_items[-1], sell_board_items[0]

                    ws.logger.info(f"Best Ask (price, size): ({best_ask.price}, {best_ask.size})")
                    ws.logger.info(f"Best Bid (price, size): ({best_bid.price}, {best_bid.size})")
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
                with SessionLocal() as db:
                    # Insert data
                    crud.insert_tick_item(db=db, insert_item=res, max_rows=1000)

                    # Create ohlcv
                    crud.create_ohlcv_from_ticks(db=db, symbol=symbol, max_rows=10)

                    print("Tick:", crud._count_ticks(db=db))
                    print("OHLCV:", crud._count_ohlcv(db=db))

                await asyncio.sleep(0.0)
            except websockets.exceptions.ConnectionClosed:
                ws.logger.error("Public websocket connection has been closed.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError

            except asyncio.TimeoutError:
                ws.logger.error("Time out for sending to pubic websocket api.")
                await asyncio.sleep(0.0)
                raise ConnectionFailedError


async def run_multiple_websockets():
    symbol = "BTC_JPY"
    await asyncio.gather(
        orderbook_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol),
    )


def main():
    try:
        # Initialize sqlite3 in-memory database
        models.Base.metadata.create_all(engine)
        asyncio.run(run_multiple_websockets())
    except ConnectionFailedError:
        # bybit_ws.is_db_refreshed = False
        # Clear in-memory DB
        models.Base.metadata.drop_all(engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(engine)

        logger.info("Reconnect websockets")
        asyncio.run(main())

    # Clear in-memory DB again for next try
    models.Base.metadata.drop_all(engine)
    # bybit_ws.is_db_refreshed = False


if __name__ == "__main__":
    while True:
        main()
