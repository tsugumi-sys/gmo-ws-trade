import websockets
import asyncio
import json
import logging
from dotenv import load_dotenv
import queue
from multiprocessing import Pool

from gmo_ws import GmoWebsocket
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
                    raise ConnectionFailedError
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
                    raise ConnectionFailedError
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


async def manage_queue(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
):
    while True:
        try:
            with SessionLocal() as db:
                # Save orderbook queue
                while True:
                    try:
                        item = gmo_ws.get_orderbook_queue()
                        crud.insert_board_items(db=db, insert_items=item, max_board_counts=max_orderbook_table_rows)
                    except queue.Empty:
                        break

                # Save ticks queue
                while True:
                    try:
                        item = gmo_ws.get_ticks_queue()
                        crud.insert_tick_item(db=db, insert_item=item, max_rows=max_tick_table_rows)
                    except queue.Empty:
                        break

                # Create ohlcv
                crud.create_ohlcv_from_ticks(db=db, symbol=symbol, time_span=time_span, max_rows=max_ohlcv_table_rows)
            await asyncio.sleep(0.0)
        except asyncio.TimeoutError:
            logger.info("Trade thread has ended with asyncio.TimeoutError")
            raise ConnectionFailedError


async def trade(symbol: str):
    while True:
        try:
            with SessionLocal() as db:
                # Get Best bid & best ask
                buy_board_items, sell_board_items = crud.get_current_board(db=db, symbol=symbol)

                # Get ohlcv
                ohlcv = crud.get_ohlcv_with_symbol(db=db, symbol=symbol, limit=1, ascending=False)
            best_bid, best_ask = buy_board_items[-1], sell_board_items[0]
            current_ohlcv = ohlcv[0]
            logger.info(f"Best Ask (price, size): ({best_ask.price}, {best_ask.size})")
            logger.info(f"Best Bid (price, size): ({best_bid.price}, {best_bid.size})")
            logger.info(f"Current ohlcv: {current_ohlcv}")
            await asyncio.sleep(0.0)
        except asyncio.TimeoutError:
            logger.info("Trade thread has ended with asyncio.TimeoutError")
            raise ConnectionFailedError


async def run_websockets(symbol: str):
    await asyncio.gather(
        orderbook_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol),
        # tick_ws(ws_url="wss://api.coin.z.com/ws/public/v1", symbol=symbol),
    )


async def run_manage_queue_and_trading(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
):
    await asyncio.gather(
        manage_queue(
            symbol=symbol,
            time_span=time_span,
            max_orderbook_table_rows=max_orderbook_table_rows,
            max_tick_table_rows=max_tick_table_rows,
            max_ohlcv_table_rows=max_ohlcv_table_rows,
        ),
        trade(symbol=symbol),
    )


def websocket_threads(symbol: str):
    try:
        # Initialize sqlite3 in-memory database
        models.Base.metadata.create_all(engine)
        asyncio.run(run_websockets(symbol=symbol))
    except ConnectionFailedError:
        # bybit_ws.is_db_refreshed = False
        # Clear in-memory DB
        models.Base.metadata.drop_all(engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(engine)

        logger.info("Rerun websockets_threads")
        asyncio.run(websocket_threads(symbol=symbol))

    # Clear in-memory DB again for next try
    models.Base.metadata.drop_all(engine)
    # bybit_ws.is_db_refreshed = False


def manage_queue_and_trading_threads(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
):
    try:
        asyncio.run(
            run_manage_queue_and_trading(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
            )
        )
    except ConnectionFailedError:
        logger.info("Rerun manage_queue_and_trading_threads")
        asyncio.run(
            manage_queue_and_trading_threads(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
            )
        )


def main():
    symbol = "BTCUSDT"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000

    websockets_process = Pool.apply(target=websocket_threads, args=(symbol,))
    websockets_process.start()

    # # wait for creating table
    # manage_queue_and_trading_process = Process(
    #     target=manage_queue_and_trading_threads,
    #     args=(
    #         symbol,
    #         time_span,
    #         max_orderbook_table_rows,
    #         max_tick_table_rows,
    #         max_ohlcv_table_rows,
    #     ),
    # )
    # manage_queue_and_trading_process.start()

    websockets_process.join()
    # manage_queue_and_trading_process.join()


if __name__ == "__main__":
    while True:
        main()
