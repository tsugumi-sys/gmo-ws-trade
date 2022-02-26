import asyncio
import logging
from dotenv import load_dotenv
import queue

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
            print("manage queue")
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

            if len(buy_board_items) > 0 and len(sell_board_items) > 0:
                best_bid, best_ask = buy_board_items[-1], sell_board_items[0]
                current_ohlcv = ohlcv[0]
                logger.info(f"Best Ask (price, size): ({best_ask.price}, {best_ask.size})")
                logger.info(f"Best Bid (price, size): ({best_bid.price}, {best_bid.size})")
                logger.info(f"Current ohlcv: {current_ohlcv}")
            print("trade")
            await asyncio.sleep(0.0)
        except asyncio.TimeoutError:
            logger.info("Trade thread has ended with asyncio.TimeoutError")
            raise ConnectionFailedError


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


def main(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
):
    try:
        # Initialize sqlite3 in-memory database
        models.Base.metadata.create_all(engine)
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
        # bybit_ws.is_db_refreshed = False
        # Clear in-memory DB
        models.Base.metadata.drop_all(engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(engine)

        logger.info("Rerun websockets_threads")
        asyncio.run(
            main(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
            )
        )

    # Clear in-memory DB again for next try
    models.Base.metadata.drop_all(engine)
    # bybit_ws.is_db_refreshed = False


if __name__ == "__main__":
    gmo_ws = GmoWebsocket()
    symbol = "BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000
    while True:
        main(
            symbol=symbol,
            time_span=time_span,
            max_orderbook_table_rows=max_orderbook_table_rows,
            max_tick_table_rows=max_tick_table_rows,
            max_ohlcv_table_rows=max_ohlcv_table_rows,
        )
