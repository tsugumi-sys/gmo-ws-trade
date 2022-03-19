import os
import sys
import asyncio
import logging
import multiprocessing
import queue
from typing import Optional, Tuple
import time

from dotenv import load_dotenv
import sqlalchemy
import aiohttp

sys.path.append(".")
from gmo_hft_bot.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot.db import crud, models
from gmo_hft_bot.db.database import initialize_database
from gmo_hft_bot.utils.custom_exceptions import ConnectionFailedError
from gmo_hft_bot.utils.logger_utils import LOGGER_FORMAT, worker_configurer


async def manage_queue(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    logger: logging.Logger,
    queue_and_trade_manager: QueueAndTradeManager,
    SessionLocal: sqlalchemy.orm.Session,
):
    while True:
        try:
            with SessionLocal() as db:
                # Save orderbook queue
                while True:
                    try:
                        item = queue_and_trade_manager.get_orderbook_queue_item()

                        logger.debug("Add orderbook queue item to DB")
                        crud.insert_board_items(db=db, insert_items=item, max_board_counts=max_orderbook_table_rows)
                    except queue.Empty:
                        break

                # Save ticks queue
                while True:
                    try:
                        item = queue_and_trade_manager.get_ticks_queue_item()

                        logger.debug("Add tick queue item to DB")
                        crud.insert_tick_item(db=db, insert_item=item, max_rows=max_tick_table_rows)
                    except queue.Empty:
                        break

                # Create ohlcv
                crud.create_ohlcv_from_ticks(db=db, symbol=symbol, time_span=time_span, max_rows=max_ohlcv_table_rows)
            await asyncio.sleep(0.0)
        except asyncio.TimeoutError:
            logger.debug("Trade thread has ended with asyncio.TimeoutError")
            raise ConnectionFailedError


async def trade(symbol: str, trade_time_span: int, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager, SessionLocal: sqlalchemy.orm.Session):
    """Trade threads

    Args:
        symbol (str): Name of symbol
        trade_time_span (int): Time span of trade. seconds
        logger (logging.Logger): logger
        queue_and_trade_manager (QueueAndTradeManager): Quene and trade manager
        SessionLocal (sqlalchemy.orm.Session): Session of sqlalchemy

    Raises:
        ConnectionFailedError: Raise if threads stopped.
    """
    before_timestamp_per_span = None
    while True:
        try:
            current_timestamp_per_span = time.time() // trade_time_span
            if before_timestamp_per_span is not None and current_timestamp_per_span > before_timestamp_per_span:

                with SessionLocal() as db:
                    predict_info = crud.get_prediction_info(db=db, symbol=symbol)
                    ohlcv_count = crud._count_ohlcv(db)
                print(ohlcv_count)

                if predict_info.buy is True:
                    # Buy
                    logger.info("Buy order.")
                    # Dummy order
                    request_url, headers = queue_and_trade_manager.test_http_private_request_args()
                    async with aiohttp.ClientSession() as session:
                        async with session.get(request_url, headers=headers) as response:
                            _ = await response.json()

                if predict_info.sell is True:
                    # Sell
                    logger.info("Sell order")
                    # Dummy order
                    request_url, headers = queue_and_trade_manager.test_http_private_request_args()
                    async with aiohttp.ClientSession() as session:
                        async with session.get(request_url, headers=headers) as response:
                            _ = await response.json()

                with SessionLocal() as db:
                    buy_predict_item = {
                        "side": "BUY",
                        "size": predict_info.buy_size,
                        "price": predict_info.buy_price,
                        "predict_value": predict_info.buy_predict_value,
                        "symbol": symbol,
                    }
                    sell_predict_item = {
                        "side": "SELL",
                        "size": predict_info.sell_size,
                        "price": predict_info.sell_price,
                        "predict_value": predict_info.sell_predict_value,
                        "symbol": symbol,
                    }
                    crud.insert_predict_items(db=db, insert_items=[buy_predict_item, sell_predict_item])

            before_timestamp_per_span = current_timestamp_per_span

            await asyncio.sleep(0.0)
        except asyncio.TimeoutError:
            logger.debug("Trade thread has ended with asyncio.TimeoutError")
            raise ConnectionFailedError


async def run_manage_queue_and_trading(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    logger: logging.Logger,
    queue_and_trade_manager: QueueAndTradeManager,
    SessionLocal: sqlalchemy.orm.Session,
):
    await asyncio.gather(
        manage_queue(
            symbol=symbol,
            time_span=time_span,
            max_orderbook_table_rows=max_orderbook_table_rows,
            max_tick_table_rows=max_tick_table_rows,
            max_ohlcv_table_rows=max_ohlcv_table_rows,
            logger=logger,
            queue_and_trade_manager=queue_and_trade_manager,
            SessionLocal=SessionLocal,
        ),
        trade(symbol=symbol, trade_time_span=time_span, logger=logger, queue_and_trade_manager=queue_and_trade_manager, SessionLocal=SessionLocal),
    )


def main(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    database_uri: Optional[str] = None,
    logging_level: Optional[Tuple[str, int]] = None,
    logging_queue: Optional[multiprocessing.Queue] = None,
):
    if logging_queue is None:
        logger = logging.getLogger("QueueAndTradeLogger")

        if logging_level is not None:
            logger.setLevel(logging_level)

    else:
        logger = logging.getLogger("QueueAndTradeLogger")
        worker_configurer(logging_queue, logger.getEffectiveLevel())

        if logging_level is not None:
            logger.setLevel(logging_level)

    # Initialize database
    database_engine, SessionLocal = initialize_database(uri=database_uri)

    try:
        # Initialize sqlite3 in-memory database
        models.Base.metadata.create_all(database_engine)
        asyncio.run(
            run_manage_queue_and_trading(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )
    except ConnectionFailedError:
        # bybit_ws.is_db_refreshed = False
        # Clear in-memory DB
        models.Base.metadata.drop_all(database_engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(database_engine)

        logger.debug("Rerun websockets_threads")
        asyncio.run(
            main(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )

    # Clear in-memory DB again for next try
    models.Base.metadata.drop_all(database_engine)


if __name__ == "__main__":
    # Load .env file
    load_dotenv()

    logging.basicConfig(level=logging.DEBUG, format=LOGGER_FORMAT)
    queue_and_trade_manager = QueueAndTradeManager(api_key=os.environ["EXCHANGE_API_KEY"], api_secret=os.environ["EXCHANGE_API_SECRET"])

    database_engine, session_local = initialize_database(uri="sqlite:///example.db")
    symbol = "BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000
    main(
        symbol=symbol,
        time_span=time_span,
        max_orderbook_table_rows=max_orderbook_table_rows,
        max_tick_table_rows=max_tick_table_rows,
        max_ohlcv_table_rows=max_ohlcv_table_rows,
        queue_and_trade_manager=queue_and_trade_manager,
        database_uri="sqlite:///example.db",
    )
