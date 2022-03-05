import asyncio
import logging
import multiprocessing
import queue
from typing import Optional, Tuple

import sqlalchemy

from gmo_hft_bot.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot.db import crud, models
from gmo_hft_bot.db.database import SessionLocal as SqlSessionLocal, engine
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
        logger.debug("Running manage_queue")
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


async def trade(symbol: str, logger: logging.Logger, SessionLocal: sqlalchemy.orm.Session):
    while True:
        logger.debug("Running trade")
        try:
            with SessionLocal() as db:
                # Get Best bid & best ask
                buy_board_items, sell_board_items = crud.get_current_board(db=db, symbol=symbol)

                # Get ohlcv
                ohlcv = crud.get_ohlcv_with_symbol(db=db, symbol=symbol, limit=1, ascending=False)

            if len(buy_board_items) > 0 and len(sell_board_items) > 0 and len(ohlcv) > 0:
                best_bid, best_ask = buy_board_items[-1], sell_board_items[0]
                current_ohlcv = ohlcv[0]
                logger.debug(f"Best Ask (price, size): ({best_ask.price}, {best_ask.size})")
                logger.debug(f"Best Bid (price, size): ({best_bid.price}, {best_bid.size})")
                logger.debug(
                    f"Current OHLCV open: {current_ohlcv.open}, high: {current_ohlcv.high}, low: {current_ohlcv.low},"
                    f" close: {current_ohlcv.close}, volume: {current_ohlcv.volume}."
                )
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
        trade(symbol=symbol, logger=logger, SessionLocal=SessionLocal),
    )


def main(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    SessionLocal: Optional[sqlalchemy.orm.Session] = None,
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

    if SessionLocal is None:
        SessionLocal = SqlSessionLocal

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
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )
    except ConnectionFailedError:
        # bybit_ws.is_db_refreshed = False
        # Clear in-memory DB
        models.Base.metadata.drop_all(engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(engine)

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
    models.Base.metadata.drop_all(engine)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=LOGGER_FORMAT)
    logger = logging.getLogger(__name__)
    queue_and_trade_manager = QueueAndTradeManager()
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
        logger=logger,
        queue_and_trade_manager=queue_and_trade_manager,
    )
