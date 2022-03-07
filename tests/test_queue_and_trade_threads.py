import unittest
import asyncio
import logging
import sys

from tests.utils.db_utils import test_engine, SessionLocal as TestSessionLocal

sys.path.append("./gmo_websocket/")
from gmo_hft_bot.queue_and_trade_manager import QueueAndTradeManager

from gmo_hft_bot.queue_and_trade_threads import manage_queue
from gmo_hft_bot.db import models


class TestQueueAndTradeThreads(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"
        self.time_span = 5
        self.max_orderbook_table_rows = 1000
        self.max_tick_table_rows = 1000
        self.max_ohlcv_table_rows = 1000
        self.logger = logging.getLogger("TestLogger")

    def setUp(self) -> None:
        models.Base.metadata.create_all(test_engine)

    def tearDown(self) -> None:
        models.Base.metadata.drop_all(test_engine)

    def test_manage_queue(self):
        # Check this! https://deniscapeto.com/2021/03/06/how-to-test-a-while-true-in-python/
        queue_and_trade_manager = QueueAndTradeManager()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            manage_queue(
                symbol=self.dummy_symbol,
                time_span=self.time_span,
                max_orderbook_table_rows=self.max_orderbook_table_rows,
                max_tick_table_rows=self.max_tick_table_rows,
                max_ohlcv_table_rows=self.max_ohlcv_table_rows,
                logger=self.logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=TestSessionLocal,
            )
        )
        loop.close()
        print("unko")


if __name__ == "__main__":
    unittest.main()
