import unittest
import sys
from dateutil import parser

from tests.utils import test_engine, SessionLocal
from tests import response_schemas

sys.path.append("./gmo-websocket/")
from gmo_websocket.db import crud, models, schemas


class TestCrudOHLCV(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"

    def setUp(self) -> None:
        models.Base.metadata.create_all(test_engine)
        # Create dummy tick data.
        dummy_tick_items = [
            response_schemas.TickResponseItem(
                channel="trades",
                price="200",
                side="BUY",
                size="0.1",
                timestamp="2020-01-01T00:00:00.900Z",
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="200",
                side="SELL",
                size="0.1",
                timestamp="2020-01-01T00:00:01.900Z",
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="100",
                side="BUY",
                size="0.1",
                timestamp="2020-01-01T00:00:02.100Z",
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="100",
                side="SELL",
                size="0.1",
                timestamp="2020-01-01T00:00:03.900Z",
                symbol=self.dummy_symbol,
            ),
        ]

        with SessionLocal() as db:
            for item in dummy_tick_items:
                crud.insert_tick_item(db=db, insert_item=item.dict())

    def tearDown(self) -> None:
        models.Base.metadata.drop_all(test_engine)

    def test_count_ohlcv(self):
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=5)

            rows = crud._count_ohlcv(db=db)

        self.assertEqual(rows, 1)

    def test_check_if_ohlcv_stored(self):
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=5)

            timestamp = parser.parse("2020-01-01T00:00:00.900Z").timestamp() * 1000
            timestamp = timestamp // (5 * 1000)
            is_stored = crud._check_if_ohclv_stored(db=db, timestamp=timestamp)

        self.assertTrue(is_stored)

    def test_get_ohlcv_with_symbol(self):
        with SessionLocal() as db:
            time_span = 2
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)
            with self.subTest("Raise ValueError if limit is less than 1"):
                with self.assertRaises(ValueError):
                    _ = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=0)

            with self.subTest("If ascending of time is True"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, ascending=True)

                timestamp = parser.parse("2020-01-01T00:00:00.900Z").timestamp() * 1000
                timestamp = timestamp // (time_span * 1000)

                self.assertEqual(res[0].timestamp, timestamp)

            with self.subTest("If ascending of time is False"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, ascending=False)

                timestamp = parser.parse("2020-01-01T00:00:02.100Z").timestamp() * 1000
                timestamp = timestamp // (time_span * 1000)

                self.assertEqual(res[0].timestamp, timestamp)

            with self.subTest("If limit is one"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1)

                self.assertEqual(len(res), 1)

    def test_get_ohlcv(self):
        with SessionLocal() as db:
            time_span = 2
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)

            with self.subTest("Raise Value error when limit is less than 0"):
                with self.assertRaises(ValueError):
                    _ = crud.get_ohlcv(db=db, limit=0)

            with self.subTest("If ascenging of time is True"):
                res = crud.get_ohlcv(db=db, ascending=True)

                timestamp = parser.parse("2020-01-01T00:00:00.900Z").timestamp() * 1000
                timestamp = timestamp // (time_span * 1000)

                self.assertEqual(res[0].timestamp, timestamp)

            with self.subTest("If ascending of time is False"):
                res = crud.get_ohlcv(db=db, ascending=False)

                timestamp = parser.parse("2020-01-01T00:00:02.100Z").timestamp() * 1000
                timestamp = timestamp // (time_span * 1000)

                self.assertEqual(res[0].timestamp, timestamp)

            with self.subTest("If limit is one"):
                res = crud.get_ohlcv(db=db, limit=1)

                self.assertEqual(len(res), 1)

    def test_insert_ohlcv_items(self):
        time_span = 2
        timestamp = parser.parse("2020-01-01T00:00:10.000Z").timestamp() * 1000
        timestamp = timestamp // (time_span * 1000)
        insert_items = [
            schemas.OHLCV(timestamp=timestamp, open=50.0, high=60.0, low=20.0, close=30.0, volume=15.0, symbol=self.dummy_symbol),
        ]
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=2)
            crud.insert_ohlcv_items(db=db, insert_items=insert_items, max_rows=1)

            rows = crud._count_ohlcv(db=db)

        self.assertEqual(rows, 1)

    def test_update_ohlcv_items(self):
        time_span = 1
        timestamp = parser.parse("2020-01-01T00:00:00.900Z").timestamp() * 1000
        timestamp = timestamp // (time_span * 1000)

        update_items = [schemas.OHLCV(timestamp=timestamp, open=0.0, high=1.0, low=0.0, close=0.5, volume=2.0, symbol=self.dummy_symbol)]
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)

            crud.update_ohlcv_items(db=db, update_items=update_items)

            res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1, ascending=True)

        self.assertEqual(res[0].open, 0.0)
        self.assertEqual(res[0].close, 0.5)

    def test_delete_ohlcv_items(self):
        time_span = 1
        timestamp = parser.parse("2020-01-01T00:00:00.900Z").timestamp() * 1000
        timestamp = timestamp // (time_span * 1000)
        delete_items = [{"timestamp": timestamp}]

        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)
            before_rows = crud._count_ohlcv(db=db)
            crud.delete_ohlcv_items(db=db, delete_items=delete_items)
            after_rows = crud._count_ohlcv(db=db)

            res = crud.get_ohlcv(db=db, limit=1, ascending=True)

        self.assertEqual(before_rows, 4)
        self.assertEqual(after_rows, 3)

        valid_timestamp = parser.parse("2020-01-01T00:00:01.900Z").timestamp() * 1000
        valid_timestamp = valid_timestamp // (time_span * 1000)
        self.assertEqual(res[0].timestamp, valid_timestamp)

    def test_create_ohlcv_from_ticks(self):
        time_span = 1
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span, max_rows=1)
            rows = crud._count_ohlcv(db)
            res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1)

        self.assertEqual(rows, 1)
        valid_timestamp = parser.parse("2020-01-01T00:00:03.900Z").timestamp() * 1000
        valid_timestamp = valid_timestamp // (time_span * 1000)
        self.assertEqual(res[0].timestamp, valid_timestamp)
