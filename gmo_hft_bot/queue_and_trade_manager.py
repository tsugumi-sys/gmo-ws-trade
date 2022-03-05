from typing import Dict

from gmo_hft_bot.utils.custom_queue import CustomQueue


class QueueAndTradeManager:
    def __init__(self) -> None:
        self.enable_trade = False
        self.orderbook_queue = CustomQueue()
        self.ticks_queue = CustomQueue()

    def __del__(self):
        import time

        # Sometime, Broken pipe error raises becase main process finishes faster than Queue.close().
        time.sleep(0.01)

    def _enable_trade(self):
        self.enable_trade = True

    def _disable_trade(self):
        self.enable_trade = False

    def add_orderbook_queue(self, item: Dict):
        self.orderbook_queue.put_nowait(item)

    def get_orderbook_queue_size(self):
        return self.orderbook_queue.qsize()

    def get_orderbook_queue_item(self):
        return self.orderbook_queue.get_nowait()

    def add_ticks_queue(self, item: Dict):
        self.ticks_queue.put_nowait(item)

    def get_ticks_queue_item(self):
        return self.ticks_queue.get_nowait()

    def get_ticks_queue_size(self):
        return self.ticks_queue.qsize()
