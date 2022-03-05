import asyncio
import websockets
import json
import multiprocessing


class SubscribeManager:
    def __init__(self) -> None:
        self.is_subscribe_orderbook = False
        self.is_subscribe_tick = False
        pass

    def subscribe_orderbook(self):
        self.is_subscribe_orderbook = True

    def subscribe_tick(self):
        self.is_subscribe_tick = True

    def unsubscribe_orderbook(self):
        self.is_subscribe_orderbook = False

    def unsubscribe_tick(self):
        self.is_subscribe_tick = False

    def is_subscribed(self) -> bool:
        return self.is_subscribe_orderbook or self.is_subscribe_tick

    def is_subscribed_orderbook(self) -> bool:
        return self.is_subscribe_orderbook

    def is_subscribed_tick(self) -> bool:
        return self.is_subscribe_tick


async def handler(websocket):
    subscribe_manager = SubscribeManager()
    while True:

        if not subscribe_manager.is_subscribed():
            message = await websocket.recv()
            print(message)
            message = json.loads(message)

            if message["subscribe"] == "orderbook":
                subscribe_manager.subscribe_orderbook()

            elif message["subscribe"] == "tick":
                subscribe_manager.subscribe_tick()

        elif subscribe_manager.is_subscribed_orderbook():
            await websocket.send(
                json.dumps(
                    {
                        "channel": "orderbooks",
                        "asks": [{"price": "455659", "size": "0.1"}, {"price": "455658", "size": "0.2"}],
                        "bids": [{"price": "455665", "size": "0.1"}, {"price": "455655", "size": "0.3"}],
                        "symbol": "BTC",
                        "timestamp": "2018-03-30T12:34:56.789Z",
                    }
                )
            )
            await asyncio.sleep(3.0)

        elif subscribe_manager.is_subscribed_tick():
            await websocket.send(
                json.dumps(
                    {
                        "channel": "trades",
                        "price": "750760",
                        "side": "BUY",
                        "size": "0.1",
                        "timestamp": "2018-03-30T12:34:56.789Z",
                        "symbol": "BTC",
                    }
                )
            )
            await asyncio.sleep(3.0)


async def main():
    async with websockets.serve(handler, "", 8001):
        await asyncio.Future()


def multiprocess_main():
    asyncio.run(main())


if __name__ == "__main__":
    p = multiprocessing.Process(target=multiprocess_main)
    p.start()

    import time

    time.sleep(5.0)
    p.terminate()
    # asyncio.run(main())
