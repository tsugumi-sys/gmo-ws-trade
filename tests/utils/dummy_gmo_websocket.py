import asyncio
import websockets
import json
import multiprocessing


class SubscribeManager:
    def __init__(self) -> None:
        self.is_subscribe_orderbook = False
        self.is_subscribe_tick = False
        self.is_subscribe_error_too_many_request = False

    def subscribe_orderbook(self):
        self.is_subscribe_orderbook = True

    def subscribe_tick(self):
        self.is_subscribe_tick = True

    def unsubscribe_orderbook(self):
        self.is_subscribe_orderbook = False

    def unsubscribe_tick(self):
        self.is_subscribe_tick = False

    def is_subscribed(self) -> bool:
        return self.is_subscribe_orderbook or self.is_subscribe_tick or self.is_subscribe_error_too_many_request

    def is_subscribed_orderbook(self) -> bool:
        return self.is_subscribe_orderbook

    def is_subscribed_tick(self) -> bool:
        return self.is_subscribe_tick

    def subscribe_error_too_many_request(self):
        self.is_subscribe_error_too_many_request = True

    def is_subscribed_error_too_many_request(self) -> bool:
        return self.is_subscribe_error_too_many_request


async def handler(websocket):
    subscribe_manager = SubscribeManager()
    while True:

        if not subscribe_manager.is_subscribed():
            message = await websocket.recv()
            message = json.loads(message)

            if message["channel"] == "orderbooks":
                subscribe_manager.subscribe_orderbook()

            elif message["channel"] == "trades":
                subscribe_manager.subscribe_tick()

            elif message["channel"] == "errorTooManyRequest":
                subscribe_manager.subscribe_error_too_many_request()

            elif message["channel"] == "stopWebsocketServer":
                asyncio.get_event_loop().stop()

            elif message["channel"] == "asyncioTimeout":
                raise asyncio.TimeoutError

            else:
                await websocket.send(json.dumps({"invalid_subscribe_msg": message}))

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
            await asyncio.sleep(1.0)

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
            await asyncio.sleep(1.0)

        elif subscribe_manager.is_subscribed_error_too_many_request():
            await websocket.send(json.dumps({"error": "Request too many"}))
            await asyncio.sleep(1.0)

        else:
            raise ValueError("subscribe failed. check your subscribe message.")


async def main():
    async with websockets.serve(handler, "", 8001):
        await asyncio.Future()


def dummy_gmo_websockt_server():
    asyncio.run(main())


if __name__ == "__main__":
    p = multiprocessing.Process(target=dummy_gmo_websockt_server)
    p.start()

    import time

    time.sleep(5.0)
    p.terminate()
    # asyncio.run(main())
