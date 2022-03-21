import websockets
import asyncio
import json


class TestTrader:
    RUNNING = True

    async def trade(self):
        async with websockets.connect(uri="ws://localhost:8001/", ping_timeout=1.0) as ws:
            message = {"channel": "orderbook"}
            topic = json.dumps(message)
            await ws.send(topic)

            while self.RUNNING:
                res = await ws.recv()
                res = json.loads(res)


if __name__ == "__main__":
    test_trader = TestTrader()
    asyncio.run(test_trader.trade())
