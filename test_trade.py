import websockets
import asyncio
import json


async def main():
    async with websockets.connect(uri="ws://localhost:8001/", ping_timeout=1.0) as ws:
        message = {"subscribe": "orderbook"}
        topic = json.dumps(message)
        await ws.send(topic)

        while True:
            res = await ws.recv()
            res = json.loads(res)
            print(res)


if __name__ == "__main__":
    asyncio.run(main())
