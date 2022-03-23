import websockets
import asyncio
import json
import logging
import time
import traceback

from gmo_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from gmo_hft_bot.utils.gmo_websocket_subscriber import GmoWebsocketSubscriber
from gmo_hft_bot.utils.custom_exceptions import ConnectionFailedError


class ConnectTickWs:
    RUNNING = True
    gmo_websocket_subscriber = GmoWebsocketSubscriber()

    async def run(self, ws_url: str, symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
        async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
            ws.logger.info("Start Ticks")
            # Subscribe ticks topic
            subscribe_message = self.gmo_websocket_subscriber.subscribe_trades_msg(symbol=symbol)
            await asyncio.wait_for(ws.send(subscribe_message), timeout=1.0)

            ws.logger.info("Ticks subsribed!!")

            while self.RUNNING:
                ws.logger.debug("Running Tick websockets")
                ws.logger.debug(f"Tick Queue count: {queue_and_trade_manager.get_ticks_queue_size()}")
                try:
                    if queue_and_trade_manager.is_subprocesses_alive() is True:
                        # Get data
                        res = await ws.recv()
                        res = json.loads(res)
                        if "error" in list(res.keys()):
                            if "Invalid request parameter" in res["error"]:
                                raise ValueError(f"Invalid request parameter sybol={symbol}")
                            else:
                                ws.logger.error(f"Error response: {res}")
                                # Try to connect again
                                time.sleep(0.5)
                                await asyncio.wait_for(ws.send(subscribe_message), timeout=1.0)
                        else:
                            queue_and_trade_manager.add_ticks_queue(res)
                    else:
                        msg = "subprocesses are dead."
                        ws.logger.error(msg)
                        raise Exception(msg)
                    await asyncio.sleep(0.0)
                except websockets.exceptions.ConnectionClosed:
                    ws.logger.error("Public websocket connection has been closed.")
                    await asyncio.sleep(0.0)
                    raise ConnectionFailedError

                except asyncio.TimeoutError:
                    ws.logger.error("Time out for sending to pubic websocket api.")
                    await asyncio.sleep(0.0)
                    raise ConnectionFailedError

                except Exception as e:
                    ws.logger.error(traceback.format_exc())
                    ws.logger.error(e)
                    raise ConnectionFailedError
