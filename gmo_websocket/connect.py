import logging
from dotenv import load_dotenv

from gmo_ws import GmoWebsocket
import websocket_threads
import queue_and_trade_threads
from utils.logger_utils import LOGGER_FORMAT

# Load .env file
load_dotenv()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOGGER_FORMAT)
    logger = logging.getLogger(__name__)
    gmo_ws = GmoWebsocket()
    symbol = "BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000
    websocket_threads.main(symbol=symbol, gmo_ws=gmo_ws, logger=logger)


if __name__ == "__main__":
    main()
