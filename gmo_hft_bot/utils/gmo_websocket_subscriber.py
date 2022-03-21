import json


class GmoWebsocketSubscriber:
    SUBSCRIBE_ORDERBOOKS_MESSAGE = {"command": "subscribe", "channel": "orderbooks", "symbol": None}
    SUBSCRIBE_TRADES_MESSAGE = {"command": "subscribe", "channel": "trades", "symbol": None}

    def subscribe_orderbooks_msg(self, symbol: str) -> str:
        """Subscribe orderbooks channel message

        Args:
            symbol (str): Name of symbol

        Returns:
            str: dumped object
        """
        msg = self.SUBSCRIBE_ORDERBOOKS_MESSAGE
        msg["symbol"] = symbol
        return json.dumps(msg)

    def subscribe_trades_msg(self, symbol: str) -> str:
        """Subsctibe trades channel message

        Args:
            symbol (str): Name of symbol

        Returns:
            str: dumped object
        """
        msg = self.SUBSCRIBE_TRADES_MESSAGE
        msg["symbol"] = symbol
        return json.dumps(msg)
