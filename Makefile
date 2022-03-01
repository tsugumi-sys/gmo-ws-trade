.PHONY run:
run:
	poetry run python ./gmo_websocket/connect.py

.PHONY run_websockets:
run_websockets:
	poetry run python ./gmo_websocket/websocket_threads.py

.PHONY run_trade:
run_trade:
	poetry run python ./gmo_websocket/queue_and_trade_threads.py

.PHONY test:
test:
	poetry run python -m unittest -v