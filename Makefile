.PHONY run:
run:
	poetry run python ./gmo_hft_bot/connect.py

.PHONY run_websockets:
run_websockets:
	poetry run python ./gmo_hft_bot/websocket_threads.py

.PHONY run_trade:
run_trade:
	poetry run python ./gmo_hft_bot/queue_and_trade_threads.py

.PHONY test:
test:
	poetry run python -m unittest -v

.PHONY run_backtest:
run_backtest:
	poetry run python ./backtest/main.py

.PHONY delete_pycache:
delete_pycache:
	find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf