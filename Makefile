.PHONY run:
run:
	poetry run python ./gmo_websocket/connect.py

.PHONY test:
test:
	poetry run python -m unittest -v