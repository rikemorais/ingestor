.DEFAULT_GOAL := help
.PHONY: help install sync reinstall run test up stop logs ps wiki btc batch custom micro-batch bridge consume-btc docs-serve docs-build docs-deploy clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install:
	uv sync

sync: install

reinstall:
	uv sync --reinstall-package data-ingestor

run:
	PYTHONPATH=src uv run python -m ingestor

test:
	PYTHONPATH=src uv run python -m unittest discover -v

up:
	docker compose up -d

stop:
	docker compose stop

logs:
	docker compose logs -f kafka

ps:
	docker compose ps

wiki:
	PYTHONPATH=src uv run --extra sse python examples/wikimedia_stream.py

btc:
	PYTHONPATH=src uv run --extra ws python examples/bitcoin_stream.py

batch:
	PYTHONPATH=src uv run python examples/batch_file.py $(FILE)

custom:
	PYTHONPATH=src uv run python examples/custom_adapter.py

micro-batch:
	PYTHONPATH=src uv run python examples/micro_batch.py

bridge:
	PYTHONPATH=src uv run --extra ws python examples/stream_to_kafka.py $(N)

consume-btc:
	PYTHONPATH=src uv run python examples/consume_btc_ticks.py

docs-serve:
	uv run --group docs mkdocs serve

docs-build:
	uv run --group docs mkdocs build --strict

docs-deploy:
	uv run --group docs mkdocs gh-deploy --force

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
