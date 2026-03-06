.PHONY: test lint format demo-sqlite demo-api

test:
	pytest

lint:
	ruff check .
	mypy src

format:
	ruff check . --fix

demo-sqlite:
	data-agent inspect --db-url sqlite:///./examples/sqlite_demo/demo.db

demo-api:
	uvicorn data_agent_core.api.app:app --reload
