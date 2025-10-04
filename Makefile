.PHONY: setup fmt lint test db-up db-down nb

setup:
	cp -n .env.example .env || true
	poetry install
	pre-commit install

fmt:
	poetry run ruff check --fix .
	poetry run black .
	poetry run isort .

lint:
	poetry run ruff .
	poetry run mypy src

test:
	poetry run pytest -q

db-up:
	docker compose up -d

db-down:
	docker compose down

nb:
	poetry run jupyter notebook
