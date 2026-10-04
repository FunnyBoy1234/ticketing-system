BASE_URL ?= http://localhost:8000

.PHONY: up down logs ps burst burst-small install lock dev edge-cases

up:            ## build and start Postgres + API (same image as the deploy)
	docker compose up --build -d

down:
	docker compose down

logs:          ## follow the API's JSON logs
	docker compose logs -f app

ps:
	docker compose ps

burst:         ## 20k-request on-sale stampede: make burst BASE_URL=https://... ADMIN_KEY=...
	./burst.sh $(BASE_URL)

burst-small:   ## quick 2k-request version
	./burst.sh $(BASE_URL) --requests 2000 --storm-size 100 --users 300

install:       ## create .venv from uv.lock
	uv sync

lock:          ## re-resolve uv.lock after editing pyproject.toml
	uv lock

dev:           ## run the API without Docker (needs DATABASE_URL)
	uv run uvicorn app.main:app --reload --port 8000

edge-cases:    ## forced-race checks against a local stack (needs DATABASE_URL)
	uv run scripts/check_edge_cases.py
