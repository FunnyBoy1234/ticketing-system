BASE_URL ?= http://localhost:8000

.PHONY: help up down logs ps burst burst-small bad-input edge-cases install lock dev

help:          ## list the targets (default)
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:[^#]*## /\t/'

up:            ## build and start Postgres + API (same image as the deploy)
	docker compose up --build -d

down:          ## stop the stack (data volume kept; `docker compose down -v` drops it)
	docker compose down

logs:          ## follow the API's JSON logs
	docker compose logs -f app

ps:            ## container status, including health
	docker compose ps

burst:         ## 20k-request on-sale stampede: make burst BASE_URL=https://... ADMIN_KEY=...
	./burst.sh $(BASE_URL)

burst-small:   ## quick 2k-request version
	./burst.sh $(BASE_URL) --requests 2000 --storm-size 100 --users 300

bad-input:     ## malformed input against BASE_URL: right 4xx for each case, never a 5xx
	python3 scripts/check_bad_input.py $(BASE_URL)

edge-cases:    ## forced-race checks inside the compose network (after `make up`)
	docker compose --profile checks run --rm edge-cases

install:       ## create .venv from uv.lock (for running without Docker)
	uv sync

lock:          ## re-resolve uv.lock after editing pyproject.toml
	uv lock

dev:           ## run the API without Docker, with reload (needs DATABASE_URL)
	uv run uvicorn app.main:app --reload --port 8000
