BASE_URL ?= http://localhost:8000

.PHONY: up down logs ps burst burst-small smoke dev

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

dev:           ## run the API without Docker (needs DATABASE_URL)
	uvicorn app.main:app --reload --port 8000
