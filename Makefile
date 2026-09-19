# Development

up:
	COMPOSE_BAKE=true docker compose up -d
	lazydocker

down:
	docker compose down

re-run:
	COMPOSE_BAKE=true docker compose up -d --force-recreate --remove-orphans --build
	lazydocker

format:
	python3.13 -m ruff format . --target-version=py313

cpy-learn-data-from-remote-local:
	python3.13 scripts/from_google_sheet_to_mail_processor_db.py

learn-and-commit-local:
	curl --request POST \
  	--url http://localhost:8081/api/embd_lrn/learn \
	--header 'content-type: application/json' \
  	--header 'x-api-version: 1' \
  	--data '{}'
	curl --request POST \
  	--url http://localhost:8081/api/embd_lrn/commit \
  	--header 'content-type: application/json' \
  	--header 'x-api-version: 1' \
  	--data '{}'
