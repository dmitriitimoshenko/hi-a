# Deployment

up-k8s:
	cd mail-tracker/ && make docker-image-build && cd ..
	cd mail-processor/ && make docker-image-build && cd ..
	cd google-sheets-accessor/ && make docker-image-build && cd ..

	cd k8s/ && make up && cd ..

down-k8s:
	cd k8s/ && make down && cd ..

	docker image rm hire_insight_ai_assistant_mail_tracker:latest
	docker image rm hire_insight_ai_assistant_mail_processor:latest
	docker image rm hire_insight_ai_assistant_google_sheets_accessor:latest

	minikube image rm docker.io/library/hire_insight_ai_assistant_mail_tracker:latest
	minikube image rm docker.io/library/hire_insight_ai_assistant_mail_processor:latest
	minikube image rm docker.io/library/hire_insight_ai_assistant_google_sheets_accessor:latest

# Development

up:
	COMPOSE_BAKE=true docker compose up -d
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
