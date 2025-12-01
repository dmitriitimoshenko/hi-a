# Docker commands

docker-image-build:
	docker build -t hire_insight_ai_assistant_google_sheets_accessor:latest .
	minikube image load hire_insight_ai_assistant_google_sheets_accessor:latest

docker-image-rm:
	docker image rm hire_insight_ai_assistant_google_sheets_accessor:latest

migration-gen:
	@if [ -z "$$DATABASE_GOOGLE_SHEETS_ACCESSOR_URL" ]; then echo "ERROR: set DATABASE_GOOGLE_SHEETS_ACCESSOR_URL"; exit 1; fi
	PYTHONPATH=. \
	DATABASE_GOOGLE_SHEETS_ACCESSOR_URL=$$DATABASE_GOOGLE_SHEETS_ACCESSOR_URL \
	alembic revision -m "init" --autogenerate

migration-up:
	@if [ -z "$$DATABASE_GOOGLE_SHEETS_ACCESSOR_URL" ]; then echo "ERROR: set DATABASE_GOOGLE_SHEETS_ACCESSOR_URL"; exit 1; fi
	PYTHONPATH=../. \
	DATABASE_GOOGLE_SHEETS_ACCESSOR_URL=$$DATABASE_GOOGLE_SHEETS_ACCESSOR_URL \
	alembic upgrade head
