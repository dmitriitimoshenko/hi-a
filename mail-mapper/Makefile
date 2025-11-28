migration-gen:
	@if [ -z "$$DATABASE_MAIL_MAPPER_URL" ]; then echo "ERROR: set DATABASE_MAIL_MAPPER_URL"; exit 1; fi
	PYTHONPATH=. \
	DATABASE_MAIL_MAPPER_URL=$$DATABASE_MAIL_MAPPER_URL \
	alembic revision -m "init" --autogenerate

migration-up:
	@if [ -z "$$DATABASE_MAIL_MAPPER_URL" ]; then echo "ERROR: set DATABASE_MAIL_MAPPER_URL"; exit 1; fi
	PYTHONPATH=../. \
	DATABASE_MAIL_MAPPER_URL=$$DATABASE_MAIL_MAPPER_URL \
	alembic upgrade head
