migration-gen:
	@if [ -z "$$DATABASE_MAIL_PROCESSOR_URL" ]; then echo "ERROR: set DATABASE_MAIL_PROCESSOR_URL"; exit 1; fi
	PYTHONPATH=. \
	DATABASE_MAIL_PROCESSOR_URL=$$DATABASE_MAIL_PROCESSOR_URL \
	alembic revision -m "init" --autogenerate

migration-up:
	@if [ -z "$$DATABASE_MAIL_PROCESSOR_URL" ]; then echo "ERROR: set DATABASE_MAIL_PROCESSOR_URL"; exit 1; fi
	PYTHONPATH=../. \
	DATABASE_MAIL_PROCESSOR_URL=$$DATABASE_MAIL_PROCESSOR_URL \
	alembic upgrade head
