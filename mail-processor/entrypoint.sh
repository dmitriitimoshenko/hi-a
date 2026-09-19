#!/bin/sh
set -e

alembic upgrade head

exec uvicorn app.server:app --host 0.0.0.0 --port "${PORT}"
