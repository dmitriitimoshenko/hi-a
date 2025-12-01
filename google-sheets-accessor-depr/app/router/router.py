from fastapi import APIRouter

from app.router.handlers.api.application.compare import compare
from app.router.handlers.api.application.cleanup_meetings import cleanup_meetings
from app.router.handlers.api.application.fetch import fetch
from app.router.handlers.api.application.last_processed_row import last_processed_row
from app.router.handlers.api.application.list import list
from app.router.handlers.api.application.update import update
from app.router.handlers.api.application.update_external import update_external
from app.router.handlers.api.health_check import health_check
from app.router.handlers.api.sheet.get import get

api_router = APIRouter()

api_router.include_router(health_check.router)

api_router.include_router(compare.router)
api_router.include_router(fetch.router)
api_router.include_router(list.router)
api_router.include_router(last_processed_row.router)
api_router.include_router(update.router)
api_router.include_router(update_external.router)
api_router.include_router(cleanup_meetings.router)

api_router.include_router(get.router)
