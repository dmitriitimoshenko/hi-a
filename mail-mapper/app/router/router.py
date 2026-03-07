from fastapi import APIRouter

from app.router.handlers.api.health_check import health_check
from app.router.handlers.api.mappings.resolve import resolve

api_router = APIRouter()

api_router.include_router(health_check.router)
api_router.include_router(resolve.router)
