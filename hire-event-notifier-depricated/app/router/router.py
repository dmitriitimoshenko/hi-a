from fastapi import APIRouter
from app.router.handlers.api.health_check import health_check

api_router = APIRouter()

api_router.include_router(health_check.router)
