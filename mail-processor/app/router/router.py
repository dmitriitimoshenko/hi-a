from fastapi import APIRouter
from app.router.handlers.api.health_check import health_check
from app.router.handlers.api.embd_lrn import commit, create, learn
from app.router.handlers.api.mails.interesting import submit

api_router = APIRouter()

api_router.include_router(health_check.router)

api_router.include_router(commit.router)
api_router.include_router(create.router)
api_router.include_router(learn.router)

api_router.include_router(submit.router)
