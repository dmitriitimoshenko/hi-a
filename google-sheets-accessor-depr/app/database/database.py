import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()

# Use psycopg v3 driver (recommended). Example URL:
# postgresql+psycopg://user:pass@localhost:5432/app
DATABASE_URL = os.getenv("DATABASE_GOOGLE_SHEETS_ACCESSOR_URL")
if DATABASE_URL is None:
    message = "DATABASE_GOOGLE_SHEETS_ACCESSOR_URL is not set"

    raise RuntimeError(message)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

# Session factory
SessionLocal = sessionmaker(
    bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True
)


def get_engine():
    return engine


def get_session_maker():
    return SessionLocal


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
