import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()

DATABASE_URL = os.getenv("DATABASE_MAIL_PROCESSOR_URL")
if DATABASE_URL is None:
    message = "DATABASE_MAIL_PROCESSOR_URL is not set"

    raise RuntimeError(message)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True
)


def get_engine():
    return engine


def get_session_maker():
    return SessionLocal


def _session_scope() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db() -> Generator[Session, None, None]:
    yield from _session_scope()


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    yield from _session_scope()
