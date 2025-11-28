from __future__ import annotations

from fastapi import APIRouter, Depends, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.orm import Session

from app.database import get_db
from app.metrics import MetricsCollector


router = APIRouter(tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
def export_metrics(db: Session = Depends(get_db)) -> Response:
    collector = MetricsCollector(db)
    registry = collector.collect()
    payload = generate_latest(registry)

    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)
