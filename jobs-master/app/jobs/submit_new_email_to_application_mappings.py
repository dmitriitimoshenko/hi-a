import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s: %(message)s",
)
logger = logging.getLogger("worker")

HEADERS_JSON = {
    "content-type": "application/json",
    "x-api-version": "1",
}
HEADERS_HEALTH = {
    "x-api-version": "1",
}


DEFAULT_TIMEOUT = httpx.Timeout(connect=5, read=30, write=30, pool=5)
DEFAULT_GSA_BASE_URL = "http://google-sheets-accessor:8083"
GSA_FETCH_ENDPOINT = "/api/application/fetch"
GSA_CLEANUP_MEETINGS_ENDPOINT = "/api/application/cleanup-meetings"


async def run(client: httpx.AsyncClient | None = None) -> None:
    logger.info("Running scheduled jobs...")

    if client is None:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as local_client:
            await _run_with_client(local_client)

        return

    await _run_with_client(client)


async def _run_with_client(client: httpx.AsyncClient) -> None:
    fetch_settings = _build_gsa_fetch_settings()
    if fetch_settings is None:
        logger.warning(
            "Skipping application fetch job: SHEET_ID is not configured",
        )
    else:
        fetch_url, fetch_payload = fetch_settings
        await _wrap_call(_post_json, client, fetch_url, payload=fetch_payload)

    cleanup_settings = _build_gsa_cleanup_settings()
    if cleanup_settings is not None:
        cleanup_url, cleanup_payload = cleanup_settings

        try:
            response = await _wrap_call(_post_json, client, cleanup_url, payload=None)
            response.raise_for_status()
            logger.info(f"{url} OK")
            logger.info(f"{url} -> {response.json()}")
        except Exception as e:
            logger.error(f"{url} failed: {e}")


    await _wrap_call(_post_json, client, "http://mail-processor:8081/api/mails/interesting/submit", payload={})


async def _wrap_call(
    call: Callable[..., Awaitable[httpx.Response]],
    client: httpx.AsyncClient,
    url: str,
    payload: dict | None = None,
) -> None:
    try:
        response = await call(client, url, payload)

        response.raise_for_status()

        logger.info(f"{url} OK")
        logger.info(f"{url} -> {response.json()}")
    except Exception as e:
        logger.error(f"{url} failed: {e}")


async def _post_json(
    client: httpx.AsyncClient,
    url: str,
    payload: dict | None = None,
) -> httpx.Response:
    json_payload = payload or {}

    response = await client.post(url, headers=HEADERS_JSON, json=json_payload)

    return response


def _build_gsa_fetch_settings() -> tuple[str, dict[str, Any]] | None:
    raw_base_url = os.getenv("GSA_BASE_URL", DEFAULT_GSA_BASE_URL)
    base_url = raw_base_url.rstrip("/") if raw_base_url else DEFAULT_GSA_BASE_URL
    sheet_id = os.getenv("SHEET_ID")

    if not sheet_id:
        return None

    sheet_page = os.getenv("SHEET_PAGE", "applications_list")

    payload: dict[str, Any] = {
        "id": sheet_id,
        "sheet_range": {
            "sheet_page": sheet_page,
        },
    }

    url = f"{base_url}{GSA_FETCH_ENDPOINT}"

    result = (url, payload)

    return result


def _build_gsa_cleanup_settings() -> tuple[str, dict[str, Any]] | None:
    raw_base_url = os.getenv("GSA_BASE_URL", DEFAULT_GSA_BASE_URL)
    base_url = raw_base_url.rstrip("/") if raw_base_url else DEFAULT_GSA_BASE_URL
    sheet_id = os.getenv("SHEET_ID")

    if not sheet_id:
        return None

    sheet_page = os.getenv("SHEET_PAGE", "applications_list")

    payload: dict[str, Any] = {
        "id": sheet_id,
        "sheet_page": sheet_page,
    }

    url = f"{base_url}{GSA_CLEANUP_MEETINGS_ENDPOINT}"

    result = (url, payload)

    return result
