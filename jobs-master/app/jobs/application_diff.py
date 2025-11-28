import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from app.kafka_client import KafkaClient


logger = logging.getLogger(__name__)

HEADERS_JSON = {
    "content-type": "application/json",
    "x-api-version": "1",
}


@dataclass
class ApplicationDiffJobConfig:
    sheet_id: str | None
    sheet_page: str
    base_url: str
    kafka_topic_applications_sync: str


_CONFIG: ApplicationDiffJobConfig | None = None

ROWS_PER_ITERATION = 25
ITERATION_DELAY_SECONDS = 10
MIN_SHEET_ROW = 3
LAST_PROCESSED_ROW_ENDPOINT = "/api/application/last-processed-row"
APPLICATION_DIFF_ENDPOINT = "/api/application/diff"


def get_application_diff_job_config() -> ApplicationDiffJobConfig:
    global _CONFIG

    if _CONFIG is None:
        sheet_id = os.getenv("SHEET_ID")
        sheet_page = os.getenv("SHEET_PAGE", "applications_list")
        base_url = os.getenv(
            "GSA_BASE_URL",
            "http://google-sheets-accessor:8083",
        )
        kafka_topic_applications_sync = os.getenv(
            "KAFKA_TOPIC_APPLICATIONS_SYNC",
            "applications-sync-unprocessed",
        )

        _CONFIG = ApplicationDiffJobConfig(
            sheet_id=sheet_id,
            sheet_page=sheet_page,
            base_url=base_url,
            kafka_topic_applications_sync=kafka_topic_applications_sync,
        )

    return _CONFIG


async def run(
    client: httpx.AsyncClient,
    kafka_client: KafkaClient,
) -> None:
    config = get_application_diff_job_config()

    if config.sheet_id is None:
        logger.warning(
            "Skipping application diff job: JOBS_MASTER_GSA_SHEET_ID is not configured",
        )

        return None

    last_processed_row = await _fetch_last_processed_row(client, config)

    if last_processed_row is None:
        return None

    row_ranges = _build_row_ranges(last_processed_row)

    if not row_ranges:
        logger.info(
            "Application diff job skipped: last_processed_row=%s is below minimum=%s",
            last_processed_row,
            MIN_SHEET_ROW,
        )

        return None

    batches_count = len(row_ranges)

    logger.info(
        "Application diff job started: last_processed_row=%s, batches=%s",
        last_processed_row,
        batches_count,
    )

    for index, (start_row, end_row) in enumerate(row_ranges):
        logger.info(
            "Processing application diff for rows %s-%s (%s/%s)",
            start_row,
            end_row,
            index + 1,
            batches_count,
        )

        diff_data = await _request_application_diff(
            client,
            config,
            start_row,
            end_row,
        )

        if diff_data is not None:
            rows_checked = diff_data.get("rows_checked")
            rows_with_differences = diff_data.get("rows_with_differences")

            logger.info(
                "Diff finished for rows %s-%s: rows_checked=%s, rows_with_differences=%s",
                start_row,
                end_row,
                rows_checked,
                rows_with_differences,
            )

            _publish_differences(
                kafka_client,
                config,
                start_row,
                end_row,
                diff_data,
            )

            kafka_client.flush()

        if index < batches_count - 1:
            await asyncio.sleep(ITERATION_DELAY_SECONDS)


async def _fetch_last_processed_row(
    client: httpx.AsyncClient,
    config: ApplicationDiffJobConfig,
) -> int | None:
    url = f"{config.base_url}{LAST_PROCESSED_ROW_ENDPOINT}"

    try:
        response = await client.post(
            url,
            headers=HEADERS_JSON,
        )
        response.raise_for_status()
    except Exception as e:
        logger.error(
            "Failed to fetch last processed row from %s: %s",
            url,
            e,
        )

        return None

    try:
        body = response.json()
    except Exception as e:
        logger.error(
            "Invalid JSON in last processed row response: %s",
            e,
        )

        return None

    data = body.get("data") if isinstance(body, dict) else None

    if not isinstance(data, dict):
        logger.warning(
            "Last processed row response has no data object: %s",
            body,
        )

        return None

    last_processed_row = data.get("last_processed_row")

    if not isinstance(last_processed_row, int):
        logger.warning(
            "Invalid last_processed_row value: %s",
            data,
        )

        return None

    return last_processed_row


async def _request_application_diff(
    client: httpx.AsyncClient,
    config: ApplicationDiffJobConfig,
    start_row: int,
    end_row: int,
) -> dict | None:
    payload = _build_diff_payload(config, start_row, end_row)

    try:
        response = await client.post(
            f"{config.base_url}{APPLICATION_DIFF_ENDPOINT}",
            headers=HEADERS_JSON,
            json=payload,
        )
        response.raise_for_status()
    except Exception as e:
        logger.error(
            "Application diff request failed for rows %s-%s: %s",
            start_row,
            end_row,
            e,
        )

        return

    try:
        body = response.json()
    except Exception as e:
        logger.error(
            "Application diff returned invalid JSON for rows %s-%s: %s",
            start_row,
            end_row,
            e,
        )

        return

    data = body.get("data") if isinstance(body, dict) else None

    if not isinstance(data, dict):
        logger.warning(
            "Application diff response has no data object for rows %s-%s: %s",
            start_row,
            end_row,
            body,
        )

        return

    return data


def _publish_differences(
    kafka_client: KafkaClient,
    config: ApplicationDiffJobConfig,
    start_row: int,
    end_row: int,
    diff_data: dict,
) -> None:
    differences = diff_data.get("differences")

    if not isinstance(differences, list):
        logger.debug(
            "Diff data for rows %s-%s has no differences list: %s",
            start_row,
            end_row,
            diff_data,
        )

        return

    for entry in differences:
        if not isinstance(entry, dict):
            logger.debug(
                "Skipping diff entry for rows %s-%s because it is not a dict: %s",
                start_row,
                end_row,
                entry,
            )

            continue

        payload = _build_kafka_payload(
            config,
            start_row,
            end_row,
            entry,
        )

        key = entry.get("application_id")

        if key is None:
            fallback_row_id = entry.get("row_id")
            key = f"row-{fallback_row_id}"

            logger.warning(
                "Diff entry for rows %s-%s has no application_id, using fallback key=%s: %s",
                start_row,
                end_row,
                key,
                entry,
            )

        try:
            kafka_client.publish(
                topic=config.kafka_topic_applications_sync,
                key=key,
                value=payload,
            )
            logger.info(
                "Enqueued diff message: topic=%s key=%s row_id=%s company=%s role=%s",
                config.kafka_topic_applications_sync,
                key,
                entry.get("row_id"),
                entry.get("company"),
                entry.get("role_title"),
            )
        except Exception as e:
            logger.error(
                "Failed to publish diff entry for rows %s-%s to topic %s: %s",
                start_row,
                end_row,
                config.kafka_topic_applications_sync,
                e,
            )


def _build_kafka_payload(
    config: ApplicationDiffJobConfig,
    start_row: int,
    end_row: int,
    entry: dict,
) -> dict:
    detected_at = _utc_now_iso()

    payload = {
        "sheet_id": config.sheet_id,
        "sheet_page": config.sheet_page,
        "range": {
            "start_row": start_row,
            "end_row": end_row,
        },
        "application_id": entry.get("application_id"),
        "row_id": entry.get("row_id"),
        "company": entry.get("company"),
        "role_title": entry.get("role_title"),
        "differences": entry.get("differences", []),
        "sheet_payload": entry.get("sheet_payload", {}),
        "db_snapshot": entry.get("db_snapshot"),
        "errors": entry.get("errors", []),
        "detected_at": detected_at,
    }

    return payload


def _utc_now_iso() -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    iso = now.isoformat()

    if iso.endswith("+00:00"):
        normalized = f"{iso[:-6]}Z"

        return normalized

    return iso


def _build_diff_payload(
    config: ApplicationDiffJobConfig,
    start_row: int,
    end_row: int,
) -> dict:
    payload = {
        "id": config.sheet_id,
        "sheet_range": {
            "sheet_page": config.sheet_page,
            "start_row": start_row,
            "end_row": end_row,
        },
    }

    return payload


def _build_row_ranges(last_processed_row: int) -> list[tuple[int, int]]:
    if last_processed_row < MIN_SHEET_ROW:
        ranges: list[tuple[int, int]] = []

        return ranges

    ranges: list[tuple[int, int]] = []
    current_end = last_processed_row

    while current_end >= MIN_SHEET_ROW:
        start_candidate = current_end - ROWS_PER_ITERATION + 1
        start_row = max(MIN_SHEET_ROW, start_candidate)
        ranges.append((start_row, current_end))
        current_end = start_row - 1

    return ranges
