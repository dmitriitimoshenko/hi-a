import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import httpx

from jobs import submit_new_email_to_application_mappings
from jobs import application_diff
from app.kafka_client import KafkaClient, get_kafka_client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Jobs Master...")


async def jobs_10_mins(
    client: httpx.AsyncClient,
    kafka_client: KafkaClient,
) -> None:
    await application_diff.run(
        client,
        kafka_client,
    )


async def jobs_15_mins(client: httpx.AsyncClient) -> None:
    await submit_new_email_to_application_mappings.run(client)


async def run_scheduler(
    client: httpx.AsyncClient,
    kafka_client: KafkaClient,
) -> None:
    ten_minutes_task = asyncio.create_task(
        _run_periodic(jobs_10_mins, 600, client, kafka_client),
    )
    fifteen_minutes_task = asyncio.create_task(
        _run_periodic(jobs_15_mins, 900, client),
    )

    await asyncio.gather(ten_minutes_task, fifteen_minutes_task)


async def _run_periodic(
    job: Callable[..., Awaitable[None]],
    interval_seconds: int,
    *job_args: Any,
) -> None:
    loop = asyncio.get_running_loop()

    while True:
        started_at = loop.time()

        try:
            await job(*job_args)
        except Exception as e:
            logger.error(
                "Scheduled job %s failed: %s",
                job.__name__,
                e,
            )

        elapsed = loop.time() - started_at
        delay = max(interval_seconds - elapsed, 0)

        await asyncio.sleep(delay)


async def main() -> None:
    timeout = httpx.Timeout(connect=5, read=30, write=30, pool=5)

    kafka_client = get_kafka_client()

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            await run_scheduler(client, kafka_client)
    finally:
        kafka_client.close()


if __name__ == "__main__":
    asyncio.run(main())
