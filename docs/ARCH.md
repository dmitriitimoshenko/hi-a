# Service Architecture

This document captures the shared architecture and layout for the Python services.

## Infrastructure

![](images/infrastruct.png)

### Cross-Service Interactions

**HTTP calls**
1. jobs-master → google-sheets-accessor: fetch/cleanup/diff via `api/application/*`.
2. jobs-master → mail-processor: `POST /api/mails/interesting/submit`.
3. mail-processor → mail-mapper: `POST /api/mappings/resolve` (MailMapperClient).
4. mail-mapper → google-sheets-accessor: `POST /api/application/list` (applications without replies).
5. telegram-bot → google-sheets-accessor: `POST /api/application/update-internal` and `POST /api/application/update-external` (diff apply buttons).

**Kafka pipelines**
* mail-tracker → new-mail → mail-processor.
* mail-processor → interesting-mail → hire-event-processor.
* hire-event-processor → hire-event → hire-event-notifier.
* hire-event-notifier → notification → telegram-bot.
* hire-event-notifier → notification-sync → telegram-bot.
* telegram-bot → application-update-unprocessed → hire-event-processor.
* hire-event-processor → application-update-processed → google-sheets-accessor.
* jobs-master → applications-sync-unprocessed → hire-event-processor.
* hire-event-processor → applications-sync-processed → hire-event-notifier.
* telegram-bot → feedback → mail-mapper.
* google-sheets-accessor → add-embedding-for-application → application-embedding-generator.
* application-embedding-generator → save-embedding-for-application → google-sheets-accessor.


## Project Layout (template)

```
service-name/
  app/
    config.py                 # dataclass with environment wiring
    server.py                 # FastAPI + Kafka bootstrapper
    router/
      router.py               # composes API routers
      handlers/
        api/
          <area>/<action>/
            messages/
              request.py      # Pydantic request models
              response.py     # Dataclass response models
            <action>.py       # FastAPI handler (business entry point)
        kafka/
          handler.py          # Kafka event dispatcher for the service
    services/                 # Application services (domain logic)
    kafka_client/             # Shared Kafka client wrapper
    storage_client/           # HTTP clients to other services (optional)
    user_client/              # Additional HTTP clients (optional)
    enums/                    # Domain enums used across layers
    database/                 # SQLAlchemy models and DB utilities (optional)
    integrations/             # External SDK adapters (optional)
    tools/                    # Miscellaneous helpers (optional)
  Dockerfile
  gunicorn.conf.py
  requirements.txt
  Makefile / scripts ...
```

## Layering

- Layer communication must follow: **server → router → handler → service → client/integration**. Services hold orchestration logic; clients wrap I/O.
- Modules and handlers follow existing paths, e.g. `app/router/handlers/api/<area>/<action>/`.

## FastAPI & HTTP Handlers

- Routers live under `app/router/handlers/api/<area>/<action>/<action>.py` with matching `messages/request.py` & `messages/response.py`.
- Handlers accept dependencies through `Depends(...)` and convert business errors into typed response objects.
- Responses are dataclasses inheriting from `BaseAPIResponse`; they can extend with `data`/extra fields.
- Request validation relies on Pydantic models with `ConfigDict` or field definitions when enums must keep their semantic type.
- Middleware in `server.py` validates headers such as `X-API-Version` and returns an explicit `JSONResponse` on mismatch.
- FastAPI routers keep request/response models in `messages/` with `request.py` and `response.py`.

## Kafka Integration Pattern

- `app/server.py` owns the consumer lifecycle: background thread, graceful shutdown via SIGINT/SIGTERM, and log helpers for consumed messages.
- `app/kafka_client/client.py` provides a typed wrapper over `confluent_kafka` with:
  - Lazy producer/consumer initialisation (`_ensure_producer/_ensure_consumer`).
  - Serialisers/deserialisers returning `(payload_bytes, content_type)` tuples.
  - `poll_once` returning `(key, value, headers, Message)` or `None` with early exits.
  - `publish` callbacks typed as `Callable[[Exception | None, Message], None]`.
- Kafka event handling logic sits in `app/router/handlers/kafka/handler.py` and delegates to application services.

## Services & Clients

- Services accept client instances & config objects via constructor injection and expose explicit methods for each use case.
- Services raise custom `...ServiceError` subclasses; handlers translate them into API responses.
- HTTP clients (`storage_client`, `user_client`, etc.) wrap `requests.Session`, enforce timeouts, and normalise response bodies. They always return intermediate variables before exiting.
- External integrations (e.g., OpenAI) expose `get_*` factory functions for FastAPI dependency injection.

## System Responsibilities

- `mail-tracker` polls Gmail via `GMAIL_USER` / `GMAIL_APP_PASSWORD`, deduplicates messages in Redis, and publishes normalised payloads with attachments/ICS to Kafka topic `new-mail`.
- `mail-processor` consumes `new-mail`, normalises content, builds OpenAI embeddings, classifies emails into hiring-related labels, stores data in Postgres, and publishes mapped interesting emails to Kafka topic `interesting-mail`.
- `mail-mapper` matches interesting emails to tracked applications using scoring weights and calibrations from Postgres, records matches for analysis, and emits user feedback events to Kafka topic `feedback`.
- `google-sheets-accessor` keeps the Google Sheet of applications in sync, persists data to Postgres, and emits Kafka events for application updates plus embedding requests to `add-embedding-for-application`.
- `application-embedding-generator` consumes embedding requests, produces embeddings via OpenAI, and publishes them back to Kafka topic `save-embedding-for-application`.
- `hire-event-processor` merges interesting emails and sheet updates into canonical hire events on Kafka topics (`hire-event`, `applications-sync-*`, `application-update-*`).
- `hire-event-notifier` converts hire events into notification payloads on `notification` and `notification-sync` topics for downstream consumers.
- `telegram-bot` delivers notifications to Telegram users and forwards manual feedback to Kafka topic `feedback`.
- `jobs-master` schedules periodic application sync jobs so the pipeline stays up to date.
