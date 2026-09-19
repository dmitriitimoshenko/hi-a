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

**Redis Streams pipelines**

The bus is Redis Streams on logical DB `2` of the shared Redis instance. Each
arrow below is a stream consumed through a consumer group with `XACK`-based
at-least-once delivery.

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
    server.py                 # FastAPI + bus bootstrapper
    router/
      router.py               # composes API routers
      handlers/
        api/
          <area>/<action>/
            messages/
              request.py      # Pydantic request models
              response.py     # Dataclass response models
            <action>.py       # FastAPI handler (business entry point)
        bus/
          handler.py          # Bus event dispatcher for the service
    services/                 # Application services (domain logic)
    bus/                      # Redis Streams client wrapper
    storage_client/           # HTTP clients to other services (optional)
    user_client/              # Additional HTTP clients (optional)
    enums/                    # Domain enums used across layers
    database/                 # SQLAlchemy models and DB utilities (optional)
    integrations/             # External SDK adapters (optional)
    tools/                    # Miscellaneous helpers (optional)
  Dockerfile
  entrypoint.sh
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

## Bus Integration Pattern

The message bus is Redis Streams. Both language stacks expose the same minimal
contract, so handlers never see the transport.

- `app/server.py` owns the consumer lifecycle: background thread, graceful shutdown via SIGINT/SIGTERM, and log helpers for consumed messages.
- `app/bus/client.py` (Python) wraps `redis-py` with:
  - Lazy client initialisation (`_ensure_client`).
  - JSON serialisers/deserialisers for the `key` and `value` stream fields.
  - `publish` mapping to `XADD` with `MAXLEN ~ STREAM_MAX_LEN` for retention.
  - `subscribe` creating the consumer group via `XGROUP CREATE ... MKSTREAM`.
  - `poll_once` returning a `BusMessage` or `None`; `commit` mapping to `XACK`.
- `internal/app/bus/client.go` (Go) exposes `Publish`, `Consume`, `Close` over `go-redis`.
- On start a consumer first replays its own unacknowledged entries (`XREADGROUP` with ID `0`), then switches to new messages (`>`). A handler that returns an error leaves the entry pending, so it is retried after a restart.
- Set `STREAM_START_AT_OLDEST=true` for services that must process a stream's backlog from the beginning.
- Bus event handling logic sits in `app/router/handlers/bus/` (Python) or `internal/app/bus/handlers/` (Go) and delegates to application services.

## Services & Clients

- Services accept client instances & config objects via constructor injection and expose explicit methods for each use case.
- Services raise custom `...ServiceError` subclasses; handlers translate them into API responses.
- HTTP clients (`storage_client`, `user_client`, etc.) wrap `requests.Session`, enforce timeouts, and normalise response bodies. They always return intermediate variables before exiting.
- External integrations (e.g., OpenAI) expose `get_*` factory functions for FastAPI dependency injection.

## System Responsibilities

- `mail-tracker` polls Gmail via `GMAIL_USER` / `GMAIL_APP_PASSWORD`, deduplicates messages in Redis, and publishes normalised payloads with attachments/ICS to stream `new-mail`.
- `mail-processor` consumes `new-mail`, normalises content, builds OpenAI embeddings, classifies emails into hiring-related labels, stores data in Postgres, and publishes mapped interesting emails to stream `interesting-mail`.
- `mail-mapper` matches interesting emails to tracked applications using scoring weights and calibrations from Postgres, records matches for analysis, and emits user feedback events to stream `feedback`.
- `google-sheets-accessor` keeps the Google Sheet of applications in sync, persists data to Postgres, and emits bus events for application updates plus embedding requests to `add-embedding-for-application`.
- `application-embedding-generator` consumes embedding requests, produces embeddings via OpenAI, and publishes them back to stream `save-embedding-for-application`.
- `hire-event-processor` merges interesting emails and sheet updates into canonical hire events on streams (`hire-event`, `applications-sync-*`, `application-update-*`).
- `hire-event-notifier` converts hire events into notification payloads on `notification` and `notification-sync` topics for downstream consumers.
- `telegram-bot` delivers notifications to Telegram users and forwards manual feedback to stream `feedback`.
- `jobs-master` schedules periodic application sync jobs so the pipeline stays up to date.
