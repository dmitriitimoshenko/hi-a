# Service Code Style & Layout

This document captures the conventions shared by the Python services in this monorepo. The rules reflect the canonical implementation in `mail-processor/` and `google-sheets-accessor/`.

## 1. Project Layout (template)

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

Layer communication must follow: **server → router → handler → service → client/integration**. Services hold orchestration logic; clients wrap I/O.

## 2. Python Coding Guidelines

- **Python version**: Assume Python 3.13 features. Use `match`/`|` typing syntax and `list[str]` generics.
- **Type hints**: Every function (public or private) requires explicit return and parameter annotations.
- **Returns**: Assign expressions to a local variable before returning (improves readability & instrumentation).
- **Spacing**: Insert a blank line before each `return` block to visually separate the final outcome.
- **Exceptions**: Always capture exceptions as `e` (`except SomeError as e:`) and propagate using `raise ... from e` when rethrowing.
- **Logging**: Retrieve module-level loggers via `logging.getLogger(__name__)`. Use descriptive messages with context data.
- **Naming**: `snake_case` for modules/functions/variables, `PascalCase` for classes & dataclasses, `CONSTANT_CASE` for constants. Keep names descriptive.
- **Control flow**: Guard clauses should return well-named dataclasses or dictionaries; avoid inline ternary expressions in returns.
- **Data containers**: Favour `dataclass` DTOs for outbound API payloads and Pydantic models for inbound requests.
- **Enum handling**: Expose enum helpers (`is_valid`, `get_all`) on enum classes for validation.
- **Language**: ONLY Python 3.13 style: where it differs - always do refactor, 4-space indent, type hints for all the functions.
- **Comments**: ONLY IN ENG! Only add comments where logic is not human readable at all and really needs a comment
- **All Exception** names should be `e`, not `exc` etc.
- **Style**: keep names descriptive and keep function bodies human readable. Always split logical parts using spaces, add spaces before `return`, use variables in return instead of expressions and always split new functions into smaller ones like in this example:
```
def main(bar: int) -> None:
  boo = foo(bar)

  boo_good = boo["rar"]
  res = far(boo_good)

  print(res)

def foo(bar: int) -> dict:
  l: list = []

  bar += 1

  l["rar"] = bar
  l["rut] = 0

  return l

def far(bar: int) -> str:
  bar_str = str(bar)

  return bar_str
```
- **Layers**: keep layers: server -> handler -> serivce -> client
- **Naming**: snake_case for files/functions, PascalCase for classes, CONSTANT_CASE for constants. Modules and handlers follow existing paths, e.g. `app/router/handlers/api/<area>/<action>/`.

## 3. FastAPI & HTTP Handlers

- Routers live under `app/router/handlers/api/<area>/<action>/<action>.py` with matching `messages/request.py` & `messages/response.py`.
- Handlers accept dependencies through `Depends(...)` and convert business errors into typed response objects.
- Responses are dataclasses inheriting from `BaseAPIResponse`; they can extend with `data`/extra fields.
- Request validation relies on Pydantic models with `ConfigDict` or field definitions when enums must keep their semantic type.
- Middleware in `server.py` validates headers such as `X-API-Version` and returns an explicit `JSONResponse` on mismatch.
- FastAPI routers: keep request/response models in `messages/` with `request.py` and `response.py`.

## 4. Kafka Integration Pattern

- `app/server.py` owns the consumer lifecycle: background thread, graceful shutdown via SIGINT/SIGTERM, and log helpers for consumed messages.
- `app/kafka_client/client.py` provides a typed wrapper over `confluent_kafka` with:
  - Lazy producer/consumer initialisation (`_ensure_producer/_ensure_consumer`).
  - Serialisers/deserialisers returning `(payload_bytes, content_type)` tuples.
  - `poll_once` returning `(key, value, headers, Message)` or `None` with early exits.
  - `publish` callbacks typed as `Callable[[Exception | None, Message], None]`.
- Kafka event handling logic sits in `app/router/handlers/kafka/handler.py` and delegates to application services.

## 5. Services & Clients

- Services accept client instances & config objects via constructor injection and expose explicit methods for each use case.
- Services raise custom `...ServiceError` subclasses; handlers translate them into API responses.
- HTTP clients (`storage_client`, `user_client`, etc.) wrap `requests.Session`, enforce timeouts, and normalise response bodies. They always return intermediate variables before exiting.
- External integrations (e.g., OpenAI) expose `get_*` factory functions for FastAPI dependency injection.

## 6. Testing & Tooling Expectations

- Prefer focussed `pytest` modules under `service/tests/` mirroring the `app/` structure.
- For smoke tests, hit `/api/health-check` with header `X-API-Version: 1`.
- Use `python3 -m compileall app` as a quick static sanity check before committing.

Adhering to this style keeps services interchangeable, makes handlers predictable, and streamlines cross-service maintenance.
