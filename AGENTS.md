# Communication Guidelines

Always answer in English unless asked to do the opposite. Check file `AGENTS_ADDITION.md` and take instructions from there as first priority.

# Repository Guidelines

## Project Structure & Module Organization
- Monorepo of Python services: `mail-processor/`, `mail-tracker/`, `google-sheets-accessor/`, `mail-mapper/`, `hire-event-processor/`, `hire-event-notifier/`, `telegram-bot/`, `jobs-master/`.
- Typical service layout: `app/` with `server.py`, `router/handlers/...`, `bus/`, `config.py`, `enums/`, optional `database/` and `services/`.
- Orchestration: `docker-compose.yaml` (local stack). Shared scripts in `scripts/`. Images and diagrams in `images/`. Secrets (mounted) in `secrets/`.
- Do not construct service dependencies inside business methods; instantiate them once in `__init__` and reuse the stored reference.
- Prefer reusable `get_*` factories that accept optional pre-built dependencies (config, clients, sessions) and only construct defaults when they are not provided; avoid re-instantiating clients directly in handlers/services.
- Avoid any other language from English in `*.md` files unless explicitly required by context or instruction.

## Build, Test, and Development Commands
- Bring up local stack: `make up` (builds all, starts containers, opens lazydocker). Tear down: `make down`. Recreate: `make re-run`.
- Service images: inside a service dir, `make docker-image-build`; remove image `make docker-image-rm`.
- DB migrations (where applicable):
  - Mail Processor: `cd mail-processor && make migration-gen` then `make migration-up`.
  - Google Sheets Accessor: `cd google-sheets-accessor && make migration-gen` then `make migration-up`.
- Health checks: `curl http://localhost:<port>/api/health-check -H 'x-api-version: 1'` (e.g., 8081, 8082, 8083, 8085–8086).

## Coding Style & Naming Conventions
- Always consult `CODESTYLE.md` for up-to-date service coding conventions and follow the instructions listed there.

## Testing Guidelines
- No central test suite yet. Prefer adding lightweight pytest tests per service under `*/tests/` mirroring `app/` structure.
- Smoke-test endpoints via health checks and sample requests; include examples in PR descriptions.

## Commit & Pull Request Guidelines
- Commits: concise, imperative subject (≤72 chars). Prefer conventional prefixes when helpful: `feat:`, `fix:`, `chore:`, `refactor:`.
- PRs: include purpose, scope, local run steps (`make up` or service commands), screenshots/log snippets for relevant APIs, and linked issues.

## Security & Configuration Tips
- Configuration via environment variables in `docker-compose.yaml` and local `.env`. Do not commit secrets. Use `secrets/` for mounted credentials (e.g., `gsa-credentials.json`).
- Required envs include `OPENAI_API_KEY`, `GMAIL_USER`, `GMAIL_APP_PASSWORD`, DB URLs, and `STREAM_*` stream names. Example: set in `.env`, then `make up`.
- The message bus is Redis Streams, wrapped by `app/bus/` (Python) and `internal/app/bus/` (Go). Consumers delegate message handling to dedicated handlers: see `mail-processor/app/router/handlers/bus/handler.py`, `mail-mapper/app/router/handlers/bus/feedback.py`, `hire-event-processor/internal/app/bus/handlers/`, `hire-event-notifier/internal/app/bus/handlers/`, and `telegram-bot/internal/app/bus/handlers/`.
