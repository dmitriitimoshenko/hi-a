# Mail Mapper

## Dynamic Scoring

- Match scoring now loads weights and optional calibration parameters from the database tables `scoring_weights`, `scoring_weight_entries`, and `scoring_calibrations`.
- Each persisted match and candidate stores the scoring configuration version and raw score to enable offline analysis and correlation with feedback.
- Default weights are migrated automatically; new configurations can be activated by inserting rows with `is_active = true`.

## Metrics Export

- The service exposes Prometheus metrics at `/api/metrics` (no API-version header required).
- Key metrics:
  - `mail_mapper_matches_total` – cumulative number of evaluated emails.
  - `mail_mapper_matches_by_status_total{status="assigned|skipped"}` – assignment split.
  - `mail_mapper_matches_by_config_total{version="..."}` – volume per scoring config.
  - `mail_mapper_match_overall_score_average` and `mail_mapper_match_raw_score_average`.
  - `mail_mapper_candidate_*` gauges for candidate scores.
  - `mail_mapper_feedback_total{reason="..."}` – user skip reasons.

## Grafana & Prometheus

- `docker-compose` now runs Prometheus (`http://localhost:9090`) and Grafana (`http://localhost:3000`).
- Grafana uses auto-provisioned Prometheus datasource and ships with `Mail Mapper Overview` dashboard.
- Default credentials can be overridden via `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` environment variables.
