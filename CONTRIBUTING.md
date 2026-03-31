# Contributing

## Branching
- Use short-lived branches from `main`.
- Open pull requests with clear context and test evidence.

## Commit style
- Follow Conventional Commits.
- Examples: `feat: add hourly ingestion retry`, `fix: handle empty raw snapshot`.

## Minimum checks before PR
- `docker compose -f docker/docker-compose.yml config`
- `python -m py_compile ingestion/coingecko_ingestion.py`
- `python -m py_compile warehouse/load_raw_to_postgres.py`
- `dbt parse --project-dir dbt --profiles-dir dbt`

## Pull request checklist
- Describe what changed and why.
- Mention risks and rollback strategy (if any).
- Include evidence of local validation.
