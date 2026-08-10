# NBA Data Lakehouse

A Databricks lakehouse project that ingests NBA game and betting-odds data, preserves raw source payloads in JSONL/Delta Bronze storage, and builds Silver tables for game, team, date, and head-to-head odds analysis.

## Architecture

~~~mermaid
flowchart TD
  A[BallDontLie API games] --> B[Local JSONL landing by ingestion date]
  C[The Odds API H2H odds] --> B
  B --> D[Databricks Volume]
  D --> E[Bronze Delta raw records]
  E --> F[Silver Delta games teams dates odds]
  F --> G[Game and latest H2H odds analysis]
~~~

## What it does

The local extractor supports daily pulls for a recent game window and historical backfills for chosen seasons. BallDontLie and, when configured, The Odds API responses are written to date-partitioned JSONL landing files before Databricks notebooks transform them into Delta tables.

## Tech stack

| Area | Tools |
| --- | --- |
| Language | Python, SQL |
| Ingestion | API clients, JSON Lines |
| Configuration | environment variables, python-dotenv |
| Lakehouse | Databricks, Spark SQL, Delta Lake, Unity Catalog / Volumes |
| Sources | BallDontLie API, The Odds API |
| Validation | Databricks Bronze/Silver validation notebooks |

## Repository structure

~~~text
ingestion/src/nba_ingest/       # configuration, common helpers, API clients/extractors
scripts/run_daily_extract.py    # daily/backfill local orchestrator
api-smoke-tests/scripts/        # source connectivity checks
notebooks/00_admin/             # catalog/schema and Volume setup
notebooks/01_bronze/            # landing-to-Bronze loading
notebooks/02_silver/            # dimensions, facts, game/odds enrichment
notebooks/99_validation/        # Bronze and Silver validation
 .env.example                   # required configuration keys without secrets
~~~

## Data layers

| Layer | Purpose |
| --- | --- |
| Landing | Minimal-transformation JSONL partitioned by ingestion date |
| Bronze | Raw Delta source records loaded from landing files |
| Silver | Normalized team/date dimensions, game facts, H2H odds, and enriched game/odds outputs |

The latest H2H workflow joins odds to games through normalized home/away team names and a time-tolerance condition while retaining bookmaker/outcome grain for line-shopping analysis.

## Setup

~~~bash
pip install python-dotenv requests
cp .env.example .env
~~~

Populate the local .env file with BALLDONTLIE_API_KEY, THE_ODDS_API_KEY, and LANDING_BASE_DIR. Never commit this file.

Run a safe API check:

~~~bash
python api-smoke-tests/scripts/test_bdl.py
python api-smoke-tests/scripts/test_odds.py
~~~

Produce landing data:

~~~bash
python scripts/run_daily_extract.py --mode daily
python scripts/run_daily_extract.py --mode backfill --season-start 2024 --season-end 2024
~~~

Then run Databricks notebooks in order: admin setup, Volume setup, landing-to-Bronze, Silver dimensions/facts, then validation.

## Data notes

- Daily pulls use a recent lookback to tolerate source/timezone delays.
- Backfills should be chunked to respect API quotas.
- Odds extraction is optional when its key is absent.
- Raw responses carry ingestion metadata for lineage and debugging.

## Security notice

Use only .env.example in Git. If a key was ever committed, revoke it at the provider, purge it from history, and replace it. A gitignore rule does not protect a secret that has already been committed.

## Next steps

- Add a package definition and dependency lock file.
- Schedule daily extraction and Databricks jobs.
- Add schema validation and source-change alerting.
- Parameterize catalog/schema names.
- Add a Gold layer for betting-market, game-performance, and model-ready features.

## License

No license is currently specified.