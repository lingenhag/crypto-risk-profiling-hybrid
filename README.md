# Return-Risk Profiling of Cryptocurrencies (RRP)

This project implements a hybrid system for return-risk profiling of cryptocurrencies, combining quantitative market metrics (e.g., Sharpe Ratio, Sortino Ratio, VaR) with generative AI-based sentiment analysis from news sources. It is developed as a proof-of-concept in the context of the CAS Frontier Technologies in Finance program at Bern University of Applied Sciences.

The system is modular, reproducible, and focuses on end-to-end pipelines for news harvesting, market data ingestion, LLM processing, and factor computation. Data is persisted in DuckDB for auditability. It does not provide trading strategies, performance predictions, or production-ready deployment—examples are for illustration only.

Key features:
- **News Harvesting**: Collects URLs from GDELT and RSS feeds (e.g., Google News), with domain filtering.
- **Market Data**: Ingests spot and historical data from CoinGecko.
- **LLM Processing**: Uses an ensemble of LLMs (OpenAI, Gemini, xAI) for relevance and sentiment scoring.
- **Factor Computation**: Integrates quantitative factors with normalized sentiment into a blended score \( P_\omega \).
- **Monitoring**: Prometheus metrics exposed (default port 8000).
- **Auditability**: Logs votes, summaries, rejections, and exports (e.g., CSV).

For more details, refer to the [transfer report](Transferbericht_Adrian_Lingenhag.pdf) (excerpts provided in the query).

## Prerequisites

- Python 3.12+
- DuckDB for data persistence.
- API keys for:
  - CoinGecko (optional, for pro features; set in `COINGECKO_API_KEY` env var).
  - OpenAI (`OPENAI_API_KEY`).
  - Google Gemini (`GEMINI_API_KEY`).
  - xAI (`XAI_API_KEY`).
- Libraries: See `requirements.txt` (create one based on imports: duckdb, requests, pyyaml, prometheus_client, newspaper3k, feedparser, etc.).

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd rrp
   ```

2. Install dependencies (assuming a `requirements.txt` file; create it with the following content or use pipenv/venv):
   ```
   # requirements.txt
   duckdb
   pyyaml
   prometheus_client
   requests
   backoff
   newspaper3k  # For article parsing
   feedparser   # For RSS
   pandas       # For computations (implied in factors)
   numpy
   scipy
   ```

   Install:
   ```
   pip install -r requirements.txt
   ```

3. Set environment variables for API keys:
   ```
   export OPENAI_API_KEY="sk-..."
   export GEMINI_API_KEY="..."
   export XAI_API_KEY="..."
   export COINGECKO_API_KEY="..."  # Optional
   ```

4. Copy and customize `config.yaml` if needed (defaults are provided).

5. Run migrations manually if needed (or use `--auto-migrate` in CLI commands):
   ```
   python src/com/lingenhag/rrp/platform/persistence/migrator.py data/rrp.duckdb src/com/lingenhag/rrp/platform/persistence/migrations
   ```

## Configuration

The project uses `config.yaml` for settings. Key sections:
- `coingecko`: API base, key, timeouts.
- `openai`, `gemini`, `xai`: LLM models, endpoints, prompts, max_tokens, etc.
- `ensemble`: Which LLMs to use (true/false for each).
- `gdelt`, `google_news`: News source configs.
- `news_query`: Asset-specific query tweaks.
- `news_domain_filter`: Enforce domain allow/block lists.
- `database`: Default DuckDB path (`data/rrp.duckdb`).
- `sentiment`: Domain weighting and normalization params.

Override with `--config path/to/custom.yaml`.

## Usage

The CLI tool is invoked as `python src/com/lingenhag/rrp/main.py` (or alias it to `rrp`).

### Global Options
- `--config <path>`: Path to config.yaml (default: config.yaml).
- `--metrics-port <int>`: Prometheus metrics port (default: 8000).

### Subcommands

Subcommands are organized by feature: `news`, `market`, `llm`.

#### News Slice
Handles news URL harvesting.

- `rrp news harvest`: Harvest URLs from sources and store in `url_harvests` table (with domain filtering).
  - `--asset <symbol>`: Required, e.g., DOT, BTC (uppercase).
  - `--days <int>`: Days back (default: 1).
  - `--from <iso>`: Start datetime (ISO, e.g., 2024-01-01T00:00:00Z).
  - `--to <iso>`: End datetime (ISO).
  - `--source <all|gdelt|rss>`: Sources to use (default: all).
  - `--limit <int>`: Max URLs after deduping (default: 100).
  - `--db <path>`: DuckDB path (default from config).
  - `--rss-workers <int>`: Threads for RSS resolution (default: 4).
  - `--auto-migrate`: Apply schema migrations if missing.
  - `--verbose`: Enable detailed logging/progress.
  - `--enforce-domain-filter`: Force domain filter (overrides config).

  Example:
  ```
  python src/com/lingenhag/rrp/main.py news harvest --asset BTC --days 1 --limit 50 --verbose
  ```

#### Market Slice
Handles market data ingestion and computations.

- `rrp market ingest`: Ingest current spot data (price, volume, market cap).
  - `--asset <symbol> [<symbol>...]`: Required, e.g., BTC ETH.
  - `--vs <currency>`: Vs currency (default: usd).
  - `--provider <str>`: Data provider (default: CoinGecko).
  - `--provider-id <str>`: Provider ID (optional, for single asset).
  - `--db <path>`: DuckDB path.
  - `--auto-migrate`: Apply migrations.

  Example:
  ```
  python src/com/lingenhag/rrp/main.py market ingest --asset BTC ETH --vs usd
  ```

- `rrp market history`: Ingest historical OHLCV data.
  - `--asset <symbol>`: Required, e.g., BTC.
  - `--vs <currency>`: Default: usd.
  - `--provider <str>`: Default: CoinGecko.
  - `--provider-id <str>`: Optional.
  - `--days <int>`: Days back (default: 30).
  - `--from-ts <unix>`: Start timestamp.
  - `--to-ts <unix>`: End timestamp.
  - `--db <path>`: DuckDB path.
  - `--auto-migrate`: Apply migrations.

  Example:
  ```
  python src/com/lingenhag/rrp/main.py market history --asset BTC --days 365
  ```

- `rrp market factors`: Compute daily factors (returns, vol, Sharpe, Sortino, VaR, sentiment, \( P_\alpha \)).
  - `--asset <symbol>`: Required.
  - `--days <int>`: Days back (default: 365).
  - `--start <iso-date>`: Start date (YYYY-MM-DD).
  - `--end <iso-date>`: End date (default: today).
  - `--alpha <float>`: Blend factor for \( P_\alpha \) (default: 0.25).
  - `--window-vol <int>`: Window for vol/Sharpe/Sortino/VaR (default: 30).
  - `--window-sent <int>`: Window for sentiment norm (default: 90).
  - `--ema-len <int>`: EMA for expected return (default: 30).
  - `--norm <zscore|winsor|minmax>`: Normalization (default: zscore).
  - `--winsor-alpha <float>`: Winsor tails (default: 0.05).
  - `--var <param95|emp95>`: VaR method (default: param95).
  - `--export <path>`: Export CSV.
  - `--db <path>`: DuckDB path.
  - `--auto-migrate`: Apply migrations.
  - `--dry-run`: Compute without persisting.

  Example:
  ```
  python src/com/lingenhag/rrp/main.py market factors --asset BTC --days 365 --alpha 0.5 --export factors.csv
  ```

- `rrp market overview`: Show KPIs (close, avg volume/cap, return).
  - `--asset <symbol>`: Required.
  - `--start <iso-date>`: Required (YYYY-MM-DD).
  - `--end <iso-date>`: Required.
  - `--vs <currency>`: Default: usd.
  - `--format <table|json>`: Output format (default: table).
  - `--db <path>`: DuckDB path.
  - `--auto-migrate`: Apply migrations.

  Example:
  ```
  python src/com/lingenhag/rrp/main.py market overview --asset BTC --start 2024-01-01 --end 2024-10-01 --format json
  ```

#### LLM Slice
Processes harvested URLs for sentiment/relevance.

- `rrp llm process`: Summarize and score URLs.
  - `--asset <symbol>`: Required.
  - `--days <int>`: Days back (default: 1).
  - `--from <iso>`: Start datetime.
  - `--to <iso>`: End datetime.
  - `--limit <int>`: Max URLs (default: 10).
  - `--db <path>`: DuckDB path (default: data/rrp.duckdb).
  - `--parallel`: Enable parallel processing.
  - `--workers <int>`: Workers for parallel (default: 8).
  - `--rate-limit <int>`: Requests per minute per model (default: 60).
  - `--export-votes-csv <path>`: Export LLM votes to CSV.
  - `--dry-run`: Simulate without DB writes/deletes.

  Example:
  ```
  python src/com/lingenhag/rrp/main.py llm process --asset ETH --days 1 --limit 20 --parallel --workers 4 --export-votes-csv votes.csv
  ```

## Example Workflow

1. Harvest news: `rrp news harvest --asset BTC --days 1`
2. Process with LLMs: `rrp llm process --asset BTC --days 1`
3. Ingest market history: `rrp market history --asset BTC --days 365`
4. Compute factors: `rrp market factors --asset BTC --days 365`

## Monitoring

Metrics are exposed at `http://localhost:8000/metrics` (e.g., harvest duration, LLM calls).

## Notes

- The dashboard (visualization of factors, drawdowns, correlations) and unit tests are still pending implementation.
