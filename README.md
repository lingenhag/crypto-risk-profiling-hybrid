# RRP: Return-Risk Profiling of Cryptocurrencies

Proof-of-Concept für hybrides Profiling mit Quant-Metriken (Sharpe, VaR) + GenAI-Sentiment (OpenAI/Gemini/xAI).

## Setup
1. `python -m venv .venv && source .venv/bin/activate` (Linux/Mac) oder `.venv\Scripts\activate` (Win).
2. `pip install -e .` (für editable install).
3. `playwright install` (für Resolver).
4. Setze Env: `OPENAI_API_KEY=...` (etc. für Gemini/xAI).
5. Migriere DB: `python -c "from src.com.lingenhag.rrp.platform.persistence.migrator import apply_migrations; apply_migrations('data/pm.duckdb', 'src/com/lingenhag/rrp/platform/persistence/migrations')"`
6. Test: `pytest tests/features/news/ -v`.
7. Run: `python src/com/lingenhag/rrp/features/news/presentation/cli_commands.py news harvest --asset BTC --days 1 --auto-migrate`.

## Struktur
- `domain/`: Models (Asset, POmegaScore).
- `features/news/`: Harvest-Pipeline (GDELT/Google RSS).
- `platform/persistence/`: DuckDB-Migrationen/Views.
- `config.yaml`: LLM/Quellen-Config (siehe Beispiele).

## Nächste Schritte
- LLM-Integration: Summarize + Ensemble-Votes.
- Market: CoinGeckoAdapter für Snapshots.
- Dashboard: Streamlit für P_ω-Sensitivität.