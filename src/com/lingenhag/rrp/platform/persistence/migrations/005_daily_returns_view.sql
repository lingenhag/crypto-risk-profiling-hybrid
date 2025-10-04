-- src/com/lingenhag/rrp/platform/persistence/migrations/005_daily_returns_view.sql
-- =============================================================================
-- Erstellt die View v_daily_returns für ComputeMarketFactors.
--  ret_1d = (close_t / close_{t-1}) - 1
-- DuckDB: Window-Funktion LAG() über (asset_symbol ORDER BY date).
-- =============================================================================

BEGIN TRANSACTION;

DROP VIEW IF EXISTS v_daily_returns;

CREATE VIEW v_daily_returns AS
WITH base AS (
    SELECT
        mh.asset_symbol,
        mh.date AS date,
        mh.close AS close,
        LAG(mh.close) OVER (
            PARTITION BY mh.asset_symbol
            ORDER BY mh.date
            ) AS prev_close
    FROM market_history AS mh
),
     ret AS (
         SELECT
             asset_symbol,
             date,
             CASE
                 WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                 ELSE (close / prev_close) - 1.0
                 END AS ret_1d
         FROM base
     )
SELECT
    asset_symbol,
    date,
    ret_1d
FROM ret
ORDER BY asset_symbol, date;

COMMIT;