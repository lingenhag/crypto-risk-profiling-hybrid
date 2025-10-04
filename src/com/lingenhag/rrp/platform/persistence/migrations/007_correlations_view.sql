-- src/com/lingenhag/rrp/platform/persistence/migrations/007_correlations_view.sql
-- =============================================================================
-- Korrelationen v_asset_correlations über 30d Fenster (DuckDB-Views).
-- =============================================================================

BEGIN TRANSACTION;

DROP VIEW IF EXISTS v_asset_correlations;

CREATE VIEW v_asset_correlations AS
WITH returns AS (
    SELECT asset_symbol, date, ret_1d
        FROM v_daily_returns
        WHERE ret_1d IS NOT NULL
        ),
        pairs AS (
        SELECT
        r1.asset_symbol AS symbol_a,
        r2.asset_symbol AS symbol_b,
        r1.date,
        r1.ret_1d AS ret_a,
        r2.ret_1d AS ret_b
        FROM returns r1
        JOIN returns r2
        ON r1.date = r2.date
        WHERE r1.asset_symbol < r2.asset_symbol
        ),
        corr_30d AS (
        SELECT
        symbol_a,
        symbol_b,
        CORR(ret_a, ret_b) OVER (
        PARTITION BY symbol_a, symbol_b
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS corr_30d,
        date
        FROM pairs
        )
SELECT
    symbol_a,
    symbol_b,
    AVG(corr_30d) AS avg_corr_30d,
    COUNT(*)      AS n_days
FROM corr_30d
WHERE corr_30d IS NOT NULL
GROUP BY symbol_a, symbol_b
HAVING n_days >= 20
ORDER BY symbol_a, symbol_b;

COMMIT;