-- src/com/lingenhag/rrp/platform/persistence/migrations/006_market_factors_daily_duckdb.sql
-- =============================================================================
-- DuckDB-kompatible Tabelle market_factors_daily (ohne Trigger/PLpgSQL).
-- P_omega wird in der Application-Schicht berechnet und upserted.
-- =============================================================================

BEGIN TRANSACTION;

DROP TABLE IF EXISTS market_factors_daily;

CREATE TABLE market_factors_daily
(
    asset_symbol     TEXT NOT NULL,
    date             DATE NOT NULL,

    -- Rendite & Risiko (rolling)
    ret_1d           DOUBLE,
    vol_30d          DOUBLE,
    sharpe_30d       DOUBLE,
    sortino_30d      DOUBLE,
    var_1d_95        DOUBLE,

    -- Erwartungswert (EMA) & Sentiment
    exp_return_30d   DOUBLE,
    sentiment_mean   DOUBLE,
    sentiment_norm   DOUBLE,

    -- Composite-Scores
    p_alpha          DOUBLE,
    alpha            DOUBLE NOT NULL,  -- ω [0,1]
    p_omega          DOUBLE,

    -- Audit
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_market_factors_daily PRIMARY KEY (asset_symbol, date),
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
);

CREATE INDEX idx_mf_daily_asset_date ON market_factors_daily(asset_symbol, date);
CREATE INDEX idx_mf_daily_palpha     ON market_factors_daily(p_alpha);
CREATE INDEX idx_mf_daily_pomega     ON market_factors_daily(p_omega);

COMMIT;