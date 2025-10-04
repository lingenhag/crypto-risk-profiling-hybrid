-- src/com/lingenhag/rrp/persistence/migrations/006_market_factors_daily.sql
-- =============================================================================
-- Erstellt Tabelle market_factors_daily für ComputeMarketFactors-Upserts.
-- Primärschlüssel: (asset_symbol, date)
-- Enthält Sharpe/Sortino/VaR/ExpReturn sowie (normalisiertes) Sentiment & Pα.
-- Erweiterung: p_omega + Trigger für Auto-Compute: (1-alpha)*p_alpha + alpha*sentiment_norm
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
    p_omega          DOUBLE,           -- Auto-computed via Trigger

    -- Audit
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_market_factors_daily PRIMARY KEY (asset_symbol, date),
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
);

-- Trigger: Auto-compute p_omega und update updated_at
CREATE OR REPLACE FUNCTION compute_p_omega() RETURNS TRIGGER AS $$
BEGIN
    NEW.p_omega := (1 - NEW.alpha) * COALESCE(NEW.p_alpha, 0) + NEW.alpha * COALESCE(NEW.sentiment_norm, 0);
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_compute_p_omega
    BEFORE INSERT OR UPDATE ON market_factors_daily
    FOR EACH ROW EXECUTE FUNCTION compute_p_omega();

CREATE INDEX idx_mf_daily_asset_date ON market_factors_daily(asset_symbol, date);
CREATE INDEX idx_mf_daily_palpha     ON market_factors_daily(p_alpha);
CREATE INDEX idx_mf_daily_pomega     ON market_factors_daily(p_omega);

COMMIT;