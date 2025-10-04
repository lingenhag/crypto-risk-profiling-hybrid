-- src/com/lingenhag/rrp/platform/persistence/migrations/003_market_init.sql
-- =============================================================================
-- 003_market_init.sql – DuckDB-Hinweise:
--  * Sequences statt GENERATED IDENTITY
--  * CURRENT_TIMESTAMP / NOW() sind ok; wir nutzen CURRENT_TIMESTAMP
--  * Views nach Tabellen definieren; arg_min/arg_max mit GROUP BY verwenden
--  * Erweiterung: Domain-gewichtetes Tages-Sentiment & Count-View
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------
-- Drop für Dev (nur Feature-Scope)
-- -----------------------------
DROP VIEW IF EXISTS v_market_sentiment_daily;
DROP VIEW IF EXISTS v_daily_sentiment;
DROP VIEW IF EXISTS v_daily_from_snapshots;

-- Erweiterte Sentiment-Views
DROP VIEW IF EXISTS v_daily_sentiment_with_counts;
DROP VIEW IF EXISTS v_daily_sentiment_weighted;
DROP VIEW IF EXISTS v_news_domain_weights;

DROP TABLE IF EXISTS market_history;
DROP TABLE IF EXISTS market_snapshots;
DROP TABLE IF EXISTS asset_providers;

DROP SEQUENCE IF EXISTS market_history_seq;
DROP SEQUENCE IF EXISTS market_snapshots_seq;
DROP SEQUENCE IF EXISTS asset_providers_seq;

-- -----------------------------
-- Sequences
-- -----------------------------
CREATE SEQUENCE asset_providers_seq  START 1;
CREATE SEQUENCE market_snapshots_seq START 1;
CREATE SEQUENCE market_history_seq   START 1;

-- -----------------------------
-- Provider-Mapping
-- -----------------------------
CREATE TABLE asset_providers
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('asset_providers_seq'),
    asset_symbol TEXT NOT NULL,
    provider     TEXT NOT NULL,
    provider_id  TEXT NOT NULL,
    UNIQUE (asset_symbol, provider)
);
CREATE INDEX idx_asset_providers_provider ON asset_providers(provider);

-- -----------------------------
-- Hochfrequente Rohdaten (Snapshots)
-- -----------------------------
CREATE TABLE market_snapshots
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('market_snapshots_seq'),
    asset_symbol TEXT NOT NULL,
    price        DOUBLE NOT NULL,
    market_cap   DOUBLE,
    volume_24h   DOUBLE,
    change_1h    DOUBLE,
    change_24h   DOUBLE,
    change_7d    DOUBLE,
    observed_at  TIMESTAMP NOT NULL,
    source       TEXT NOT NULL DEFAULT 'CoinGecko',
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
);
CREATE UNIQUE INDEX uq_market_snapshots_asset_observed_source
    ON market_snapshots(asset_symbol, observed_at, source);
CREATE INDEX idx_market_snapshots_asset_time
    ON market_snapshots(asset_symbol, observed_at);

-- -----------------------------
-- Tages-Candles (History)
-- -----------------------------
CREATE TABLE market_history
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('market_history_seq'),
    asset_symbol TEXT NOT NULL,
    provider     TEXT NOT NULL DEFAULT 'CoinGecko',
    provider_id  TEXT NOT NULL,
    vs_currency  TEXT NOT NULL DEFAULT 'usd',
    date         DATE NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    market_cap   DOUBLE,
    volume       DOUBLE,
    source       TEXT NOT NULL DEFAULT 'CoinGecko',
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
);
CREATE UNIQUE INDEX ux_market_history_asset_day
    ON market_history(asset_symbol, provider, vs_currency, date);
CREATE INDEX idx_market_history_asset_time
    ON market_history(asset_symbol, date);

-- -----------------------------
-- Views (nach Tabellen anlegen)
-- -----------------------------
-- Intraday → Daily-Rollup aus Snapshots (für QA/Backtests)
CREATE VIEW v_daily_from_snapshots AS
SELECT
    ms.asset_symbol,
    CAST(ms.observed_at AS DATE) AS day,
    arg_min(ms.price, ms.observed_at)      AS open,
    max(ms.price)                          AS high,
    min(ms.price)                          AS low,
    arg_max(ms.price, ms.observed_at)      AS close,
    arg_max(ms.market_cap, ms.observed_at) AS market_cap,
    avg(ms.volume_24h)                     AS volume
FROM market_snapshots AS ms
GROUP BY ms.asset_symbol, CAST(ms.observed_at AS DATE);

-- Tägliches Sentiment je Asset (nur wenn Sentiment vorhanden)
CREATE VIEW v_daily_sentiment AS
SELECT
    sa.asset_symbol,
    CAST(sa.published_at AS DATE) AS day,
    avg(sa.sentiment) AS avg_sentiment,
    count(*)          AS n_articles
FROM summarized_articles AS sa
WHERE sa.sentiment IS NOT NULL
GROUP BY sa.asset_symbol, CAST(sa.published_at AS DATE);

-- -----------------------------
-- Erweiterung: Domain-Gewichte & gewichtetes Tages-Sentiment
-- Benötigt: news_domain_stats(asset_symbol, domain, harvested_total, stored_total, llm_accepted, llm_rejected)
-- -----------------------------

-- Hybridgewicht je (asset_symbol, domain) mit Laplace-Smoothing & Median-Skalierung
CREATE VIEW v_news_domain_weights AS
WITH raw AS (
    SELECT
        nds.asset_symbol,
        nds.domain,
        COALESCE(nds.llm_accepted, 0) AS acc,
        COALESCE(nds.stored_total, 0) AS stored
    FROM news_domain_stats AS nds
),
     medians AS (
         SELECT
             asset_symbol,
             MEDIAN(acc)    AS med_acc,
             MEDIAN(stored) AS med_store
         FROM raw
         GROUP BY asset_symbol
     ),
     scaled AS (
         SELECT
             r.asset_symbol,
             r.domain,
             (r.acc    + 3.0) / COALESCE(NULLIF(m.med_acc,   0) + 3.0, 1.0) AS acc_norm,
             (r.stored + 3.0) / COALESCE(NULLIF(m.med_store, 0) + 3.0, 1.0) AS store_norm
         FROM raw r
                  LEFT JOIN medians m ON m.asset_symbol = r.asset_symbol
     ),
     hybrid AS (
         -- α=0.6 – qualitative Evidenz (acc) etwas stärker als quantitative (stored)
         SELECT
             asset_symbol,
             domain,
             0.6 * acc_norm + 0.4 * store_norm AS w_raw
         FROM scaled
     )
SELECT
    asset_symbol,
    domain,
    GREATEST(0.7, LEAST(1.3, w_raw)) AS weight  -- Caps 0.7..1.3
FROM hybrid;

-- Tages-Sentiment domain-gewichtet
CREATE VIEW v_daily_sentiment_weighted AS
WITH base AS (
    SELECT
        sa.asset_symbol,
        CAST(sa.published_at AS DATE) AS day,
        sa.source       AS domain,
        sa.sentiment
    FROM summarized_articles AS sa
    WHERE sa.sentiment IS NOT NULL
),
     joined AS (
         SELECT
             b.asset_symbol,
             b.day,
             b.sentiment,
             COALESCE(w.weight, 1.0) AS w
         FROM base b
                  LEFT JOIN v_news_domain_weights w
                            ON w.asset_symbol = b.asset_symbol
                                AND w.domain = b.domain
     )
SELECT
    asset_symbol,
    day,
    CASE WHEN SUM(w) = 0 THEN NULL
         ELSE SUM(sentiment * w) / SUM(w)
        END AS avg_sentiment_weighted,
    COUNT(*) AS n_articles
FROM joined
GROUP BY asset_symbol, day
ORDER BY asset_symbol, day;

-- Tages-Sentiment mit Artikelanzahl (ungewichtet) – für Count-Gewichte in der App
CREATE VIEW v_daily_sentiment_with_counts AS
SELECT
    sa.asset_symbol,
    CAST(sa.published_at AS DATE) AS day,
    AVG(sa.sentiment) AS avg_sentiment,
    COUNT(*)          AS n_articles
FROM summarized_articles AS sa
WHERE sa.sentiment IS NOT NULL
GROUP BY sa.asset_symbol, CAST(sa.published_at AS DATE)
ORDER BY sa.asset_symbol, day;

-- Join-View: Market (History) × (ungewichtetes) Sentiment
CREATE VIEW v_market_sentiment_daily AS
SELECT
    mh.asset_symbol,
    mh.date AS day,
    mh.open, mh.high, mh.low, mh.close, mh.market_cap, mh.volume,
    ds.avg_sentiment,
    ds.n_articles
FROM market_history AS mh
         LEFT JOIN v_daily_sentiment AS ds
                   ON ds.asset_symbol = mh.asset_symbol
                       AND ds.day = mh.date;

-- -----------------------------
-- Seeds (Provider-Mapping)
-- -----------------------------
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('BTC', 'CoinGecko', 'bitcoin')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('ETH', 'CoinGecko', 'ethereum')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('DOT', 'CoinGecko', 'polkadot')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('SOL', 'CoinGecko', 'solana')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('XRP', 'CoinGecko', 'ripple')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('ADA', 'CoinGecko', 'cardano')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;
INSERT INTO asset_providers(asset_symbol, provider, provider_id)
VALUES ('BNB', 'CoinGecko', 'binancecoin')
ON CONFLICT (asset_symbol, provider) DO UPDATE SET provider_id = excluded.provider_id;

COMMIT;