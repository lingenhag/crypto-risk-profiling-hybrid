-- src/com/lingenhag/rrp/platform/persistence/migrations/001_news_init.sql
-- =============================================================================
-- DuckDB-Dialekt – Kurzleitfaden für Migrationen (aus unseren Fix-Runden)
-- -----------------------------------------------------------------------------
-- 1) IDs / Auto-Increment:
--    * KEIN "GENERATED AS IDENTITY" in DuckDB. Stattdessen:
--        CREATE SEQUENCE my_seq START 1;
--        CREATE TABLE t (..., id INTEGER PRIMARY KEY DEFAULT nextval('my_seq'), ...);
--
-- 2) Upsert-Strategien:
--    * DuckDBs "ON CONFLICT" braucht ein eindeutiges Ziel, sonst Binder-Fehler.
--      Wenn mehrere UNIQUE/PKs existieren, nutze:
--        - "INSERT OR IGNORE" + anschliessendes "UPDATE", oder
--        - "MERGE" (wenn ein eindeutiger Match-Key vorhanden ist).
--    * Beispiel (einfacher Schlüssel):
--        INSERT INTO assets(symbol, name) VALUES ('BTC','Bitcoin')
--        ON CONFLICT (symbol) DO UPDATE SET name=excluded.name;
--
-- 3) Zeitfunktionen:
--    * CURRENT_TIMESTAMP ist portabel; NOW() geht in DuckDB meist auch.
--    * Wir speichern TIMESTAMPs UTC-naiv; Konvertierung macht die App-Schicht.
--
-- 4) Views / Aggregationen:
--    * Views NACH allen benötigten Tabellen anlegen.
--    * arg_min/arg_max funktionieren in DuckDB, benötigen korrektes GROUP BY.
--
-- 5) IN-Listen mit Parametern:
--    * Für "col IN (?)" mit Parametern nutze:
--        WHERE col IN (SELECT value FROM UNNEST(?))
--
-- 6) JSON:
--    * DuckDB unterstützt JSON; für Audit/Details-Felder unkritisch.
--
-- 7) Portabilität & „Dev-Drop“-Sicherheit:
--    * Wenn Tabellen FK-abhängig von "assets" sind (z. B. summarized_articles,
--      market_history, market_snapshots), MÜSSEN diese zuerst gedroppt werden.
--      Darum enthält diese Migration bewusst alle relevanten DROPs,
--      auch wenn sie in anderen Files wieder angelegt werden.
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------
-- Drop für Dev (Feature-übergreifend, in FK-sicherer Reihenfolge)
-- -----------------------------
-- Zuerst Views, die auf Tabellen referenzieren
DROP VIEW IF EXISTS v_market_sentiment_daily;
DROP VIEW IF EXISTS v_daily_sentiment;
DROP VIEW IF EXISTS v_daily_from_snapshots;
DROP VIEW IF EXISTS v_daily_returns;

-- Dann alle Tabellen, die (direkt/indirekt) von assets abhängen
DROP TABLE IF EXISTS llm_votes;
DROP TABLE IF EXISTS summarized_articles;

DROP TABLE IF EXISTS market_history;
DROP TABLE IF EXISTS market_snapshots;
DROP TABLE IF EXISTS asset_providers;

-- News/Harvest-Teil
DROP TABLE IF EXISTS rejections;
DROP TABLE IF EXISTS url_harvests;
DROP TABLE IF EXISTS news_domain_stats;
DROP TABLE IF EXISTS news_domain_policy;

-- Registry-Tabellen, die vor assets kommen sollten
DROP TABLE IF EXISTS asset_aliases;
DROP TABLE IF EXISTS asset_negative_terms;

-- Zuletzt assets (FK-Haupttabelle)
DROP TABLE IF EXISTS assets;

-- Sequences zum Schluss (idempotent)
DROP SEQUENCE IF EXISTS llm_votes_seq;
DROP SEQUENCE IF EXISTS summarized_articles_seq;

DROP SEQUENCE IF EXISTS market_history_seq;
DROP SEQUENCE IF EXISTS market_snapshots_seq;
DROP SEQUENCE IF EXISTS asset_providers_seq;

DROP SEQUENCE IF EXISTS rejections_seq;
DROP SEQUENCE IF EXISTS url_harvests_seq;
DROP SEQUENCE IF EXISTS assets_seq;

DROP SEQUENCE IF EXISTS asset_aliases_seq;
DROP SEQUENCE IF EXISTS asset_negative_terms_seq;

-- -----------------------------
-- Sequences
-- -----------------------------
CREATE SEQUENCE assets_seq           START 1;
CREATE SEQUENCE url_harvests_seq     START 1;
CREATE SEQUENCE rejections_seq       START 1;

CREATE SEQUENCE asset_aliases_seq        START 1;
CREATE SEQUENCE asset_negative_terms_seq START 1;

-- -----------------------------
-- Core: Assets & URL-Harvest
-- -----------------------------
CREATE TABLE assets
(
    id     INTEGER PRIMARY KEY DEFAULT nextval('assets_seq'),
    symbol TEXT UNIQUE NOT NULL,
    name   TEXT NOT NULL
);

CREATE TABLE url_harvests
(
    id            INTEGER PRIMARY KEY DEFAULT nextval('url_harvests_seq'),
    source        TEXT,
    url           TEXT NOT NULL,
    asset_symbol  TEXT NOT NULL,
    published_at  TIMESTAMP,
    title         TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
);
CREATE UNIQUE INDEX uq_url_harvest_url_asset ON url_harvests(url, asset_symbol);
CREATE INDEX idx_url_harvests_asset_symbol   ON url_harvests(asset_symbol);
CREATE INDEX idx_url_harvests_published_at   ON url_harvests(published_at);
CREATE INDEX idx_url_harvests_discovered_at  ON url_harvests(discovered_at);

-- -----------------------------
-- Rejections (Harvest/Summarize-Ablehnungen)
-- -----------------------------
CREATE TABLE rejections
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('rejections_seq'),
    url          TEXT,
    asset_symbol TEXT NOT NULL,
    source       TEXT,
    reason       TEXT,
    context      TEXT NOT NULL,      -- 'harvest' | 'summarize' | ...
    article_id   INTEGER,            -- optional: Falls Ablehnung *nach* Artikel
    model        TEXT,               -- z. B. 'ensemble' (Summarize) oder NULL (Harvest)
    details      JSON,               -- Optional: kompakte Votes/Diagnose
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_rejections_asset   ON rejections(asset_symbol);
CREATE INDEX idx_rejections_created ON rejections(created_at);
CREATE INDEX idx_rejections_reason  ON rejections(reason);

-- -----------------------------
-- Domain-Policy & Stats
-- -----------------------------
CREATE TABLE IF NOT EXISTS news_domain_policy
(
    asset_symbol TEXT NOT NULL,
    domain       TEXT NOT NULL,
    allowed      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_news_domain_policy PRIMARY KEY (asset_symbol, domain)
);
CREATE INDEX IF NOT EXISTS idx_news_domain_policy_asset   ON news_domain_policy(asset_symbol);
CREATE INDEX IF NOT EXISTS idx_news_domain_policy_allowed ON news_domain_policy(allowed);

CREATE TABLE IF NOT EXISTS news_domain_stats
(
    asset_symbol     TEXT NOT NULL,
    domain           TEXT NOT NULL,
    harvested_total  BIGINT NOT NULL DEFAULT 0,
    stored_total     BIGINT NOT NULL DEFAULT 0,
    llm_accepted     BIGINT NOT NULL DEFAULT 0,
    llm_rejected     BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT pk_news_domain_stats PRIMARY KEY (asset_symbol, domain)
);
CREATE INDEX IF NOT EXISTS idx_news_domain_stats_asset ON news_domain_stats(asset_symbol);
CREATE INDEX IF NOT EXISTS idx_news_domain_stats_hv    ON news_domain_stats(harvested_total, stored_total);
CREATE INDEX IF NOT EXISTS idx_news_domain_stats_llm   ON news_domain_stats(llm_accepted, llm_rejected);

-- -----------------------------
-- Query-Registry (Aliases / Negative Terms)
-- -----------------------------
CREATE TABLE IF NOT EXISTS asset_aliases
(
    id     INTEGER PRIMARY KEY DEFAULT nextval('asset_aliases_seq'),
    symbol TEXT NOT NULL,
    alias  TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_aliases_symbol_alias
    ON asset_aliases(symbol, alias);

CREATE TABLE IF NOT EXISTS asset_negative_terms
(
    id     INTEGER PRIMARY KEY DEFAULT nextval('asset_negative_terms_seq'),
    symbol TEXT NOT NULL,
    term   TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_negative_terms_symbol_term
    ON asset_negative_terms(symbol, term);

-- -----------------------------
-- Seeds (Assets & Registry)
-- -----------------------------
INSERT INTO assets(symbol, name) VALUES ('BTC', 'Bitcoin')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('ETH', 'Ethereum')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('DOT', 'Polkadot')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('SOL', 'Solana')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('XRP', 'XRP')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('ADA', 'Cardano')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;
INSERT INTO assets(symbol, name) VALUES ('BNB', 'BNB')
ON CONFLICT (symbol) DO UPDATE SET name = excluded.name;

-- Registry Seeds
INSERT INTO asset_aliases(symbol, alias) VALUES ('SOL', 'Solana')               ON CONFLICT (symbol, alias) DO NOTHING;
INSERT INTO asset_aliases(symbol, alias) VALUES ('SOL', 'Solana Foundation')    ON CONFLICT (symbol, alias) DO NOTHING;
INSERT INTO asset_aliases(symbol, alias) VALUES ('SOL', 'Solana Labs')          ON CONFLICT (symbol, alias) DO NOTHING;

INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'solar')                   ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'el sol')                  ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'peru')                    ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'moneda')                  ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'peruvian sol')            ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'hotel')                   ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'cerveza')                 ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'energy')                  ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'kwh')                     ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'sunscreen')               ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'sol de mexico')           ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('SOL', 'statute of limitations')  ON CONFLICT (symbol, term) DO NOTHING;

INSERT INTO asset_aliases(symbol, alias) VALUES ('BTC', 'Bitcoin') ON CONFLICT (symbol, alias) DO NOTHING;

INSERT INTO asset_negative_terms(symbol, term) VALUES ('BTC', 'bitcoin pizza') ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('BTC', 'bitcoin era')   ON CONFLICT (symbol, term) DO NOTHING;
INSERT INTO asset_negative_terms(symbol, term) VALUES ('BTC', 'bitcoin prime') ON CONFLICT (symbol, term) DO NOTHING;

INSERT INTO asset_aliases(symbol, alias) VALUES ('ETH', 'Ethereum') ON CONFLICT (symbol, alias) DO NOTHING;

INSERT INTO asset_aliases(symbol, alias) VALUES ('DOT', 'Polkadot')        ON CONFLICT (symbol, alias) DO NOTHING;
INSERT INTO asset_aliases(symbol, alias) VALUES ('DOT', 'Web3 Foundation') ON CONFLICT (symbol, alias) DO NOTHING;

COMMIT;