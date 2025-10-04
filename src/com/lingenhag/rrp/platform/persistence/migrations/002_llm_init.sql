-- src/com/lingenhag/rrp/platform/persistence/migrations/002_llm_init.sql
-- =============================================================================
-- 002_llm_init.sql – DuckDB-Hinweise:
--  * Sequences statt GENERATED IDENTITY.
--  * CURRENT_TIMESTAMP als Default-Zeit.
--  * UNIQUE-Keys vorhanden; in App-Schicht Upsert per INSERT OR IGNORE + UPDATE.
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------
-- Drop für Dev (nur Feature-Scope)
-- -----------------------------
DROP TABLE IF EXISTS llm_votes;
DROP TABLE IF EXISTS summarized_articles;

DROP SEQUENCE IF EXISTS llm_votes_seq;
DROP SEQUENCE IF EXISTS summarized_articles_seq;

-- -----------------------------
-- Sequences
-- -----------------------------
CREATE SEQUENCE summarized_articles_seq START 1;
CREATE SEQUENCE llm_votes_seq          START 1;

-- -----------------------------
-- summarized_articles
-- -----------------------------
CREATE TABLE summarized_articles
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('summarized_articles_seq'),
    url          TEXT NOT NULL,
    published_at TIMESTAMP,
    summary      TEXT,
    asset_symbol TEXT NOT NULL,
    source       TEXT,
    ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model        TEXT NOT NULL,      -- z. B. "gpt-5" oder "ensemble[...]"
    sentiment    DOUBLE,             -- -1..1, auf 2 Dezimal gerundet
    FOREIGN KEY (asset_symbol) REFERENCES assets(symbol),
    CONSTRAINT uq_summarized_url_asset UNIQUE (url, asset_symbol)
);
CREATE INDEX idx_summarized_asset     ON summarized_articles(asset_symbol);
CREATE INDEX idx_summarized_published ON summarized_articles(published_at);

-- -----------------------------
-- llm_votes (Audit-Einzelvotes)
-- -----------------------------
CREATE TABLE llm_votes
(
    id           INTEGER PRIMARY KEY DEFAULT nextval('llm_votes_seq'),
    url          TEXT,               -- nullable; bei relevanten Artikeln meist NULL + article_id gesetzt
    asset_symbol TEXT NOT NULL,
    model        TEXT NOT NULL,      -- gpt-5, gemini-2.5-flash, grok-4
    relevance    BOOLEAN NOT NULL,
    sentiment    DOUBLE,             -- -1..1 (optional; 2 Dezimal)
    summary      TEXT,               -- Kurz-Zusammenfassung des Voters
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    harvest_id   INTEGER,            -- Referenz url_harvests.id (optional)
    article_id   INTEGER,            -- Referenz summarized_articles.id (bei relevanten Artikeln)
    CONSTRAINT chk_llm_votes_url_or_article CHECK (url IS NOT NULL OR article_id IS NOT NULL)
);
-- Mehrfach-NULL in UNIQUE-Key ist in DuckDB erlaubt → Auditfälle ohne URL sind ok.
CREATE UNIQUE INDEX uq_llm_votes_url_asset_model ON llm_votes(url, asset_symbol, model);
CREATE INDEX idx_llm_votes_asset     ON llm_votes(asset_symbol);
CREATE INDEX idx_llm_votes_created   ON llm_votes(created_at);
CREATE INDEX idx_llm_votes_harvest   ON llm_votes(harvest_id);
CREATE INDEX idx_llm_votes_article   ON llm_votes(article_id);

COMMIT;