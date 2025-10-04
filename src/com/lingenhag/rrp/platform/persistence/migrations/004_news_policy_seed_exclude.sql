-- src/com/lingenhag/rrp/platform/persistence/migrations/004_news_policy_seed_exclude.sql
-- =============================================================================
-- Sperrt "news.google.com" pauschal für alle Assets in der Domain-Policy.
-- Hintergrund: Google-News-Wrapper sind nicht LLM-tauglich; wir wollen
-- nur voll aufgelöste Publisher-Domains verarbeiten.
-- Idempotent via ON CONFLICT.
-- =============================================================================

BEGIN TRANSACTION;

INSERT INTO news_domain_policy (asset_symbol, domain, allowed)
SELECT
    a.symbol,
    'news.google.com' AS domain,
    FALSE             AS allowed
FROM assets AS a
ON CONFLICT (asset_symbol, domain)
    DO UPDATE SET
                  allowed    = EXCLUDED.allowed,
                  updated_at = now();

COMMIT;