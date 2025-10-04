# tests/features/news/test_duckdb_news_repository.py
import pytest
from datetime import datetime, timezone
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import DuckDBNewsRepository
# No import for in_memory_repo – fixture auto-injected


def test_save_url_harvest_new(in_memory_repo):
    repo = in_memory_repo
    now = datetime.now(timezone.utc)
    id_, is_dup = repo.save_url_harvest(
        url="https://test.com/new",
        asset_symbol="BTC",
        source="test",
        published_at=now,
        title="New Article"
    )

    assert id_ > 0
    assert not is_dup

    # Verify inserted
    with repo._connect() as con:
        row = con.execute("SELECT url, asset_symbol FROM url_harvests WHERE id = ?", (id_,)).fetchone()
        assert row[0] == "https://test.com/new"
        assert row[1] == "BTC"


def test_save_url_harvest_duplicate(in_memory_repo):
    repo = in_memory_repo
    # First insert
    repo.save_url_harvest(url="https://test.com/dup", asset_symbol="BTC", source=None, published_at=None, title=None)
    id_, is_dup = repo.save_url_harvest(url="https://test.com/dup", asset_symbol="BTC", source=None, published_at=None, title=None)

    assert id_ > 0
    assert is_dup  # Duplicate detected


def test_save_rejection(in_memory_repo):
    repo = in_memory_repo
    id_ = repo.save_rejection(
        url="https://test.com/reject",
        asset_symbol="BTC",
        reason="Irrelevant",
        source="test",
        context="harvest"
    )

    assert id_ > 0

    # Verify
    with repo._connect() as con:
        row = con.execute("SELECT reason FROM rejections WHERE id = ?", (id_,)).fetchone()
        assert row[0] == "Irrelevant"


def test_fetch_url_harvest_batch(in_memory_repo):
    repo = in_memory_repo
    # Insert two
    repo.save_url_harvest(url="https://test.com/1", asset_symbol="BTC", source=None, published_at=None, title=None)
    repo.save_url_harvest(url="https://test.com/2", asset_symbol="BTC", source=None, published_at=None, title=None)

    batch = repo.fetch_url_harvest_batch("BTC", limit=2)

    assert len(batch) == 2
    assert batch[0]["url"] == "https://test.com/1"