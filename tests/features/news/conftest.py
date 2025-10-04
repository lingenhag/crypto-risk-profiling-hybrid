# tests/features/news/conftest.py
import os
import tempfile
import pytest
from unittest.mock import Mock
from datetime import datetime, timezone
from com.lingenhag.rrp.features.news.application.ports import (
    NewsSourcePort, NewsRepositoryPort, DomainPolicyPort, HarvestCriteriaDTO
)
from com.lingenhag.rrp.features.news.application.usecases.harvest_urls import HarvestUrls
from com.lingenhag.rrp.domain.models import UrlHarvest, HarvestSummary
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import DuckDBNewsRepository
import duckdb


@pytest.fixture
def mock_news_source() -> NewsSourcePort:
    source = Mock(spec=NewsSourcePort)
    source.SOURCE_NAME = "test_source"
    source.fetch_documents.return_value = [
        {"url": "https://test.com/1", "title": "Test1", "published_at": "2025-10-01T00:00:00Z"}
    ]
    return source


@pytest.fixture
def mock_repo() -> NewsRepositoryPort:
    repo = Mock(spec=NewsRepositoryPort)
    repo.save_url_harvest.return_value = (1, False)
    repo.save_rejection.return_value = 1
    repo.now_utc.return_value = datetime.now(timezone.utc)
    return repo


@pytest.fixture
def mock_domain_policy() -> DomainPolicyPort:
    policy = Mock(spec=DomainPolicyPort)
    policy.is_allowed.return_value = True
    return policy


@pytest.fixture
def sample_criteria():
    return HarvestCriteriaDTO(
        asset_symbol="BTC",
        start=datetime(2025, 10, 1, tzinfo=timezone.utc),
        end=datetime(2025, 10, 2, tzinfo=timezone.utc),
        limit=10
    )


@pytest.fixture
def in_memory_repo():
    # Make a temp directory and point DuckDB at a file that doesn't exist yet
    tmpdir = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmpdir.name, "test.duckdb")
    repo = DuckDBNewsRepository(db_path)
    try:
        with repo._connect() as con:
            # Assets
            con.execute("CREATE SEQUENCE assets_seq START 1;")
            con.execute("""
                        CREATE TABLE assets (
                                                id INTEGER PRIMARY KEY DEFAULT nextval('assets_seq'),
                                                symbol TEXT UNIQUE NOT NULL,
                                                name TEXT NOT NULL
                        );
                        """)
            con.execute("INSERT INTO assets VALUES (nextval('assets_seq'), 'BTC', 'Bitcoin');")
            # URL Harvests
            con.execute("CREATE SEQUENCE url_harvests_seq START 1;")
            con.execute("""
                        CREATE TABLE url_harvests (
                                                      id INTEGER PRIMARY KEY DEFAULT nextval('url_harvests_seq'),
                                                      source TEXT,
                                                      url TEXT NOT NULL,
                                                      asset_symbol TEXT NOT NULL,
                                                      published_at TIMESTAMP,
                                                      title TEXT,
                                                      discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                      FOREIGN KEY (asset_symbol) REFERENCES assets(symbol)
                        );
                        """)
            con.execute("CREATE UNIQUE INDEX uq_url_harvest_url_asset ON url_harvests(url, asset_symbol);")
            # Summarized Articles
            con.execute("CREATE SEQUENCE summarized_articles_seq START 1;")
            con.execute("""
                        CREATE TABLE summarized_articles (
                                                             id INTEGER PRIMARY KEY DEFAULT nextval('summarized_articles_seq'),
                                                             url TEXT NOT NULL,
                                                             published_at TIMESTAMP,
                                                             summary TEXT,
                                                             asset_symbol TEXT NOT NULL,
                                                             source TEXT,
                                                             ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                             model TEXT NOT NULL,
                                                             sentiment DOUBLE,
                                                             FOREIGN KEY (asset_symbol) REFERENCES assets(symbol),
                                                             CONSTRAINT uq_summarized_url_asset UNIQUE (url, asset_symbol)
                        );
                        """)
            # Rejections
            con.execute("CREATE SEQUENCE rejections_seq START 1;")
            con.execute("""
                        CREATE TABLE rejections (
                                                    id INTEGER PRIMARY KEY DEFAULT nextval('rejections_seq'),
                                                    url TEXT,
                                                    asset_symbol TEXT NOT NULL,
                                                    reason TEXT,
                                                    source TEXT,
                                                    context TEXT NOT NULL,
                                                    article_id INTEGER,
                                                    model TEXT,
                                                    details JSON,
                                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        """)
        yield repo
    finally:
        tmpdir.cleanup()