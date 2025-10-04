# tests/features/news/test_news_query_builder.py
from unittest.mock import Mock
import pytest
from com.lingenhag.rrp.features.news.application.news_query_builder import NewsQueryBuilder, _quote_if_phrase_or_proper
from com.lingenhag.rrp.features.news.application.ports_asset_registry import AssetRegistryPort
from com.lingenhag.rrp.features.news.application.news_query_builder import QueryBuildParams


def test_quote_if_phrase_or_proper():
    assert _quote_if_phrase_or_proper("BTC") == "BTC"
    assert _quote_if_phrase_or_proper("Bitcoin") == '"Bitcoin"'
    assert _quote_if_phrase_or_proper("Solana Labs") == '"Solana Labs"'
    assert _quote_if_phrase_or_proper("") == ""


@pytest.fixture
def mock_registry():
    registry = Mock(spec=AssetRegistryPort)
    registry.get_aliases.return_value = ["Solana Labs"]
    registry.get_negative_terms.return_value = ["solar", "peru"]
    return registry


def test_news_query_builder_core(mock_registry):
    builder = NewsQueryBuilder(asset_registry=mock_registry)
    query = builder.build_core_boolean("SOL")

    # Expected: (SOL OR sol OR "Solana Labs") AND (crypto OR ...) NOT (solar OR peru)
    # Actual has mixed case – check lowercase
    lower_query = query.lower()
    assert "sol or solana or \"solana labs\"" in lower_query
    assert "crypto or cryptocurrency" in lower_query
    assert "not (solar or peru)" in lower_query


def test_news_query_builder_no_context(mock_registry):
    mock_registry.get_aliases.return_value = []
    # Create new params instance to avoid frozen error
    params = QueryBuildParams(require_crypto_context=False)
    builder = NewsQueryBuilder(asset_registry=mock_registry, params=params)
    query = builder.build_core_boolean("BTC")

    assert "Bitcoin" in query  # Hard synonym
    assert "crypto" not in query  # No context


def test_news_query_builder_gdelt(mock_registry):
    builder = NewsQueryBuilder(asset_registry=mock_registry)
    query = builder.build_for_gdelt("DOT")

    assert "Polkadot" in query  # Synonym


def test_news_query_builder_rss(mock_registry):
    builder = NewsQueryBuilder(asset_registry=mock_registry)
    query = builder.build_for_rss("ETH", "2025-10-01", "2025-10-02")

    assert "after:2025-10-01 before:2025-10-02" in query
    assert "Ethereum" in query