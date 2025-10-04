# tests/features/news/test_harvest_urls.py
from unittest.mock import Mock, patch
import pytest
from datetime import datetime, timezone
from com.lingenhag.rrp.features.news.application.usecases.harvest_urls import HarvestUrls, is_valid_news_url
from com.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO, NewsSourcePort
from com.lingenhag.rrp.domain.models import HarvestSummary
# No conftest import – fixtures auto-injected


def test_is_valid_news_url():
    assert is_valid_news_url("https://test.com/article.html") is True
    assert is_valid_news_url("https://test.com/image.jpg") is False
    assert is_valid_news_url("ftp://invalid") is False
    assert is_valid_news_url("") is False


def test_harvest_urls_basic_flow(mock_news_source, mock_repo, mock_domain_policy, sample_criteria):
    sources = [mock_news_source]
    svc = HarvestUrls(sources=sources, repo=mock_repo, domain_policy=mock_domain_policy)

    summary = svc.run(criteria=sample_criteria, verbose=False)

    mock_news_source.fetch_documents.assert_called_once_with(sample_criteria)
    mock_repo.save_url_harvest.assert_called_once()
    mock_domain_policy.is_allowed.assert_called_once()
    mock_domain_policy.record_harvest.assert_called_once()

    assert summary.total_docs == 1
    assert summary.after_assemble == 1
    assert summary.after_dedupe == 1
    assert summary.saved == 1
    assert summary.skipped_duplicates == 0
    assert summary.rejected_invalid == 0


def test_harvest_urls_duplicate(mock_news_source, mock_repo, mock_domain_policy, sample_criteria):
    mock_repo.save_url_harvest.return_value = (1, True)  # Duplicate
    sources = [mock_news_source]
    svc = HarvestUrls(sources=sources, repo=mock_repo, domain_policy=mock_domain_policy)

    summary = svc.run(criteria=sample_criteria, verbose=False)

    assert summary.saved == 0
    assert summary.skipped_duplicates == 1


def test_harvest_urls_invalid_url(mock_news_source, mock_repo, mock_domain_policy, sample_criteria):
    mock_news_source.fetch_documents.return_value = [
        {"url": "https://test.com/image.jpg", "title": "Invalid"}
    ]
    sources = [mock_news_source]
    svc = HarvestUrls(sources=sources, repo=mock_repo, domain_policy=mock_domain_policy)

    summary = svc.run(criteria=sample_criteria, verbose=False)

    mock_repo.save_url_harvest.assert_not_called()
    assert summary.rejected_invalid == 1
    assert summary.after_assemble == 0


def test_harvest_urls_enforce_domain_filter_reject(mock_news_source, mock_repo, mock_domain_policy, sample_criteria):
    mock_domain_policy.is_allowed.return_value = False
    sources = [mock_news_source]
    svc = HarvestUrls(sources=sources, repo=mock_repo, domain_policy=mock_domain_policy, enforce_domain_filter=True)

    summary = svc.run(criteria=sample_criteria, verbose=False)

    assert summary.rejected_invalid == 1
    assert summary.after_assemble == 0


def test_harvest_urls_multiple_sources(mock_news_source, mock_repo, mock_domain_policy, sample_criteria):
    source2 = Mock(spec=NewsSourcePort)
    source2.SOURCE_NAME = "source2"
    source2.fetch_documents.return_value = [
        {"url": "https://test.com/2", "title": "Test2", "published_at": "2025-10-01T00:00:00Z"}
    ]
    sources = [mock_news_source, source2]
    svc = HarvestUrls(sources=sources, repo=mock_repo, domain_policy=mock_domain_policy)

    summary = svc.run(criteria=sample_criteria, verbose=False)

    assert summary.total_docs == 2
    assert mock_repo.save_url_harvest.call_count == 2