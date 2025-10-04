# src/com/lingenhag/rrp/platform/monitoring/metrics.py
from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server


class Metrics:
    def __init__(self, port: int = 8000):
        # ---- Generic API metrics (existing) ----
        self.api_requests_total = Counter(
            "api_requests_total",
            "Total number of API requests",
            ["client", "status"],
        )
        self.api_request_duration_seconds = Histogram(
            "api_request_duration_seconds",
            "API request duration in seconds",
            ["client"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf")),
        )
        self.harvest_duration_seconds = Histogram(
            "harvest_duration_seconds",
            "Duration of URL harvesting in seconds",
            ["asset_symbol"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf")),
        )
        self.summarize_duration_seconds = Histogram(
            "summarize_duration_seconds",
            "Duration of article summarization in seconds",
            ["asset_symbol", "mode"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf")),
        )
        self.compute_factors_duration_seconds = Histogram(
            "compute_factors_duration_seconds",
            "Duration of factors computation in seconds",
            ["asset_symbol"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf")),
        )

        # ---- NEWS-specific metrics (NEW) ----
        # Label-Set: asset, source/resolver, outcome
        self.news_source_fetch_total = Counter(
            "news_source_fetch_total",
            "Outcome counter for news source fetches (per asset/source/outcome).",
            ["source", "asset", "outcome"],
        )
        self.news_source_fetch_duration_seconds = Histogram(
            "news_source_fetch_duration_seconds",
            "Duration of news source fetch calls (per source).",
            ["source"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf")),
        )
        self.news_resolver_total = Counter(
            "news_resolver_total",
            "Outcome counter for URL resolver (per resolver/asset/outcome).",
            ["resolver", "asset", "outcome"],
        )
        self.news_resolver_duration_seconds = Histogram(
            "news_resolver_duration_seconds",
            "Duration of URL resolver operations (per resolver).",
            ["resolver"],
            buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, float("inf")),
        )

        self._port = port
        self._started = False

    # ---- Server lifecycle ----
    def start_server(self) -> None:
        if not self._started:
            start_http_server(self._port)
            self._started = True
            print(f"[monitoring] Prometheus metrics server started on port {self._port}")

    # ---- Existing helpers (backward-compat) ----
    def track_api_request(self, client: str, status: str) -> None:
        self.api_requests_total.labels(client=client, status=status).inc()

    def track_api_duration(self, client: str, duration: float) -> None:
        self.api_request_duration_seconds.labels(client=client).observe(duration)

    def track_harvest_duration(self, asset_symbol: str, duration: float) -> None:
        self.harvest_duration_seconds.labels(asset_symbol=asset_symbol).observe(duration)

    def track_summarize_duration(self, asset_symbol: str, mode: str, duration: float) -> None:
        self.summarize_duration_seconds.labels(asset_symbol=asset_symbol, mode=mode).observe(duration)

    def track_compute_factors_duration(self, asset_symbol: str, duration: float) -> None:
        self.compute_factors_duration_seconds.labels(asset_symbol=asset_symbol).observe(duration)

    # ---- NEWS-specific helpers (NEW) ----
    def track_news_source_fetch(self, *, source: str, asset: str, outcome: str) -> None:
        self.news_source_fetch_total.labels(source=source, asset=asset, outcome=outcome).inc()

    def track_news_source_duration(self, *, source: str, duration: float) -> None:
        self.news_source_fetch_duration_seconds.labels(source=source).observe(duration)

    def track_news_resolver(self, *, resolver: str, asset: str, outcome: str) -> None:
        self.news_resolver_total.labels(resolver=resolver, asset=asset, outcome=outcome).inc()

    def track_news_resolver_duration(self, *, resolver: str, duration: float) -> None:
        self.news_resolver_duration_seconds.labels(resolver=resolver).observe(duration)