# src/com/lingenhag/rrp/features/market/infrastructure/coingecko_client.py
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from com.lingenhag.rrp.domain.models import MarketSnapshot
from com.lingenhag.rrp.features.market.application.ports import MarketDataPort
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics

_LOG = logging.getLogger(__name__)


def _maybe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _pct_to_float(v: Any) -> Optional[float]:
    return _maybe_float(v)


def _classify_endpoint_hint(status_code: int, body_text: str) -> Optional[str]:
    txt = (body_text or "").lower()
    if status_code == 400 and ("10010" in txt or "pro api key" in txt):
        return "use_pro"
    if status_code == 400 and ("10011" in txt or "demo api key" in txt):
        return "use_public"
    return None


@dataclass(frozen=True)
class CoinGeckoClient(MarketDataPort):
    api_base: str = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
    api_key: Optional[str] = os.getenv("COINGECKO_API_KEY")
    timeout: int = 20
    max_retries: int = 3
    initial_backoff: float = 1.0
    metrics: Optional[Metrics] = None

    def _bases_for_key(self) -> tuple[str, str]:
        # (public, pro)
        return ("https://api.coingecko.com/api/v3", "https://pro-api.coingecko.com/api/v3")

    def _headers(self, use_pro: bool) -> Dict[str, str]:
        headers = {
            "User-Agent": "com.lingenhag.rrp/1.0 coingecko-client",
            "Accept": "application/json",
        }
        if use_pro and self.api_key:
            headers["x-cg-pro-api-key"] = self.api_key
        return headers

    def _request(self, method: str, path: str, params: Dict[str, Any]) -> Any:
        public_base, pro_base = self._bases_for_key()
        use_pro = bool(self.api_key)
        backoff = self.initial_backoff

        for attempt in range(1, self.max_retries + 1):
            base = pro_base if use_pro else public_base
            url = f"{base.rstrip('/')}/{path.lstrip('/')}"
            start_time = time.time()
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=self._headers(use_pro=use_pro),
                    timeout=self.timeout,
                )
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < self.max_retries:
                        _LOG.warning(
                            "CoinGecko %s %s -> HTTP %s; retry in %.1fs (attempt %s/%s)",
                            method,
                            resp.url,
                            resp.status_code,
                            backoff,
                            attempt,
                            self.max_retries,
                        )
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                if resp.status_code >= 400:
                    body_text = resp.text[:600].replace("\n", " ") if resp.text else "<no body>"
                    hint = _classify_endpoint_hint(resp.status_code, body_text)
                    if hint == "use_pro" and not use_pro:
                        _LOG.info("CoinGecko hint suggests Pro endpoint; switching to Pro.")
                        use_pro = True
                        continue
                    if hint == "use_public" and use_pro:
                        _LOG.info("CoinGecko hint suggests Public endpoint; switching to Public.")
                        use_pro = False
                        continue
                    if self.metrics:
                        self.metrics.track_api_request("coingecko", "error")
                        self.metrics.track_api_duration("coingecko", time.time() - start_time)
                    _LOG.error(
                        "CoinGecko error %s for %s (params=%s): %s",
                        resp.status_code,
                        resp.url,
                        params,
                        body_text,
                    )
                    resp.raise_for_status()
                data = resp.json()
                if self.metrics:
                    self.metrics.track_api_request("coingecko", "success")
                    self.metrics.track_api_duration("coingecko", time.time() - start_time)
                return data
            except requests.RequestException as e:
                if self.metrics:
                    self.metrics.track_api_request("coingecko", "error")
                    self.metrics.track_api_duration("coingecko", time.time() - start_time)
                if attempt >= self.max_retries:
                    raise
                _LOG.warning(
                    "CoinGecko request error '%s' (attempt %s/%s); retry in %.1fs",
                    e,
                    attempt,
                    self.max_retries,
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("Retries exhausted without return/raise.")

    def fetch_spot(self, provider_ids: List[str], vs_currency: str = "usd") -> List[MarketSnapshot]:
        if not provider_ids:
            raise ValueError("provider_ids darf nicht leer sein")
        if not vs_currency or not vs_currency.strip():
            raise ValueError("vs_currency darf nicht leer sein")
        ids_str = ",".join(sorted(set(provider_ids)))
        params = {
            "vs_currency": vs_currency.lower(),
            "ids": ids_str,
            "order": "market_cap_desc",
            "per_page": max(1, min(len(set(provider_ids)), 250)),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d",
        }
        _LOG.info(
            "CoinGecko fetch_spot(ids=%s, vs=%s, per_page=%s)",
            ids_str,
            params["vs_currency"],
            params["per_page"],
        )
        data = self._request("GET", "/coins/markets", params)
        observed_at = datetime.now(timezone.utc)
        out: List[MarketSnapshot] = []
        for item in data or []:
            symbol_upper = (item.get("symbol") or "").upper()
            out.append(
                MarketSnapshot(
                    asset_symbol=symbol_upper or (item.get("id") or "").upper(),
                    price=float(item.get("current_price")) if item.get("current_price") is not None else 0.0,
                    market_cap=_maybe_float(item.get("market_cap")),
                    volume_24h=_maybe_float(item.get("total_volume")),
                    change_1h=_pct_to_float(item.get("price_change_percentage_1h_in_currency")),
                    change_24h=_pct_to_float(item.get("price_change_percentage_24h_in_currency")),
                    change_7d=_pct_to_float(item.get("price_change_percentage_7d_in_currency")),
                    observed_at=observed_at,
                    source="CoinGecko",
                )
            )
        return out

    def fetch_history_range(
            self,
            provider_id: str,
            vs_currency: str,
            ts_from: int,
            ts_to: int,
            granularity: str = "hourly",
    ) -> List[MarketSnapshot]:
        if not provider_id:
            raise ValueError("provider_id darf nicht leer sein")
        if not vs_currency:
            raise ValueError("vs_currency darf nicht leer sein")
        params = {
            "vs_currency": vs_currency.lower(),
            "from": int(ts_from),
            "to": int(ts_to),
        }
        _LOG.info(
            "CoinGecko fetch_history_range(id=%s, vs=%s, from=%s, to=%s)",
            provider_id,
            params["vs_currency"],
            params["from"],
            params["to"],
        )
        data = self._request("GET", f"/coins/{provider_id}/market_chart/range", params)
        prices = data.get("prices") or []
        market_caps = data.get("market_caps") or []
        total_volumes = data.get("total_volumes") or []
        mcap_map = {int(x[0]): _maybe_float(x[1]) for x in market_caps if isinstance(x, list) and len(x) == 2}
        vol_map = {int(x[0]): _maybe_float(x[1]) for x in total_volumes if isinstance(x, list) and len(x) == 2}
        out: List[MarketSnapshot] = []
        for p in prices:
            if not isinstance(p, list) or len(p) != 2:
                continue
            ts_ms, price = int(p[0]), _maybe_float(p[1])
            observed_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            out.append(
                MarketSnapshot(
                    asset_symbol=(provider_id or "").upper(),
                    price=price or 0.0,
                    market_cap=mcap_map.get(ts_ms),
                    volume_24h=vol_map.get(ts_ms),
                    change_1h=None,
                    change_24h=None,
                    change_7d=None,
                    observed_at=observed_at,
                    source="CoinGecko",
                )
            )
        return out