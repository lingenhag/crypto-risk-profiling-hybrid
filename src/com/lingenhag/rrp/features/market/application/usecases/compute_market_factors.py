# src/com/lingenhag/rrp/features/market/application/usecases/compute_market_factors.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import mean, pstdev, median
from typing import Dict, Iterable, List, Optional, Tuple

from com.lingenhag.rrp.features.market.application.ports import MarketRepositoryPort


# -----------------------------
# Domain DTO for factors
# -----------------------------
@dataclass(frozen=True)
class MarketFactorsDaily:
    asset_symbol: str
    day: date
    ret_1d: Optional[float]
    vol_30d: Optional[float]
    sharpe_30d: Optional[float]
    exp_return_30d: Optional[float]
    sentiment_mean: Optional[float]
    sentiment_norm: Optional[float]
    p_alpha: Optional[float]
    alpha: float
    # Extensions (already present in schema)
    sortino_30d: Optional[float] = None
    var_1d_95: Optional[float] = None


# -----------------------------
# Use Case Result
# -----------------------------
@dataclass(frozen=True)
class ComputeFactorsResult:
    rows: List[MarketFactorsDaily]
    inserted: int
    updated: int
    days_processed: int


class ComputeMarketFactors:
    """
    Berechnet tägliche Faktoren (Return, Vol/Sharpe/Sortino/VaR, EMA-ExpReturn),
    verknüpft sie mit Tages-Sentiment, normalisiert (Z-Score/Winsor/MinMax)
    und bildet den Pα-Score:
        Pα = (1 - α) * normalize(ExpReturn_30d) + α * Sentiment_norm

    Konfiguration:
      - window_vol: Fenster für Vol/Sharpe/Sortino/VaR
      - window_sent: Fenster für Sentiment-Normalisierung
      - ema_len: Länge der EMA für exp_return_30d
      - norm_method: "zscore" | "winsor" | "minmax"
      - winsor_alpha: Anteil für Winsorize-Clipping (z. B. 0.05)
      - var_method: "param95" | "emp95"
      - sentiment_weight: "none" | "count" | "domain_weight"
        ("count" nutzt Tages-Artikelanzahl als Gewichte in der Rolling-Norm,
         "domain_weight" nutzt v_daily_sentiment_weighted als Tageswert S(t) und
         N(t) als Evidenzgewicht)
      - article_weight_beta, article_weight_cap:
         Parameter für w_norm(t) = min((N/Median_N)^beta, cap) bei domain_weight.
    """

    def __init__(
            self,
            repo: MarketRepositoryPort,
            *,
            window_vol: int = 30,
            window_sent: int = 90,
            ema_len: int = 30,
            norm_method: str = "zscore",
            winsor_alpha: float = 0.05,
            var_method: str = "param95",
            sentiment_weight: str = "none",
            article_weight_beta: float = 0.5,
            article_weight_cap: float = 3.0,
    ) -> None:
        self.repo = repo
        self.window_vol = int(window_vol)
        self.window_sent = int(window_sent)
        self.ema_len = int(ema_len)
        self.norm_method = str(norm_method).lower()
        self.winsor_alpha = float(winsor_alpha)
        self.var_method = str(var_method).lower()
        self.sentiment_weight = str(sentiment_weight).lower()
        self.article_weight_beta = float(article_weight_beta)
        self.article_weight_cap = float(article_weight_cap)

    # ---------- helpers ----------
    @staticmethod
    def _ema(series: List[Optional[float]], length: int) -> List[Optional[float]]:
        if length <= 0:
            raise ValueError("EMA length must be positive")
        k = 2.0 / (length + 1.0)
        out: List[Optional[float]] = []
        ema_val: Optional[float] = None
        for v in series:
            if v is None:
                out.append(ema_val)
                continue
            if ema_val is None:
                ema_val = v
            else:
                ema_val = ema_val + k * (v - ema_val)
            out.append(ema_val)
        return out

    @staticmethod
    def _winsorize(values: List[float], alpha: float) -> List[float]:
        if not values or alpha <= 0.0:
            return list(values)
        xs = sorted(values)
        n = len(xs)
        lo_idx = max(0, min(n - 1, int(alpha * (n - 1))))
        hi_idx = max(0, min(n - 1, int((1.0 - alpha) * (n - 1))))
        lo = xs[lo_idx]
        hi = xs[hi_idx]
        return [min(hi, max(lo, v)) for v in values]

    @staticmethod
    def _weighted_stats(values: Iterable[float], weights: Iterable[float]) -> tuple[Optional[float], Optional[float]]:
        vs: List[float] = []
        ws: List[float] = []
        for v, w in zip(values, weights):
            if w is None or w <= 0 or v is None:
                continue
            vs.append(float(v))
            ws.append(float(w))
        if len(vs) == 0 or sum(ws) <= 0:
            return None, None
        w_sum = sum(ws)
        mu = sum(v * w for v, w in zip(vs, ws)) / w_sum
        var = sum(w * (v - mu) * (v - mu) for v, w in zip(vs, ws)) / w_sum
        sd = var ** 0.5
        return mu, sd

    def _rolling_var95(self, rets: List[Optional[float]], window: int, method: str) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        buf: List[float] = []
        m = method.lower()
        for r in rets:
            if r is not None:
                buf.append(r)
            if len(buf) > window:
                buf.pop(0)
            if len(buf) >= 2:
                if m == "emp95":
                    xs = sorted(buf)
                    if xs:
                        q_idx = max(0, int(0.05 * (len(xs) - 1)))
                        out.append(xs[q_idx])
                    else:
                        out.append(None)
                else:
                    mu = mean(buf)
                    sd = pstdev(buf)
                    out.append(mu - 1.65 * sd if sd is not None else None)
            else:
                out.append(None)
        return out

    def _rolling_sortino(self, rets: List[Optional[float]], window: int) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        buf: List[float] = []
        for r in rets:
            if r is not None:
                buf.append(r)
            if len(buf) > window:
                buf.pop(0)
            if len(buf) >= 2:
                mu = mean(buf)
                downs = [min(0.0, x) for x in buf]
                if all(d == 0.0 for d in downs):
                    out.append(None)
                else:
                    sd_down = (sum(d * d for d in downs) / len(downs)) ** 0.5
                    out.append(mu / sd_down if sd_down else None)
            else:
                out.append(None)
        return out

    def _normalize_series(
            self,
            series: List[Optional[float]],
            window: int,
            method: str,
            *,
            winsor_alpha: float = 0.05,
            weights: Optional[List[Optional[float]]] = None,
            min_points: int = 2,
    ) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        buf_vals: List[float] = []
        buf_wts: List[float] = []
        use_w = weights is not None and method in ("zscore", "winsor")

        for i, v in enumerate(series):
            w = weights[i] if (weights and i < len(weights)) else None

            if v is not None:
                buf_vals.append(v)
                if use_w:
                    buf_wts.append(float(w) if (w is not None and w > 0) else 0.0)

            if len(buf_vals) > window:
                buf_vals.pop(0)
                if use_w and buf_wts:
                    buf_wts.pop(0)

            if len(buf_vals) >= min_points:
                if method == "minmax":
                    mn = min(buf_vals)
                    mx = max(buf_vals)
                    if mn == mx or v is None:
                        out.append(None)
                    else:
                        mm = (v - mn) / (mx - mn)
                        out.append(mm * 2.0 - 1.0)
                else:
                    vals = list(buf_vals)
                    if method == "winsor":
                        vals = self._winsorize(vals, winsor_alpha)
                    if use_w:
                        mu, sd = self._weighted_stats(vals, buf_wts)
                        if mu is None or sd is None or not sd or v is None:
                            out.append(None)
                        else:
                            x_eff = min(max(v, min(vals)), max(vals)) if method == "winsor" else v
                            out.append((x_eff - mu) / sd)
                    else:
                        mu = mean(vals)
                        sd = pstdev(vals)
                        if sd and sd != 0 and v is not None:
                            x_eff = min(max(v, min(vals)), max(vals)) if method == "winsor" else v
                            out.append((x_eff - mu) / sd)
                        else:
                            out.append(None)
            else:
                out.append(None)
        return out

    @staticmethod
    def _build_article_weights_counts(stats: Dict[date, int], days: List[date]) -> List[Optional[float]]:
        return [float(stats.get(d, 0)) if stats.get(d, 0) is not None else None for d in days]

    def _build_article_weights_normed(self, stats: Dict[date, int], days: List[date]) -> List[Optional[float]]:
        """w_norm(t) = min((N/median_N)^beta, cap) mit defensiven Defaults."""
        ns = [float(stats.get(d, 0) or 0.0) for d in days]
        pos = [x for x in ns if x > 0]
        med = median(pos) if pos else 0.0
        out: List[Optional[float]] = []
        for n in ns:
            if n <= 0:
                out.append(0.0)
                continue
            base = (n / med) if med > 0 else 1.0
            weight = min(base ** self.article_weight_beta, self.article_weight_cap)
            out.append(weight)
        return out

    # ---------- main ----------
    def execute(
            self,
            *,
            asset_symbol: str,
            start: date,
            end: date,
            alpha: float = 0.25,
            persist: bool = True,
    ) -> ComputeFactorsResult:
        # 1) Returns laden
        returns: List[Tuple[date, Optional[float]]] = self.repo.fetch_daily_returns(asset_symbol, start, end)
        days = [d for d, _ in returns]
        ret_vals = [r for _, r in returns]

        # 2) Rolling Risk: Vol/Sharpe/Sortino
        vol_30: List[Optional[float]] = []
        sharpe_30: List[Optional[float]] = []
        window_vals: List[float] = []
        for r in ret_vals:
            if r is not None:
                window_vals.append(r)
            if len(window_vals) > self.window_vol:
                window_vals.pop(0)
            if len(window_vals) >= 2:
                m = mean(window_vals)
                sd = pstdev(window_vals)
                vol_30.append(sd if sd != 0 else None)
                sharpe_30.append((m / sd) if sd not in (0, None) else None)
            else:
                vol_30.append(None)
                sharpe_30.append(None)

        sortino_30 = self._rolling_sortino(ret_vals, self.window_vol)

        # 3) VaR95
        var_1d_95 = self._rolling_var95(ret_vals, self.window_vol, self.var_method)

        # 4) ExpReturn (EMA)
        exp_return = self._ema(ret_vals, self.ema_len)

        # 5) Sentiment-Serie + Gewichte
        if self.sentiment_weight == "domain_weight":
            sent_map: Dict[date, Optional[float]] = {}
            try:
                # bevorzugt gewichtete View
                sent_map = self.repo.fetch_daily_sentiment(asset_symbol, start, end)  # fallback init
                # Wenn Repo eine gewichtete Methode anbietet, try/except in Use-Case:
                if hasattr(self.repo, "fetch_daily_sentiment_weighted"):  # type: ignore[attr-defined]
                    sent_map = getattr(self.repo, "fetch_daily_sentiment_weighted")(asset_symbol, start, end)  # type: ignore[call-arg]
            except Exception:
                # Fallback: ungewichtetes Sentiment
                sent_map = self.repo.fetch_daily_sentiment(asset_symbol, start, end)

            sentiment_mean: List[Optional[float]] = [sent_map.get(d) for d in days]

            # Evidenzgewichte N(t)
            stats: Dict[date, int] = {}
            if hasattr(self.repo, "fetch_daily_sentiment_stats"):
                try:
                    stats = getattr(self.repo, "fetch_daily_sentiment_stats")(asset_symbol, start, end)  # type: ignore[call-arg]
                except Exception:
                    stats = {}
            weights = self._build_article_weights_normed(stats, days)

        else:
            # "count" oder "none"
            sent_map = self.repo.fetch_daily_sentiment(asset_symbol, start, end)
            sentiment_mean = [sent_map.get(d) for d in days]

            weights = None
            if self.sentiment_weight == "count":
                stats: Dict[date, int] = {}
                if hasattr(self.repo, "fetch_daily_sentiment_stats"):
                    try:
                        stats = getattr(self.repo, "fetch_daily_sentiment_stats")(asset_symbol, start, end)  # type: ignore[call-arg]
                    except Exception:
                        stats = {}
                weights = self._build_article_weights_counts(stats, days)

        # 6) Normalisierung
        sentiment_norm = self._normalize_series(
            sentiment_mean,
            self.window_sent,
            self.norm_method,
            winsor_alpha=self.winsor_alpha,
            weights=weights,
            min_points=2,
        )
        exp_return_norm = self._normalize_series(
            exp_return,
            self.window_sent,
            "zscore",
            winsor_alpha=self.winsor_alpha,
            weights=None,
            min_points=2,
        )

        # 7) Pα
        p_alpha: List[Optional[float]] = []
        for er_n, s_n in zip(exp_return_norm, sentiment_norm):
            if er_n is None and s_n is None:
                p_alpha.append(None)
            elif er_n is None:
                p_alpha.append(s_n)
            elif s_n is None:
                p_alpha.append(er_n)
            else:
                p_alpha.append((1.0 - alpha) * er_n + alpha * s_n)

        # 8) Persist/Result
        rows: List[MarketFactorsDaily] = []
        for i, d in enumerate(days):
            rows.append(
                MarketFactorsDaily(
                    asset_symbol=asset_symbol,
                    day=d,
                    ret_1d=ret_vals[i],
                    vol_30d=vol_30[i],
                    sharpe_30d=sharpe_30[i],
                    exp_return_30d=exp_return[i],
                    sentiment_mean=sentiment_mean[i],
                    sentiment_norm=sentiment_norm[i],
                    p_alpha=p_alpha[i],
                    alpha=alpha,
                    sortino_30d=sortino_30[i],
                    var_1d_95=var_1d_95[i],
                )
            )

        inserted, updated = (0, 0)
        if persist:
            inserted, updated = self.repo.upsert_factors(rows)

        return ComputeFactorsResult(
            rows=rows,
            inserted=inserted,
            updated=updated,
            days_processed=len(rows),
        )