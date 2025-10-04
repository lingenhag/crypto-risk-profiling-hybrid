# src/com/lingenhag/rrp/domain/models.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import List, Optional, Set
from enum import Enum

# -----------------------------
# Core Domain Models
# -----------------------------
@dataclass(frozen=True)
class CryptoAsset:
    symbol: str
    name: str
    aliases: Set[str] = field(default_factory=set)

    def __post_init__(self):
        if not self.symbol.isupper():
            raise ValueError("Symbol must be uppercase")

@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def duration(self) -> timedelta:
        return self.end - self.start

    def split(self, max_span: timedelta) -> List["TimeRange"]:
        ranges: List[TimeRange] = []
        current_start = self.start
        while current_start < self.end:
            current_end = min(current_start + max_span, self.end)
            ranges.append(TimeRange(start=current_start, end=current_end))
            current_start = current_end
        return ranges

@dataclass(frozen=True)
class SearchCriteria:
    asset: CryptoAsset
    time_range: TimeRange
    languages: Set[str] = field(default_factory=lambda: {"en"})
    limit: int = 100

    def keywords(self) -> Set[str]:
        return {self.asset.symbol, self.asset.name, *self.asset.aliases}

# -----------------------------
# Legacy Article Models
# -----------------------------
@dataclass(frozen=True)
class Article:
    url: str
    title: str
    content: Optional[str] = None
    source: Optional[str] = None
    published_at: Optional[datetime] = None
    asset_symbol: str = ""
    raw_meta: dict = field(default_factory=dict)
    ingested_at: Optional[datetime] = None

@dataclass(frozen=True)
class SentimentDecision:
    label: str  # e.g., "positive", "negative", "neutral"
    score: float  # [-1, 1]
    confidence: Optional[float] = None
    summary: Optional[str] = None
    model: str = ""

    def __post_init__(self):
        if not -1 <= self.score <= 1:
            raise ValueError("Sentiment score must be in [-1, 1]")

@dataclass(frozen=True)
class RelevanceDecision:
    is_relevant: bool
    score: float  # [0, 1]
    reason: str
    model: str = ""

    def __post_init__(self):
        if not 0 <= self.score <= 1:
            raise ValueError("Relevance score must be in [0, 1]")

@dataclass
class NewsResults:
    asset: CryptoAsset
    time_range: TimeRange
    articles: List[Article] = field(default_factory=list)
    rejections: List[dict] = field(default_factory=list)

    def count(self) -> int:
        return len(self.articles)

    def aggregate_sentiment(self) -> Optional[float]:
        if not self.articles:
            return None
        # Placeholder: Average sentiment from decisions (extend with actual logic)
        return 0.0

# -----------------------------
# URL Harvesting
# -----------------------------
@dataclass(frozen=True)
class UrlHarvest:
    url: str
    asset_symbol: str
    source: Optional[str] = None
    published_at: Optional[datetime] = None
    title: Optional[str] = None
    discovered_at: Optional[datetime] = None

@dataclass(frozen=True)
class HarvestSummary:
    total_docs: int
    after_assemble: int
    after_dedupe: int
    saved: int
    skipped_duplicates: int
    rejected_invalid: int

# -----------------------------
# Summarized Article (Fixed: All non-defaults first, then defaults)
# -----------------------------
@dataclass(frozen=True)
class SummarizedArticle:
    url: str
    summary: str
    asset_symbol: str
    source: Optional[str] = None
    model: str = ""
    sentiment: Optional[float] = None  # [-1, 1]
    published_at: Optional[datetime] = None
    ingested_at: Optional[datetime] = None

    def __post_init__(self):
        if self.sentiment is not None and not -1 <= self.sentiment <= 1:
            raise ValueError("Sentiment must be in [-1, 1]")

# -----------------------------
# Market Data (Fixed: Non-default 'observed_at' before defaults)
# -----------------------------
@dataclass(frozen=True)
class MarketSnapshot:
    asset_symbol: str
    price: float
    observed_at: datetime
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    change_1h: Optional[float] = None
    change_24h: Optional[float] = None
    change_7d: Optional[float] = None
    source: str = "CoinGecko"

@dataclass(frozen=True)
class DailyCandle:
    asset_symbol: str
    provider: str
    provider_id: str
    vs_currency: str
    day: date
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    market_cap: Optional[float] = None
    volume: Optional[float] = None
    source: str = "CoinGecko"

# -----------------------------
# Risk Factors (Generic)
# -----------------------------
class RiskFactorType(Enum):
    RETURN_1D = "return_1d"
    VOL_30D = "vol_30d"
    SHARPE_30D = "sharpe_30d"
    SORTINO_30D = "sortino_30d"
    VAR_95 = "var_95"
    EXP_RETURN_30D = "exp_return_30d"

@dataclass(frozen=True)
class RiskFactor:
    type: RiskFactorType
    value: Optional[float] = None
    normalized: Optional[float] = None  # z-Score or Winsorized

    def z_score(self, mean: float, std: float) -> float:
        if self.value is None or std == 0:
            return 0.0
        return (self.value - mean) / std

# -----------------------------
# Market Factors (Persisted) - Extended
# -----------------------------
@dataclass(frozen=True)
class MarketFactorsDaily:
    asset_symbol: str
    day: date
    ret_1d: Optional[float] = None
    vol_30d: Optional[float] = None
    sharpe_30d: Optional[float] = None
    sortino_30d: Optional[float] = None
    var_95: Optional[float] = None  # Value-at-Risk 95%
    exp_return_30d: Optional[float] = None
    sentiment_mean: Optional[float] = None  # Aggregated sentiment
    sentiment_norm: Optional[float] = None  # Normalized [-1,1]
    p_alpha: Optional[float] = None  # Quantitative score
    alpha: Optional[float] = None  # Sentiment weight ω [0,1]
    p_omega: Optional[float] = None  # Integrated: (1-ω)*p_alpha + ω*sentiment_norm

    def __post_init__(self):
        if self.p_omega is None and self.p_alpha is not None and self.alpha is not None and self.sentiment_norm is not None:
            # Auto-compute P_ω if missing (Bericht 8.2)
            object.__setattr__(self, 'p_omega', (1 - self.alpha) * self.p_alpha + self.alpha * self.sentiment_norm)

# -----------------------------
# POmega Score (Value Object)
# -----------------------------
@dataclass(frozen=True)
class POmegaScore:
    quantitative_score: float  # p_alpha: z-scored factors
    sentiment_score: float  # Normalized sentiment
    omega: float  # Weight [0,1]
    integrated_value: float  # P_ω = (1-ω)*quant + ω*sent

    def __post_init__(self):
        if not 0 <= self.omega <= 1:
            raise ValueError("Omega must be in [0, 1]")
        object.__setattr__(self, 'integrated_value', (1 - self.omega) * self.quantitative_score + self.omega * self.sentiment_score)

    def sensitivity_to_omega(self) -> dict:
        """Bericht 8.3: Sensitivity analysis"""
        return {
            "baseline": self.integrated_value,
            "omega_0": self.quantitative_score,
            "omega_1": self.sentiment_score,
            "delta": self.sentiment_score - self.quantitative_score
        }