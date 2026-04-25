"""
Economic Releases with BLS API for CPI/PPI/NFP, consensus deviation scoring.
"""

import asyncio
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from loguru import logger

try:
    import aiohttp
    _AIOHTTP = True
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP = False


class IndicatorType(Enum):
    CPI = "CPI"
    PPI = "PPI"
    NFP = "NFP"
    GDP = "GDP"
    RETAIL_SALES = "RETAIL_SALES"
    PMI = "PMI"
    UNEMPLOYMENT = "UNEMPLOYMENT"


@dataclass
class EconomicRelease:
    release_id: str
    indicator_type: IndicatorType
    title: str
    release_date: datetime
    actual: Optional[float] = None
    consensus: Optional[float] = None
    previous: Optional[float] = None
    deviation: Optional[float] = None
    deviation_pct: Optional[float] = None
    importance: int = 3
    source: str = "BLS"


_BLS_SERIES_MAP: dict[str, IndicatorType] = {
    "CUUR0000SA0": IndicatorType.CPI,
    "WPSFD49207": IndicatorType.PPI,
    "CES0000000001": IndicatorType.NFP,
}


class EconomicReleases:
    """
    Economic releases manager with BLS API integration,
    consensus deviation calculation, and historical tracking.
    """

    BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def __init__(self, enable_paper_mode: bool = True):
        self.paper_mode = enable_paper_mode
        self.releases: list[EconomicRelease] = []
        self._http_session: aiohttp.ClientSession | None = None

    async def initialize(self) -> None:
        if _AIOHTTP:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )
        elif not self.paper_mode:
            logger.warning("aiohttp not installed — falling back to paper mode for EconomicReleases")
            self.paper_mode = True

        await self._load_historical_releases()
        await self._load_scheduled_releases()

        logger.info("Loaded {} economic releases (paper_mode={})", len(self.releases), self.paper_mode)

    async def _load_historical_releases(self) -> None:
        if self.paper_mode:
            self._seed_paper_releases()
            return

        try:
            now = datetime.now(tz=timezone.utc)
            series_ids = list(_BLS_SERIES_MAP.keys())
            payload = {
                "seriesid": series_ids,
                "startyear": str(now.year - 2),
                "endyear": str(now.year),
            }
            async with self._http_session.post(self.BLS_API_URL, json=payload) as response:
                if response.status != 200:
                    logger.warning("BLS API returned status {} — using paper fallback", response.status)
                    self._seed_paper_releases()
                    return
                data = await response.json(content_type=None)
                if not isinstance(data, dict):
                    logger.warning("BLS API returned unexpected data type — using paper fallback")
                    self._seed_paper_releases()
                    return
                self._process_bls_data(data)
                if not self.releases:
                    logger.warning("BLS API returned no usable data — using paper fallback")
                    self._seed_paper_releases()

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("BLS API unreachable ({}), using paper fallback", exc)
            self._seed_paper_releases()
        except Exception as exc:
            logger.error("Unexpected error loading economic releases: {}", exc)
            self._seed_paper_releases()

    def _seed_paper_releases(self) -> None:
        now = datetime.now(tz=timezone.utc)
        self.releases.extend([
            EconomicRelease(
                release_id=f"cpi_{i}",
                indicator_type=IndicatorType.CPI,
                title=f"Consumer Price Index {i}",
                release_date=now - timedelta(days=30 * i),
                actual=3.0 + (i % 3) * 0.1,
                consensus=3.1,
                previous=3.0 + ((i - 1) % 3) * 0.1,
                importance=5,
                source="paper",
            )
            for i in range(1, 7)
        ])

    def _process_bls_data(self, data: dict) -> None:
        results = data.get("Results", {})
        if not isinstance(results, dict):
            return
        for series in results.get("series", []):
            series_id = series.get("seriesID", "")
            indicator = _BLS_SERIES_MAP.get(series_id)
            if not indicator:
                continue

            prev_value: float | None = None
            for item in series.get("data", []):
                try:
                    year = int(item["year"])
                    period = item.get("period", "")
                    if period.startswith("M"):
                        month = int(period[1:])
                    elif period.startswith("Q"):
                        month = int(period[1:]) * 3
                    else:
                        continue

                    release_date = datetime(year, month, 1, tzinfo=timezone.utc)
                    value = float(item["value"])

                    release = EconomicRelease(
                        release_id=f"{series_id}_{year}_{month}",
                        indicator_type=indicator,
                        title=f"{indicator.value} {year}-{month:02d}",
                        release_date=release_date,
                        actual=value,
                        previous=prev_value,
                        importance=5,
                        source="BLS",
                    )
                    self.releases.append(release)
                    prev_value = value

                except (ValueError, TypeError, KeyError):
                    continue

    async def _load_scheduled_releases(self) -> None:
        now = datetime.now(tz=timezone.utc)
        scheduled = [
            (IndicatorType.CPI, "Consumer Price Index", 5),
            (IndicatorType.PPI, "Producer Price Index", 4),
            (IndicatorType.NFP, "Non-Farm Payrolls", 5),
            (IndicatorType.PMI, "Manufacturing PMI", 4),
        ]

        existing_upcoming_types = {
            release.indicator_type
            for release in self.releases
            if release.release_date > now
        }

        for i, (indicator, title, importance) in enumerate(scheduled):
            if indicator in existing_upcoming_types:
                continue
            release = EconomicRelease(
                release_id=f"{indicator.value.lower()}_{i}",
                indicator_type=indicator,
                title=title,
                release_date=now + timedelta(days=7 + i * 5),
                importance=importance,
                source="scheduled",
            )
            self.releases.append(release)

    async def calculate_consensus_deviation(
        self,
        release_id: str,
        consensus: float,
    ) -> Optional[float]:
        for release in self.releases:
            if release.release_id == release_id and release.actual is not None:
                deviation = release.actual - consensus
                release.consensus = consensus
                release.deviation = deviation
                release.deviation_pct = (deviation / consensus * 100) if consensus != 0 else None
                return deviation
        return None

    def get_upcoming_releases(
        self,
        days: int = 14,
        min_importance: int = 3,
    ) -> list[EconomicRelease]:
        now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(days=days)
        return [
            release
            for release in self.releases
            if now < release.release_date <= cutoff
            and release.importance >= min_importance
        ]

    def get_recent_releases(self, days: int = 30) -> list[EconomicRelease]:
        now = datetime.now(tz=timezone.utc)
        cutoff = now - timedelta(days=days)
        return [
            release
            for release in self.releases
            if cutoff <= release.release_date <= now
        ]

    def get_high_deviation_releases(
        self,
        threshold_pct: float = 10.0,
        days: int = 90,
    ) -> list[EconomicRelease]:
        now = datetime.now(tz=timezone.utc)
        cutoff = now - timedelta(days=days)
        return [
            release
            for release in self.releases
            if release.release_date >= cutoff
            and release.deviation_pct is not None
            and abs(release.deviation_pct) >= threshold_pct
        ]

    def get_indicator_impact_score(self, indicator_type: IndicatorType) -> float:
        impact_scores = {
            IndicatorType.CPI: 0.95,
            IndicatorType.NFP: 0.93,
            IndicatorType.PPI: 0.85,
            IndicatorType.GDP: 0.90,
            IndicatorType.RETAIL_SALES: 0.80,
            IndicatorType.PMI: 0.75,
            IndicatorType.UNEMPLOYMENT: 0.88,
        }
        return impact_scores.get(indicator_type, 0.70)

    async def close(self) -> None:
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
