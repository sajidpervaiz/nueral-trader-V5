"""Unit tests for EconomicReleases schedule fallback behavior."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from data_ingestion.economic_releases import EconomicReleases, EconomicRelease, IndicatorType


@pytest.mark.asyncio
async def test_scheduled_releases_added_in_non_paper_mode() -> None:
    releases = EconomicReleases(enable_paper_mode=False)

    # Simulate historical-only state: no upcoming entries.
    releases.releases = [
        EconomicRelease(
            release_id="hist_cpi",
            indicator_type=IndicatorType.CPI,
            title="Historical CPI",
            release_date=datetime.now(tz=timezone.utc) - timedelta(days=30),
            actual=3.0,
            importance=5,
        )
    ]

    await releases._load_scheduled_releases()

    upcoming = [r for r in releases.releases if r.release_date > datetime.now(tz=timezone.utc)]
    upcoming_types = {r.indicator_type for r in upcoming}

    assert IndicatorType.CPI in upcoming_types
    assert IndicatorType.PPI in upcoming_types
    assert IndicatorType.NFP in upcoming_types
    assert IndicatorType.PMI in upcoming_types
