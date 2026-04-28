"""Unit tests for FedCalendar behavior."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from data_ingestion.fed_calendar import FedCalendar, FedEvent, FedEventType


@pytest.mark.asyncio
async def test_initialize_builds_event_index_for_countdown() -> None:
    calendar = FedCalendar(enable_paper_mode=True)
    await calendar.initialize()
    try:
        assert calendar.events
        first = calendar.events[0]
        remaining = calendar.get_countdown(first.event_id)
        assert remaining is not None
    finally:
        await calendar.close()


def test_is_fomc_week_uses_next_upcoming_event() -> None:
    calendar = FedCalendar(enable_paper_mode=True)
    now = datetime.now(tz=timezone.utc)
    calendar.events = [
        FedEvent(
            event_id="past_fomc",
            event_type=FedEventType.FOMC_DECISION,
            title="Past FOMC",
            description="Already happened",
            date=now - timedelta(days=20),
            importance=5,
            expected_impact="HIGH",
        ),
        FedEvent(
            event_id="next_fomc",
            event_type=FedEventType.FOMC_DECISION,
            title="Upcoming FOMC",
            description="Next meeting",
            date=now + timedelta(days=2),
            importance=5,
            expected_impact="HIGH",
        ),
    ]

    assert calendar.is_fomc_week() is True
