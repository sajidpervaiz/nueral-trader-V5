"""
Fed Calendar integration with FOMC dates, Fed speeches, and event countdowns.
"""

import asyncio
import time
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


class FedEventType(Enum):
    FOMC_DECISION = "FOMC_DECISION"
    FOMC_MINUTES = "FOMC_MINUTES"
    FED_SPEECH = "FED_SPEECH"
    INTERVENTION = "INTERVENTION"
    TESTIMONY = "TESTIMONY"


@dataclass
class FedEvent:
    event_id: str
    event_type: FedEventType
    title: str
    description: str
    date: datetime
    importance: int  # 1-5 scale
    expected_impact: str
    speaker: Optional[str] = None
    location: Optional[str] = None


class FedCalendar:
    """
    Fed Calendar manager with FOMC date tracking,
    Fed speech monitoring, event countdowns, and impact assessment.
    """

    FOMC_API_URL = "https://api.federalreserve.gov/v1/fomc/meetings"
    SPEECHES_URL = "https://www.federalreserve.gov/json/speeches.json"

    def __init__(self, enable_paper_mode: bool = True):
        self.paper_mode = enable_paper_mode
        self.events: list[FedEvent] = []
        self.event_index: dict[str, FedEvent] = {}
        self._http_session: aiohttp.ClientSession | None = None

    async def initialize(self) -> None:
        if _AIOHTTP:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )
        elif not self.paper_mode:
            logger.warning("aiohttp not installed — falling back to paper mode for FedCalendar")
            self.paper_mode = True

        await self._load_fomc_dates()
        await self._load_scheduled_speeches()
        self._rebuild_index()

        logger.info("Loaded {} Fed events (paper_mode={})", len(self.events), self.paper_mode)

    def _rebuild_index(self) -> None:
        self.event_index = {event.event_id: event for event in self.events}

    async def _load_fomc_dates(self) -> None:
        if self.paper_mode:
            self._seed_paper_fomc()
            return

        try:
            async with self._http_session.get(self.FOMC_API_URL) as response:
                if response.status != 200:
                    logger.warning("FOMC API returned status {} — using paper fallback", response.status)
                    self._seed_paper_fomc()
                    return
                data = await response.json(content_type=None)
                if not isinstance(data, dict):
                    logger.warning("FOMC API returned unexpected data type — using paper fallback")
                    self._seed_paper_fomc()
                    return

                meetings = data.get("meetings", [])
                if not isinstance(meetings, list):
                    self._seed_paper_fomc()
                    return

                loaded = 0
                for meeting in meetings:
                    meeting_date = meeting.get("date")
                    if not meeting_date:
                        continue
                    try:
                        event = FedEvent(
                            event_id=f"fomc_{meeting_date}",
                            event_type=FedEventType.FOMC_DECISION,
                            title=meeting.get("title", "FOMC Meeting"),
                            description=meeting.get("statement", ""),
                            date=datetime.fromisoformat(meeting_date).replace(tzinfo=timezone.utc),
                            importance=5,
                            expected_impact="HIGH",
                        )
                        self.events.append(event)
                        loaded += 1
                    except ValueError:
                        logger.debug("Skipping FOMC meeting with invalid date: {}", meeting_date)

                if loaded == 0:
                    logger.warning("FOMC API returned no valid meetings — using paper fallback")
                    self._seed_paper_fomc()

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("FOMC API unreachable ({}), using paper fallback", exc)
            self._seed_paper_fomc()
        except Exception as exc:
            logger.error("Unexpected error loading FOMC dates: {}", exc)
            self._seed_paper_fomc()

    def _seed_paper_fomc(self) -> None:
        now = datetime.now(tz=timezone.utc)
        self.events.extend([
            FedEvent(
                event_id=f"fomc_{now.year}_{i}",
                event_type=FedEventType.FOMC_DECISION,
                title=f"FOMC Meeting {now.year}",
                description="Federal Open Market Committee rate decision",
                date=now + timedelta(days=30 * i),
                importance=5,
                expected_impact="HIGH",
            )
            for i in range(1, 6)
        ])

    async def _load_scheduled_speeches(self) -> None:
        if self.paper_mode:
            self._seed_paper_speeches()
            return

        try:
            async with self._http_session.get(self.SPEECHES_URL) as response:
                if response.status != 200:
                    logger.debug("Fed speeches API returned status {}", response.status)
                    self._seed_paper_speeches()
                    return
                data = await response.json(content_type=None)
                if not isinstance(data, list):
                    logger.debug("Fed speeches response is not a list, using paper fallback")
                    self._seed_paper_speeches()
                    return

                loaded = 0
                for speech in data[:10]:
                    if not isinstance(speech, dict):
                        continue
                    date_str = speech.get("date", "")
                    if not date_str:
                        continue
                    try:
                        event = FedEvent(
                            event_id=f"speech_{speech.get('id', loaded)}",
                            event_type=FedEventType.FED_SPEECH,
                            title=speech.get("title", "Fed Speech"),
                            description=speech.get("text", "")[:500],
                            date=datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc),
                            importance=3,
                            expected_impact="MEDIUM",
                            speaker=speech.get("speaker"),
                        )
                        self.events.append(event)
                        loaded += 1
                    except ValueError:
                        continue

                if loaded == 0:
                    self._seed_paper_speeches()

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.debug("Fed speeches API unreachable ({}), using paper fallback", exc)
            self._seed_paper_speeches()
        except Exception as exc:
            logger.error("Unexpected error loading Fed speeches: {}", exc)
            self._seed_paper_speeches()

    def _seed_paper_speeches(self) -> None:
        now = datetime.now(tz=timezone.utc)
        speakers = ["Jerome Powell", "Michael Barr", "Michelle Bowman"]
        for i, speaker in enumerate(speakers):
            event = FedEvent(
                event_id=f"speech_{now.year}_{i}",
                event_type=FedEventType.FED_SPEECH,
                title=f"{speaker} Speech",
                description=f"Upcoming speech by {speaker}",
                date=now + timedelta(days=7 + (i * 3)),
                importance=3,
                expected_impact="MEDIUM",
                speaker=speaker,
            )
            self.events.append(event)

    def get_upcoming_events(
        self,
        days: int = 30,
        min_importance: int = 3,
    ) -> list[FedEvent]:
        now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(days=days)
        return [
            event
            for event in self.events
            if now < event.date <= cutoff
            and event.importance >= min_importance
        ]

    def get_next_event(self) -> Optional[FedEvent]:
        now = datetime.now(tz=timezone.utc)
        upcoming = [event for event in self.events if event.date > now]
        if upcoming:
            upcoming.sort(key=lambda e: e.date)
            return upcoming[0]
        return None

    def get_countdown(self, event_id: str) -> Optional[timedelta]:
        event = self.event_index.get(event_id)
        if not event:
            return None
        now = datetime.now(tz=timezone.utc)
        return event.date - now

    def get_event_risk_level(self, days_window: int = 7) -> str:
        """Returns 'LOW', 'MEDIUM', 'HIGH', or 'EXTREME'."""
        upcoming = self.get_upcoming_events(days=days_window)
        if not upcoming:
            return "LOW"

        high_impact = sum(1 for e in upcoming if e.importance >= 5)
        medium_impact = sum(1 for e in upcoming if e.importance == 4)

        if high_impact >= 2:
            return "EXTREME"
        elif high_impact == 1 or medium_impact >= 3:
            return "HIGH"
        elif medium_impact >= 1:
            return "MEDIUM"
        else:
            return "LOW"

    def is_fomc_week(self) -> bool:
        now = datetime.now(tz=timezone.utc)
        upcoming_fomc = sorted(
            [
                e for e in self.events
                if e.event_type == FedEventType.FOMC_DECISION and e.date >= now
            ],
            key=lambda e: e.date,
        )
        next_fomc = upcoming_fomc[0] if upcoming_fomc else None
        if not next_fomc:
            return False
        days_diff = (next_fomc.date - now).days
        return -1 <= days_diff <= 3

    async def close(self) -> None:
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
