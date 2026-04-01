"""
Fed Calendar integration with FOMC dates, Fed speeches, and event countdowns.
"""

import asyncio
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import aiohttp
from loguru import logger


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
    Fed Calendar manager with:
    - FOMC date tracking
    - Fed speech monitoring
    - Event countdowns
    - Impact assessment
    """

    def __init__(self, enable_paper_mode: bool = True):
        self.paper_mode = enable_paper_mode
        self.events: List[FedEvent] = []
        self.event_index: Dict[str, FedEvent] = {}

        self._http_session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Initialize Fed Calendar."""
        self._http_session = aiohttp.ClientSession()

        await self._load_fomc_dates()
        await self._load_scheduled_speeches()

        logger.info(f"Loaded {len(self.events)} Fed events")

    async def _load_fomc_dates(self) -> None:
        """Load FOMC meeting dates."""
        if self.paper_mode:
            now = datetime.now()

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

            return

        try:
            url = "https://api.federalreserve.gov/v1/fomc/meetings"
            async with self._http_session.get(url) as response:
                if response.status == 200:
                    data = await response.json()

                    for meeting in data.get('meetings', []):
                        event = FedEvent(
                            event_id=f"fomc_{meeting.get('date', '')}",
                            event_type=FedEventType.FOMC_DECISION,
                            title=meeting.get('title', 'FOMC Meeting'),
                            description=meeting.get('statement', ''),
                            date=datetime.fromisoformat(meeting.get('date', '')),
                            importance=5,
                            expected_impact="HIGH",
                        )
                        self.events.append(event)

        except Exception as e:
            logger.error(f"Error loading FOMC dates: {e}")

    async def _load_scheduled_speeches(self) -> None:
        """Load scheduled Fed speeches."""
        if self.paper_mode:
            now = datetime.now()

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

            return

        try:
            url = "https://www.federalreserve.gov/json/speeches.json"
            async with self._http_session.get(url) as response:
                if response.status == 200:
                    data = await response.json()

                    for speech in data[:10]:
                        event = FedEvent(
                            event_id=f"speech_{speech.get('id', '')}",
                            event_type=FedEventType.FED_SPEECH,
                            title=speech.get('title', 'Fed Speech'),
                            description=speech.get('text', ''),
                            date=datetime.fromisoformat(speech.get('date', '')),
                            importance=3,
                            expected_impact="MEDIUM",
                            speaker=speech.get('speaker'),
                        )
                        self.events.append(event)

        except Exception as e:
            logger.error(f"Error loading Fed speeches: {e}")

    def get_upcoming_events(
        self,
        days: int = 30,
        min_importance: int = 3,
    ) -> List[FedEvent]:
        """Get upcoming Fed events."""
        now = datetime.now()
        cutoff = now + timedelta(days=days)

        return [
            event
            for event in self.events
            if now < event.date <= cutoff
            and event.importance >= min_importance
        ]

    def get_next_event(self) -> Optional[FedEvent]:
        """Get next Fed event."""
        now = datetime.now()
        upcoming = [
            event for event in self.events
            if event.date > now
        ]

        if upcoming:
            upcoming.sort(key=lambda e: e.date)
            return upcoming[0]

        return None

    def get_countdown(self, event_id: str) -> Optional[timedelta]:
        """Get countdown to event."""
        event = self.event_index.get(event_id)
        if not event:
            return None

        now = datetime.now()
        return event.date - now

    def get_event_risk_level(self, days_window: int = 7) -> str:
        """
        Get overall risk level based on upcoming events.

        Returns: 'LOW', 'MEDIUM', 'HIGH', or 'EXTREME'
        """
        upcoming = self.get_upcoming_events(days=days_window)

        if not upcoming:
            return 'LOW'

        high_impact = sum(1 for e in upcoming if e.importance >= 5)
        medium_impact = sum(1 for e in upcoming if e.importance == 4)

        if high_impact >= 2:
            return 'EXTREME'
        elif high_impact == 1 or medium_impact >= 3:
            return 'HIGH'
        elif medium_impact >= 1:
            return 'MEDIUM'
        else:
            return 'LOW'

    def is_fomc_week(self) -> bool:
        """Check if we're in FOMC week (week of meeting)."""
        next_fomc = next(
            (e for e in self.events if e.event_type == FedEventType.FOMC_DECISION),
            None
        )

        if not next_fomc:
            return False

        now = datetime.now()
        days_diff = (next_fomc.date - now).days

        return -1 <= days_diff <= 3

    async def close(self) -> None:
        """Clean up resources."""
        if self._http_session:
            await self._http_session.close()
