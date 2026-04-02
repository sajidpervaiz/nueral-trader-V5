from __future__ import annotations

import asyncio

import pytest

from core.dispatcher import Dispatcher
from core.event_bus import EventBus


class _FailingComponent:
    async def run(self) -> None:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_dispatcher_fails_fast_on_component_crash() -> None:
    bus = EventBus()
    dispatcher = Dispatcher(
        event_bus=bus,
        data_manager=_FailingComponent(),
    )

    with pytest.raises(RuntimeError, match="component failure"):
        await asyncio.wait_for(dispatcher.run_forever(), timeout=2.0)
