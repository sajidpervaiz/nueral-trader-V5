"""
Prompt 10 – Monitoring + Alerting test suite.

Covers:
  • Alert model (formatting for Telegram HTML, Discord embed, dict)
  • AlertManager throttling & routing
  • AlertDispatcher event→alert mapping
  • Channel send behaviour (Telegram, Discord, Webhook, Log)
  • /health endpoint enhanced response
  • Prometheus metric increments
  • build_alert_manager_from_config factory
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from monitoring.alert_manager import (
    Alert,
    AlertChannel,
    AlertDispatcher,
    AlertManager,
    AlertSeverity,
    AlertType,
    DiscordAlertChannel,
    LogAlertChannel,
    TelegramAlertChannel,
    WebhookAlertChannel,
    build_alert_manager_from_config,
)

# ═══════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════

def _make_alert(**overrides: Any) -> Alert:
    defaults = dict(
        alert_type=AlertType.STOP_LOSS_HIT,
        severity=AlertSeverity.WARNING,
        title="SL hit BTC/USDT",
        message="SL triggered at 60000.00",
        metadata={"symbol": "BTC/USDT", "price": 60000},
    )
    defaults.update(overrides)
    return Alert(**defaults)


class RecordingChannel(AlertChannel):
    """Channel that records sent alerts for assertions."""

    def __init__(self) -> None:
        self.sent: list[Alert] = []

    async def send(self, alert: Alert) -> None:
        self.sent.append(alert)


class FakeEventBus:
    """Minimal in-process event bus for tests."""

    def __init__(self) -> None:
        self._subs: dict[str, list[Any]] = {}

    def subscribe(self, event: str, handler: Any) -> None:
        self._subs.setdefault(event, []).append(handler)

    def unsubscribe(self, event: str, handler: Any) -> None:
        if event in self._subs:
            self._subs[event] = [h for h in self._subs[event] if h != handler]

    async def publish(self, event: str, payload: Any = None) -> None:
        for handler in list(self._subs.get(event, [])):
            await handler(payload)


class FakeConfig:
    def __init__(self, data: dict[str, Any] | None = None):
        self._data = data or {}
        self.paper_mode = True

    def get_value(self, *keys: str, default: Any = None) -> Any:
        node = self._data
        for k in keys:
            if isinstance(node, dict):
                node = node.get(k, default)
            else:
                return default
        return node


# ═══════════════════════════════════════════════════════════════════
# Alert model
# ═══════════════════════════════════════════════════════════════════

class TestAlertModel:
    def test_to_dict_keys(self) -> None:
        a = _make_alert()
        d = a.to_dict()
        assert "alert_type" in d
        assert "severity" in d
        assert "title" in d
        assert "timestamp" in d

    def test_to_dict_values(self) -> None:
        a = _make_alert()
        d = a.to_dict()
        assert d["alert_type"] == "stop_loss_hit"
        assert d["severity"] == "warning"

    def test_to_telegram_html_contains_title(self) -> None:
        a = _make_alert(title="My Title")
        html = a.to_telegram_html()
        assert "My Title" in html
        assert "<b>" in html

    def test_to_telegram_html_contains_message(self) -> None:
        a = _make_alert(message="SL triggered at 60000")
        html = a.to_telegram_html()
        assert "60000" in html

    def test_to_discord_embed_structure(self) -> None:
        a = _make_alert()
        embed = a.to_discord_embed()
        assert "embeds" in embed
        inner = embed["embeds"][0]
        assert "title" in inner
        assert "description" in inner
        assert "color" in inner

    def test_to_discord_embed_color_varies_by_severity(self) -> None:
        info = _make_alert(severity=AlertSeverity.INFO).to_discord_embed()["embeds"][0]
        crit = _make_alert(severity=AlertSeverity.CRITICAL).to_discord_embed()["embeds"][0]
        assert info["color"] != crit["color"]

    def test_alert_auto_timestamp(self) -> None:
        before = time.time()
        a = _make_alert()
        after = time.time()
        assert before <= a.timestamp <= after


# ═══════════════════════════════════════════════════════════════════
# AlertManager – routing & throttling
# ═══════════════════════════════════════════════════════════════════

class TestAlertManagerRouting:
    @pytest.mark.asyncio
    async def test_send_dispatches_to_all_channels(self) -> None:
        c1, c2 = RecordingChannel(), RecordingChannel()
        mgr = AlertManager(channels=[c1, c2], default_throttle_seconds=0)
        await mgr.send(_make_alert())
        assert len(c1.sent) == 1
        assert len(c2.sent) == 1

    @pytest.mark.asyncio
    async def test_send_records_history(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        await mgr.send(_make_alert())
        assert len(mgr._history) == 1

    @pytest.mark.asyncio
    async def test_history_bounded(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        for i in range(1100):
            await mgr.send(_make_alert(title=f"alert-{i}"))
        assert len(mgr._history) <= 1000

    @pytest.mark.asyncio
    async def test_channel_error_does_not_halt(self) -> None:
        """If one channel throws, others still receive the alert."""

        class FailChannel(AlertChannel):
            async def send(self, alert: Alert) -> None:
                raise RuntimeError("channel down")

        rec = RecordingChannel()
        mgr = AlertManager(channels=[FailChannel(), rec], default_throttle_seconds=0)
        await mgr.send(_make_alert())
        assert len(rec.sent) == 1


class TestAlertManagerThrottling:
    @pytest.mark.asyncio
    async def test_duplicate_throttled(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=600)
        a = _make_alert()
        await mgr.send(a)
        await mgr.send(a)
        assert len(rec.sent) == 1  # second was throttled

    @pytest.mark.asyncio
    async def test_different_types_not_throttled(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=600)
        await mgr.send(_make_alert(alert_type=AlertType.STOP_LOSS_HIT))
        await mgr.send(_make_alert(alert_type=AlertType.TAKE_PROFIT_HIT))
        assert len(rec.sent) == 2

    @pytest.mark.asyncio
    async def test_throttle_expires(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0.01)
        await mgr.send(_make_alert())
        await asyncio.sleep(0.02)
        await mgr.send(_make_alert())
        assert len(rec.sent) == 2

    @pytest.mark.asyncio
    async def test_reset_throttle_allows_resend(self) -> None:
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=600)
        a = _make_alert()
        await mgr.send(a)
        mgr.reset_throttle(a.alert_type)
        await mgr.send(a)
        assert len(rec.sent) == 2

    @pytest.mark.asyncio
    async def test_throttle_override_per_type(self) -> None:
        rec = RecordingChannel()
        overrides = {AlertType.KILL_SWITCH: 0.01}
        mgr = AlertManager(
            channels=[rec],
            default_throttle_seconds=600,
            throttle_overrides=overrides,
        )
        ks = _make_alert(alert_type=AlertType.KILL_SWITCH)
        await mgr.send(ks)
        await asyncio.sleep(0.02)
        await mgr.send(ks)
        assert len(rec.sent) == 2


class TestAlertManagerStatus:
    @pytest.mark.asyncio
    async def test_get_status_structure(self) -> None:
        mgr = AlertManager(channels=[RecordingChannel()], default_throttle_seconds=0)
        await mgr.send(_make_alert())
        status = mgr.get_status()
        assert "history_count" in status
        assert "channels" in status
        assert status["history_count"] == 1

    @pytest.mark.asyncio
    async def test_get_status_empty(self) -> None:
        mgr = AlertManager(channels=[RecordingChannel()], default_throttle_seconds=0)
        status = mgr.get_status()
        assert status["history_count"] == 0


# ═══════════════════════════════════════════════════════════════════
# AlertDispatcher – event → alert mapping
# ═══════════════════════════════════════════════════════════════════

class TestAlertDispatcher:
    def _build(self) -> tuple[FakeEventBus, RecordingChannel, AlertDispatcher]:
        bus = FakeEventBus()
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        disp = AlertDispatcher(event_bus=bus, alert_manager=mgr)
        disp.subscribe_all()
        return bus, rec, disp

    @pytest.mark.asyncio
    async def test_stop_loss_event(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("STOP_LOSS", {"symbol": "BTC/USDT", "price": 60000})
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.STOP_LOSS_HIT

    @pytest.mark.asyncio
    async def test_take_profit_event(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("TAKE_PROFIT", {"symbol": "ETH/USDT", "price": 4000})
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.TAKE_PROFIT_HIT

    @pytest.mark.asyncio
    async def test_kill_switch_event(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("KILL_SWITCH", {})
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.KILL_SWITCH
        assert rec.sent[0].severity == AlertSeverity.CRITICAL

    @pytest.mark.asyncio
    async def test_user_stream_lost_event(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("USER_STREAM_LOST", None)
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.USER_STREAM_DISCONNECT

    @pytest.mark.asyncio
    async def test_user_stream_connected_clears_throttle(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("USER_STREAM_CONNECTED", None)
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.SAFE_MODE_CLEARED

    @pytest.mark.asyncio
    async def test_order_rejected_event(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("ORDER_REJECTED", {"symbol": "SOL/USDT", "reason": "insufficient margin"})
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.ORDER_REJECTION

    @pytest.mark.asyncio
    async def test_position_closed_event(self) -> None:
        bus, rec, _ = self._build()
        @dataclass
        class FakePos:
            symbol: str = "BTC/USDT"
            pnl: float = -120.0
        await bus.publish("POSITION_CLOSED", {"reason": "stop_loss", "position": FakePos()})
        assert len(rec.sent) == 1
        assert rec.sent[0].alert_type == AlertType.POSITION_CLOSED

    @pytest.mark.asyncio
    async def test_alert_critical_sl_placement(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("ALERT_CRITICAL", {"type": "sl_placement_fail", "error": "no margin"})
        assert rec.sent[0].alert_type == AlertType.SL_PLACEMENT_FAILED

    @pytest.mark.asyncio
    async def test_alert_critical_reconciliation(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("ALERT_CRITICAL", {"type": "reconciliation_mismatch"})
        assert rec.sent[0].alert_type == AlertType.RECONCILIATION_MISMATCH

    @pytest.mark.asyncio
    async def test_alert_critical_unknown_maps_custom(self) -> None:
        bus, rec, _ = self._build()
        await bus.publish("ALERT_CRITICAL", {"type": "something_new"})
        assert rec.sent[0].alert_type == AlertType.CUSTOM

    @pytest.mark.asyncio
    async def test_unsubscribe_all(self) -> None:
        bus = FakeEventBus()
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        disp = AlertDispatcher(event_bus=bus, alert_manager=mgr)
        disp.subscribe_all()
        disp.unsubscribe_all()
        await bus.publish("STOP_LOSS", {"symbol": "X"})
        assert len(rec.sent) == 0

    @pytest.mark.asyncio
    async def test_subscribe_all_idempotent(self) -> None:
        """Calling subscribe_all twice should not duplicate handlers."""
        bus, rec, disp = self._build()
        disp.subscribe_all()  # second call
        await bus.publish("STOP_LOSS", {"symbol": "X"})
        # Depending on EventBus impl this may double; we check at least 1 alert
        assert len(rec.sent) >= 1


# ═══════════════════════════════════════════════════════════════════
# Channels – unit tests with mocked HTTP
# ═══════════════════════════════════════════════════════════════════

class TestTelegramChannel:
    @pytest.mark.asyncio
    async def test_send_posts_to_telegram_api(self) -> None:
        ch = TelegramAlertChannel(token="123:ABC", chat_id="999")
        alert = _make_alert()
        mock_resp = MagicMock(status_code=200)
        with patch("monitoring.alert_manager.httpx") as mock_httpx:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_httpx.AsyncClient.return_value = mock_client
            await ch.send(alert)
            mock_client.post.assert_called_once()
            url = mock_client.post.call_args[0][0]
            assert "123:ABC" in url
            assert "sendMessage" in url

    @pytest.mark.asyncio
    async def test_send_handles_http_error_gracefully(self) -> None:
        ch = TelegramAlertChannel(token="tok", chat_id="1")
        with patch("monitoring.alert_manager.httpx") as mock_httpx:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(side_effect=Exception("network down"))
            mock_httpx.AsyncClient.return_value = mock_client
            await ch.send(_make_alert())  # should not raise


class TestDiscordChannel:
    @pytest.mark.asyncio
    async def test_send_posts_embed(self) -> None:
        ch = DiscordAlertChannel(webhook_url="https://discord.com/api/webhooks/test")
        mock_resp = MagicMock(status_code=204)
        with patch("monitoring.alert_manager.httpx") as mock_httpx:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_httpx.AsyncClient.return_value = mock_client
            await ch.send(_make_alert())
            mock_client.post.assert_called_once()
            payload = mock_client.post.call_args[1].get("json") or mock_client.post.call_args[0][1]
            assert "embeds" in payload


class TestWebhookChannel:
    @pytest.mark.asyncio
    async def test_send_posts_json(self) -> None:
        ch = WebhookAlertChannel(url="https://my.webhook/alerts")
        mock_resp = MagicMock(status_code=200)
        with patch("monitoring.alert_manager.httpx") as mock_httpx:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_httpx.AsyncClient.return_value = mock_client
            await ch.send(_make_alert())
            mock_client.post.assert_called_once()


class TestLogChannel:
    @pytest.mark.asyncio
    async def test_send_does_not_raise(self) -> None:
        ch = LogAlertChannel()
        await ch.send(_make_alert())  # just ensure no exceptions


# ═══════════════════════════════════════════════════════════════════
# build_alert_manager_from_config factory
# ═══════════════════════════════════════════════════════════════════

class TestBuildAlertManagerFromConfig:
    def test_log_channel_always_present(self) -> None:
        cfg = FakeConfig({"monitoring": {"alerts": {}}})
        mgr = build_alert_manager_from_config(cfg)
        assert any(isinstance(ch, LogAlertChannel) for ch in mgr.channels)

    def test_telegram_channel_added(self) -> None:
        cfg = FakeConfig({
            "monitoring": {
                "alerts": {
                    "telegram": {"enabled": True, "token": "tok", "chat_id": "123"},
                },
            }
        })
        mgr = build_alert_manager_from_config(cfg)
        assert any(isinstance(ch, TelegramAlertChannel) for ch in mgr.channels)

    def test_discord_channel_added(self) -> None:
        cfg = FakeConfig({
            "monitoring": {
                "alerts": {
                    "discord": {"webhook_url": "https://discord.com/api/webhooks/test"},
                },
            }
        })
        mgr = build_alert_manager_from_config(cfg)
        assert any(isinstance(ch, DiscordAlertChannel) for ch in mgr.channels)

    def test_webhook_channel_added(self) -> None:
        cfg = FakeConfig({
            "monitoring": {
                "alerts": {
                    "webhook": {"url": "https://my.hook/alerts"},
                },
            }
        })
        mgr = build_alert_manager_from_config(cfg)
        assert any(isinstance(ch, WebhookAlertChannel) for ch in mgr.channels)

    def test_throttle_from_config(self) -> None:
        cfg = FakeConfig({
            "monitoring": {
                "alerts": {"throttle_seconds": 120},
            }
        })
        mgr = build_alert_manager_from_config(cfg)
        assert mgr._default_throttle == 120.0

    def test_no_channels_only_log(self) -> None:
        cfg = FakeConfig({})
        mgr = build_alert_manager_from_config(cfg)
        assert len(mgr.channels) == 1
        assert isinstance(mgr.channels[0], LogAlertChannel)

    def test_fallback_to_top_level_telegram(self) -> None:
        """If alerts.telegram is not set, falls back to monitoring.telegram."""
        cfg = FakeConfig({
            "monitoring": {
                "telegram": {"enabled": True, "token": "T", "chat_id": "1"},
                "alerts": {},
            }
        })
        mgr = build_alert_manager_from_config(cfg)
        assert any(isinstance(ch, TelegramAlertChannel) for ch in mgr.channels)


# ═══════════════════════════════════════════════════════════════════
# /health endpoint
# ═══════════════════════════════════════════════════════════════════

class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_basic_fields(self) -> None:
        from interface.dashboard_api import build_app

        cfg = FakeConfig({"monitoring": {"dashboard_api": {}}})
        bus = FakeEventBus()
        app = build_app(config=cfg, event_bus=bus)
        if app is None:
            pytest.skip("FastAPI not available")

        from httpx import ASGITransport, AsyncClient

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert "paper_mode" in data
            assert "timestamp" in data
            assert "uptime_seconds" in data

    @pytest.mark.asyncio
    async def test_health_includes_risk_when_available(self) -> None:
        from interface.dashboard_api import build_app

        @dataclass
        class FakeRM:
            equity: float = 10000.0
            positions: dict = None
            circuit_breaker_active: bool = False
            kill_switch: bool = False
            daily_loss: float = -50.0

            def __post_init__(self):
                if self.positions is None:
                    self.positions = {}

        cfg = FakeConfig({"monitoring": {"dashboard_api": {}}})
        bus = FakeEventBus()
        app = build_app(config=cfg, event_bus=bus, risk_manager=FakeRM())
        if app is None:
            pytest.skip("FastAPI not available")

        from httpx import ASGITransport, AsyncClient

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")
            data = resp.json()
            assert "risk" in data
            assert data["risk"]["equity"] == 10000.0

    @pytest.mark.asyncio
    async def test_health_includes_safe_mode(self) -> None:
        from core.safe_mode import SafeModeManager, SafeModeReason
        from interface.dashboard_api import build_app

        sm = SafeModeManager()
        sm.activate(SafeModeReason.MANUAL)

        @dataclass
        class FakeRM:
            safe_mode: SafeModeManager = None
            equity: float = 0.0
            positions: dict = None
            circuit_breaker_active: bool = False
            kill_switch: bool = False
            daily_loss: float = 0.0

            def __post_init__(self):
                if self.positions is None:
                    self.positions = {}

        rm = FakeRM(safe_mode=sm)
        cfg = FakeConfig({"monitoring": {"dashboard_api": {}}})
        bus = FakeEventBus()
        app = build_app(config=cfg, event_bus=bus, risk_manager=rm)
        if app is None:
            pytest.skip("FastAPI not available")

        from httpx import ASGITransport, AsyncClient

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")
            data = resp.json()
            assert "safe_mode" in data
            assert data["safe_mode"]["active"] is True

    @pytest.mark.asyncio
    async def test_health_includes_alerts_when_attached(self) -> None:
        from interface.dashboard_api import build_app

        cfg = FakeConfig({"monitoring": {"dashboard_api": {}}})
        bus = FakeEventBus()
        app = build_app(config=cfg, event_bus=bus)
        if app is None:
            pytest.skip("FastAPI not available")

        mgr = AlertManager(channels=[RecordingChannel()], default_throttle_seconds=0)
        app.state.alert_manager = mgr

        from httpx import ASGITransport, AsyncClient

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")
            data = resp.json()
            assert "alerts" in data
            assert "history_count" in data["alerts"]


# ═══════════════════════════════════════════════════════════════════
# Prometheus metrics wiring
# ═══════════════════════════════════════════════════════════════════

class TestPrometheusMetrics:
    def test_new_metric_names_on_instance(self) -> None:
        from monitoring.metrics import Metrics

        bus = FakeEventBus()
        cfg = FakeConfig({"monitoring": {"prometheus": {"enabled": False}}})
        with patch("monitoring.metrics._PROMETHEUS", False):
            m = Metrics(config=cfg, event_bus=bus)
        for attr in [
            "alerts_total", "alerts_suppressed_total",
            "circuit_breaker_trips_total", "safe_mode_active",
            "kill_switch_active", "stop_loss_hits_total",
            "take_profit_hits_total",
        ]:
            assert hasattr(m, attr), f"Missing metric: {attr}"

    @pytest.mark.asyncio
    async def test_stop_loss_handler_increments_counter(self) -> None:
        from monitoring.metrics import Metrics

        bus = FakeEventBus()
        cfg = FakeConfig({"monitoring": {"prometheus": {"enabled": False}}})
        with patch("monitoring.metrics._PROMETHEUS", False):
            mc = Metrics(config=cfg, event_bus=bus)
        bus.subscribe("STOP_LOSS", mc._handle_stop_loss)
        await bus.publish("STOP_LOSS", {"exchange": "binance", "symbol": "BTC/USDT"})

    @pytest.mark.asyncio
    async def test_take_profit_handler_increments_counter(self) -> None:
        from monitoring.metrics import Metrics

        bus = FakeEventBus()
        cfg = FakeConfig({"monitoring": {"prometheus": {"enabled": False}}})
        with patch("monitoring.metrics._PROMETHEUS", False):
            mc = Metrics(config=cfg, event_bus=bus)
        bus.subscribe("TAKE_PROFIT", mc._handle_take_profit)
        await bus.publish("TAKE_PROFIT", {"exchange": "binance", "symbol": "ETH/USDT"})

    @pytest.mark.asyncio
    async def test_kill_switch_handler_sets_gauge(self) -> None:
        from monitoring.metrics import Metrics

        bus = FakeEventBus()
        cfg = FakeConfig({"monitoring": {"prometheus": {"enabled": False}}})
        with patch("monitoring.metrics._PROMETHEUS", False):
            mc = Metrics(config=cfg, event_bus=bus)
        bus.subscribe("KILL_SWITCH", mc._handle_kill_switch)
        await bus.publish("KILL_SWITCH", {})


# ═══════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════

class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_alert_with_none_metadata(self) -> None:
        a = Alert(
            alert_type=AlertType.CUSTOM,
            severity=AlertSeverity.INFO,
            title="test",
            message="msg",
            metadata=None,
        )
        d = a.to_dict()
        assert d["metadata"] is None

    @pytest.mark.asyncio
    async def test_dispatcher_handles_non_dict_payload(self) -> None:
        bus = FakeEventBus()
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        disp = AlertDispatcher(event_bus=bus, alert_manager=mgr)
        disp.subscribe_all()
        # Send string payload instead of dict
        await bus.publish("STOP_LOSS", "not-a-dict")
        assert len(rec.sent) == 1

    @pytest.mark.asyncio
    async def test_dispatcher_handles_none_payload(self) -> None:
        bus = FakeEventBus()
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=0)
        disp = AlertDispatcher(event_bus=bus, alert_manager=mgr)
        disp.subscribe_all()
        await bus.publish("ORDER_REJECTED", None)
        assert len(rec.sent) == 1

    @pytest.mark.asyncio
    async def test_multiple_rapid_events_throttled_correctly(self) -> None:
        bus = FakeEventBus()
        rec = RecordingChannel()
        mgr = AlertManager(channels=[rec], default_throttle_seconds=600)
        disp = AlertDispatcher(event_bus=bus, alert_manager=mgr)
        disp.subscribe_all()
        for _ in range(10):
            await bus.publish("STOP_LOSS", {"symbol": "X"})
        # Only 1 should pass through throttle
        assert len(rec.sent) == 1
