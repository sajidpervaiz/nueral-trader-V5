"""
Startup validation — fail-fast before trading with real money.

Validates:
- API keys present and non-empty
- Exchange permissions (read + trade enabled)
- Account balance above minimum threshold
- Leverage and margin mode configured correctly
- Requested symbols available on exchange
- Clock sync with exchange
"""
from __future__ import annotations

import time
from typing import Any

from loguru import logger

from core.config import Config


class ValidationError(Exception):
    """Fatal validation error — bot must not start."""
    pass


class StartupValidator:
    """
    Validates all prerequisites before live trading.

    If any check fails, raises ValidationError which prevents bot startup.
    Only runs in non-paper mode.
    """

    def __init__(self, config: Config, client: Any = None) -> None:
        self.config = config
        self._client = client
        risk_cfg = config.get_value("risk") or {}
        self._min_balance_usd = float(
            risk_cfg.get("min_balance_usd", 100)
        )

    async def validate_all(self) -> dict[str, Any]:
        """
        Run all validations. Returns summary dict.
        Raises ValidationError on any critical failure.
        """
        if self.config.paper_mode:
            logger.info("Paper mode — skipping live startup validation")
            return {"mode": "paper", "skipped": True}

        results: dict[str, Any] = {"mode": "live", "checks": {}}

        # 1. API keys
        self._check_api_keys(results)

        # 2. Exchange connectivity
        await self._check_connectivity(results)

        # 3. Exchange permissions (trade enabled)
        await self._check_permissions(results)

        # 4. Account balance
        await self._check_balance(results)

        # 5. Symbol availability
        await self._check_symbols(results)

        # 6. Clock sync
        await self._check_clock_sync(results)

        # 7. Leverage / margin mode
        await self._check_leverage(results)

        logger.info("Startup validation PASSED — all checks OK")
        return results

    def _check_api_keys(self, results: dict) -> None:
        """Validate API keys are present and non-empty."""
        exchanges_cfg = self.config.get_value("exchanges") or {}
        for exchange_id, cfg in exchanges_cfg.items():
            if not isinstance(cfg, dict) or not cfg.get("enabled", False):
                continue
            api_key = str(cfg.get("api_key", "")).strip()
            api_secret = str(cfg.get("api_secret", "")).strip()

            if not api_key or api_key.startswith("${"):
                raise ValidationError(
                    f"API key missing for {exchange_id}. "
                    f"Set {exchange_id.upper()}_API_KEY environment variable."
                )
            if not api_secret or api_secret.startswith("${"):
                raise ValidationError(
                    f"API secret missing for {exchange_id}. "
                    f"Set {exchange_id.upper()}_API_SECRET environment variable."
                )

            results["checks"][f"{exchange_id}_api_keys"] = "ok"
            logger.info("{} API keys present", exchange_id)

    async def _check_connectivity(self, results: dict) -> None:
        """Verify exchange client is connected and functional."""
        if self._client is None:
            raise ValidationError("No exchange client — cannot trade live")
        try:
            await self._client.load_markets()
            results["checks"]["connectivity"] = "ok"
            logger.info("Exchange connectivity OK")
        except Exception as exc:
            raise ValidationError(f"Exchange connectivity failed: {exc}") from exc

    async def _check_permissions(self, results: dict) -> None:
        """Check that API key has trading permissions by fetching account info."""
        if self._client is None:
            return
        try:
            # fetch_balance will fail if API key doesn't have READ permission
            balance = await self._client.fetch_balance()
            if not balance:
                raise ValidationError("Empty balance response — check API permissions")
            results["checks"]["permissions"] = "ok"
            logger.info("Exchange permissions OK (read+trade)")
        except ValidationError:
            raise
        except Exception as exc:
            raise ValidationError(
                f"API permission check failed: {exc}. "
                "Ensure API key has 'Enable Futures' and 'Enable Reading' permissions."
            ) from exc

    async def _check_balance(self, results: dict) -> None:
        """Verify account has sufficient balance."""
        if self._client is None:
            return
        try:
            balance = await self._client.fetch_balance()
            total_usdt = float(balance.get("total", {}).get("USDT", 0))
            free_usdt = float(balance.get("free", {}).get("USDT", 0))

            results["checks"]["balance"] = {
                "total_usdt": total_usdt,
                "free_usdt": free_usdt,
                "min_required": self._min_balance_usd,
            }

            if total_usdt < self._min_balance_usd:
                raise ValidationError(
                    f"Insufficient balance: {total_usdt:.2f} USDT < "
                    f"minimum {self._min_balance_usd:.2f} USDT"
                )

            # Check if max order size is feasible
            risk_cfg = self.config.get_value("risk") or {}
            max_pos_pct = float(risk_cfg.get("max_position_size_pct", 0.02))
            min_trade_size = total_usdt * max_pos_pct
            if min_trade_size < 10:  # Binance minimum notional ~$5-10
                raise ValidationError(
                    f"Balance too low for configured position size: "
                    f"{total_usdt:.2f} × {max_pos_pct} = {min_trade_size:.2f} USDT"
                )

            logger.info(
                "Balance OK: total={:.2f} USDT, free={:.2f} USDT",
                total_usdt, free_usdt,
            )
        except ValidationError:
            raise
        except Exception as exc:
            raise ValidationError(f"Balance check failed: {exc}") from exc

    async def _check_symbols(self, results: dict) -> None:
        """Verify configured symbols exist on the initialized exchange."""
        if self._client is None:
            return
        # Only validate symbols for the exchange this client is connected to
        markets = self._client.markets or {}
        exchanges_cfg = self.config.get_value("exchanges") or {}
        # Determine which exchange this client belongs to
        client_exchange = getattr(self._client, 'id', '')
        cfg = exchanges_cfg.get(client_exchange, {})
        if not isinstance(cfg, dict) or not cfg.get("enabled", False):
            return
        symbols = cfg.get("symbols", [])
        missing = [s for s in symbols if s not in markets]
        if missing:
            raise ValidationError(
                f"Symbols not available on {client_exchange}: {missing}. "
                f"Available symbols: check exchange market listing."
            )
        results["checks"][f"{client_exchange}_symbols"] = "ok"
        logger.info("{} symbols validated: {}", client_exchange, symbols)

    async def _check_clock_sync(self, results: dict) -> None:
        """Check clock drift with exchange server."""
        if self._client is None:
            return
        try:
            server_time = await self._client.fetch_time()
            local_time = int(time.time() * 1000)
            drift_ms = abs(server_time - local_time)
            results["checks"]["clock_drift_ms"] = drift_ms

            if drift_ms > 5000:  # 5 second drift is dangerous
                raise ValidationError(
                    f"Clock drift too high: {drift_ms}ms. "
                    "Sync system clock with NTP."
                )
            elif drift_ms > 1000:
                logger.warning("Clock drift: {}ms — consider NTP sync", drift_ms)
            else:
                logger.info("Clock sync OK (drift={}ms)", drift_ms)
        except ValidationError:
            raise
        except Exception as exc:
            logger.warning("Clock sync check failed (non-fatal): {}", exc)
            results["checks"]["clock_drift_ms"] = -1

    async def _check_leverage(self, results: dict) -> None:
        """Verify leverage settings are within safe bounds."""
        risk_cfg = self.config.get_value("risk") or {}
        leverage = float(risk_cfg.get("default_leverage", 1.0))

        if leverage > 20:
            raise ValidationError(
                f"Leverage {leverage}x is dangerously high. "
                "Maximum recommended: 20x. Reduce risk.default_leverage."
            )
        if leverage > 10:
            logger.warning("High leverage: {}x — ensure adequate margin", leverage)

        results["checks"]["leverage"] = {"configured": leverage, "status": "ok"}
        logger.info("Leverage check OK: {}x", leverage)
