"""
Smart Order Router with venue scoring and liquidity-based routing.
"""

import asyncio
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger
from execution.binance_executor import BinanceExecutor, OrderSide, OrderType
from execution.bybit_executor import BybitExecutor
from execution.okx_executor import OKXExecutor


class Venue(Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    HYPERLIQUID = "hyperliquid"


@dataclass
class VenueScore:
    venue: Venue
    score: float
    liquidity: float
    spread: float
    fee: float
    latency_ms: float
    reliability: float
    metadata: Dict = field(default_factory=dict)


@dataclass
class Route:
    venue: Venue
    quantity: float
    expected_fill_price: float
    estimated_cost: float
    execution_time_ms: float
    score: float


@dataclass
class RoutingDecision:
    routes: List[Route]
    total_quantity: float
    expected_avg_price: float
    total_cost: float
    recommended_venue: Venue
    confidence: float


class SmartOrderRouter:
    """
    Production-grade smart order router with:
    - Venue scoring based on multiple factors
    - Liquidity-based routing
    - Fee optimization
    - Latency considerations
    - Reliability tracking
    """

    def __init__(
        self,
        binance_executor: Optional[BinanceExecutor] = None,
        bybit_executor: Optional[BybitExecutor] = None,
        okx_executor: Optional[OKXExecutor] = None,
    ):
        self.executors = {
            Venue.BINANCE: binance_executor,
            Venue.BYBIT: bybit_executor,
            Venue.OKX: okx_executor,
        }

        self.venue_scores: Dict[Venue, VenueScore] = {}
        self.historical_performance: Dict[str, List[float]] = {}

        self.routing_weights = {
            "liquidity": 0.35,
            "spread": 0.25,
            "fee": 0.15,
            "latency": 0.15,
            "reliability": 0.10,
        }

        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize router by fetching initial venue scores."""
        logger.info("Initializing Smart Order Router")

        for venue, executor in self.executors.items():
            if executor:
                score = await self._calculate_venue_score(venue)
                self.venue_scores[venue] = score
                logger.info(f"{venue.value} score: {score.score:.2f}")

    async def route_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        max_venues: int = 1,
        min_score_threshold: float = 0.5,
    ) -> Optional[RoutingDecision]:
        """
        Route order to optimal venue(s).

        Args:
            symbol: Trading pair symbol
            side: Order side
            quantity: Order quantity
            max_venues: Maximum number of venues to use
            min_score_threshold: Minimum score for venue selection

        Returns:
            Routing decision with venue(s) and execution plan
        """
        async with self._lock:
            available_venues = [
                (venue, score)
                for venue, score in self.venue_scores.items()
                if score.liquidity >= quantity
                and score.score >= min_score_threshold
            ]

            if not available_venues:
                logger.warning(f"No suitable venues for {symbol} with quantity {quantity}")
                return None

            available_venues.sort(key=lambda x: x[1].score, reverse=True)

            selected_venues = available_venues[:max_venues]
            routes = []

            remaining_qty = quantity

            for venue, score in selected_venues:
                if remaining_qty <= 0:
                    break

                route_qty = min(remaining_qty, score.liquidity * 0.8)

                expected_price = await self._get_expected_price(venue, symbol, side)

                route = Route(
                    venue=venue,
                    quantity=route_qty,
                    expected_fill_price=expected_price,
                    estimated_cost=self._calculate_estimated_cost(score, route_qty),
                    execution_time_ms=score.latency_ms,
                    score=score.score,
                )

                routes.append(route)
                remaining_qty -= route_qty

            if remaining_qty > 0.001:
                logger.warning(
                    f"Could not route full quantity: {remaining_qty:.6f} remaining"
                )

            total_qty = sum(r.quantity for r in routes)
            weighted_price = sum(r.quantity * r.expected_fill_price for r in routes) / total_qty
            total_cost = sum(r.estimated_cost for r in routes)

            return RoutingDecision(
                routes=routes,
                total_quantity=total_qty,
                expected_avg_price=weighted_price,
                total_cost=total_cost,
                recommended_venue=selected_venues[0][0],
                confidence=selected_venues[0][1].score,
            )

    async def execute_routed_order(
        self,
        decision: RoutingDecision,
        symbol: str,
        side: OrderSide,
        order_type: OrderType = OrderType.MARKET,
        price: Optional[float] = None,
    ) -> List[Dict]:
        """Execute order across selected venues."""
        results = []

        for route in decision.routes:
            executor = self.executors.get(route.venue)
            if not executor:
                logger.warning(f"Executor not available for {route.venue.value}")
                continue

            try:
                result = await executor.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=route.quantity,
                    price=price,
                )

                results.append({
                    "venue": route.venue.value,
                    "route": route,
                    "result": result,
                    "status": "success" if result else "failed",
                })

            except Exception as e:
                logger.error(f"Failed to execute on {route.venue.value}: {e}")
                results.append({
                    "venue": route.venue.value,
                    "route": route,
                    "result": None,
                    "status": "error",
                    "error": str(e),
                })

        return results

    async def _calculate_venue_score(self, venue: Venue) -> VenueScore:
        """Calculate comprehensive venue score."""
        executor = self.executors.get(venue)
        if not executor:
            return VenueScore(
                venue=venue,
                score=0.0,
                liquidity=0.0,
                spread=float('inf'),
                fee=0.0,
                latency_ms=float('inf'),
                reliability=0.0,
            )

        try:
            symbol = "BTC/USDT"

            snapshot = await executor.get_orderbook_snapshot(symbol, depth=10)

            liquidity = sum(
                level.quantity
                for level in snapshot.bids[:5] + snapshot.asks[:5]
            )

            if snapshot.bids and snapshot.asks:
                spread = snapshot.asks[0].price - snapshot.bids[0].price
                mid_price = (snapshot.bids[0].price + snapshot.asks[0].price) / 2
                spread_pct = (spread / mid_price) * 100
            else:
                spread = float('inf')
                spread_pct = float('inf')

            fee = self._get_venue_fee(venue)
            latency_ms = self._get_venue_latency(venue)
            reliability = self._get_venue_reliability(venue)

            normalized_liquidity = min(liquidity / 1000, 1.0)
            normalized_spread = max(0, 1 - (spread_pct / 0.1))
            normalized_fee = max(0, 1 - (fee / 0.01))
            normalized_latency = max(0, 1 - (latency_ms / 1000))

            score = (
                self.routing_weights["liquidity"] * normalized_liquidity +
                self.routing_weights["spread"] * normalized_spread +
                self.routing_weights["fee"] * normalized_fee +
                self.routing_weights["latency"] * normalized_latency +
                self.routing_weights["reliability"] * reliability
            )

            return VenueScore(
                venue=venue,
                score=score,
                liquidity=liquidity,
                spread=spread,
                fee=fee,
                latency_ms=latency_ms,
                reliability=reliability,
            )

        except Exception as e:
            logger.error(f"Error calculating score for {venue.value}: {e}")
            return VenueScore(
                venue=venue,
                score=0.0,
                liquidity=0.0,
                spread=float('inf'),
                fee=0.0,
                latency_ms=float('inf'),
                reliability=0.0,
            )

    async def _get_expected_price(
        self,
        venue: Venue,
        symbol: str,
        side: OrderSide,
    ) -> float:
        """Get expected execution price for venue."""
        executor = self.executors.get(venue)
        if not executor:
            return 0.0

        try:
            snapshot = await executor.get_orderbook_snapshot(symbol, depth=1)

            if side == OrderSide.BUY:
                return snapshot.asks[0].price if snapshot.asks else 0.0
            else:
                return snapshot.bids[0].price if snapshot.bids else 0.0

        except Exception:
            return 0.0

    def _calculate_estimated_cost(self, score: VenueScore, quantity: float) -> float:
        """Calculate estimated execution cost including fees and slippage."""
        mid_price = score.liquidity / quantity

        slippage_cost = (score.spread / 2) * quantity
        fee_cost = mid_price * quantity * score.fee

        return slippage_cost + fee_cost

    def _get_venue_fee(self, venue: Venue) -> float:
        """Get venue's trading fee."""
        fee_map = {
            Venue.BINANCE: 0.001,
            Venue.BYBIT: 0.001,
            Venue.OKX: 0.0008,
            Venue.HYPERLIQUID: 0.00025,
        }
        return fee_map.get(venue, 0.001)

    def _get_venue_latency(self, venue: Venue) -> float:
        """Get venue's average latency in ms."""
        latency_map = {
            Venue.BINANCE: 50,
            Venue.BYBIT: 45,
            Venue.OKX: 60,
            Venue.HYPERLIQUID: 100,
        }
        return latency_map.get(venue, 100)

    def _get_venue_reliability(self, venue: Venue) -> float:
        """Get venue's reliability score (0-1)."""
        reliability_map = {
            Venue.BINANCE: 0.99,
            Venue.BYBIT: 0.97,
            Venue.OKX: 0.96,
            Venue.HYPERLIQUID: 0.90,
        }
        return reliability_map.get(venue, 0.95)

    def update_routing_weights(self, weights: Dict[str, float]) -> None:
        """Update routing weight configuration."""
        total = sum(weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Weights must sum to 1.0, got {total}")

        self.routing_weights = weights
        logger.info(f"Routing weights updated: {weights}")

    def get_venue_scores(self) -> Dict[Venue, VenueScore]:
        """Get current venue scores."""
        return self.venue_scores.copy()
