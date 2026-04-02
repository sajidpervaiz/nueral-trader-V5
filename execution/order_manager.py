"""
Production-grade Order Manager with idempotency, lifecycle tracking,
and self-trade prevention.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from core.circuit_breaker import CircuitBreaker
from core.idempotency import IdempotencyManager
from core.retry import RetryPolicy


class OrderStatus(Enum):
    """Order lifecycle states"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OrderSide(Enum):
    """Order side (buy/sell)"""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type"""
    LIMIT = "limit"
    MARKET = "market"
    POST_ONLY = "post_only"
    IOC = "ioc"


class OrderLifecycleStage(Enum):
    VALIDATION = "VALIDATION"
    RISK_CHECK = "RISK_CHECK"
    SUBMISSION = "SUBMISSION"
    ACKNOWLEDGEMENT = "ACKNOWLEDGEMENT"
    PARTIAL_FILL = "PARTIAL_FILL"
    COMPLETE_FILL = "COMPLETE_FILL"
    CANCELLATION = "CANCELLATION"
    COMPLETION = "COMPLETION"


@dataclass
class OrderStage:
    stage: OrderLifecycleStage
    timestamp: float
    metadata: Dict = field(default_factory=dict)


@dataclass
class OrderFill:
    """Represents a partial or complete fill"""
    fill_id: str
    timestamp: int
    quantity: float
    price: float
    fee: float = 0.0
    fee_currency: str = "USDT"

    @property
    def total_value(self) -> float:
        """Total value of fill (quantity * price)"""
        return self.quantity * self.price


@dataclass
class Order:
    order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float]
    status: OrderStatus
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0
    avg_fill_price: float = 0.0
    cumulative_quantity: float = 0.0
    average_fill_price: float = 0.0
    total_fee: float = 0.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    submitted_at: Optional[int] = None
    filled_at: Optional[int] = None
    exchange_order_id: Optional[str] = None
    venue: Optional[str] = None
    time_in_force: str = "GTC"
    reduce_only: bool = False
    user_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    stages: List[OrderStage] = field(default_factory=list)
    fills: List[OrderFill] = field(default_factory=list)
    error_message: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self):
        if self.remaining_quantity == 0.0:
            self.remaining_quantity = self.quantity

    def add_stage(self, stage: OrderLifecycleStage, metadata: Optional[Dict] = None):
        """Record a lifecycle stage."""
        self.stages.append(OrderStage(
            stage=stage,
            timestamp=time.time(),
            metadata=metadata or {},
        ))
        self.updated_at = time.time()

    def update_fill(self, fill_qty: float, fill_price: float):
        """Update order with fill information."""
        total_value = (self.avg_fill_price * self.filled_quantity) + (fill_price * fill_qty)
        self.filled_quantity += fill_qty
        self.remaining_quantity = max(0.0, self.quantity - self.filled_quantity)

        if self.filled_quantity > 0:
            self.avg_fill_price = total_value / self.filled_quantity

        self.updated_at = time.time()

        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
            self.add_stage(OrderLifecycleStage.COMPLETE_FILL)
        else:
            self.status = OrderStatus.PARTIALLY_FILLED
            self.add_stage(OrderLifecycleStage.PARTIAL_FILL)


class OrderManager:
    """
    Production-grade order manager with:
    - Idempotent order placement
    - Lifecycle tracking
    - Self-trade prevention
    - Order amend/cancel with retry logic
    """

    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        circuit_breaker: CircuitBreaker,
    ):
        self.config = config
        self.event_bus = event_bus
        self.circuit_breaker = circuit_breaker
        
        # Order storage
        self.orders: Dict[str, Order] = {}
        self.client_order_map: Dict[str, Order] = {}
        self.exchange_order_map: Dict[str, Order] = {}
        self.pending_orders: Dict[str, Order] = {}
        self.user_orders: Dict[str, Dict[str, Order]] = {}
        
        # Idempotency manager
        self.idempotency = IdempotencyManager(ttl=86400, max_size=50000)
        
        # Retry policy
        self.retry_policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        
        # Self-trade prevention window
        self.self_trade_window_ms = 60000
        
        # Audit trail
        self.audit_log: List[Dict] = []
        self.max_audit_log_size = 100000
        self.smart_router: Any = None
        
        self._lock = asyncio.Lock()
        self._running = False

    def attach_router(self, router: Any) -> None:
        """Attach an optional smart order router for auto venue selection."""
        self.smart_router = router

    def _default_exchange(self) -> str:
        exchanges = self.config.get_value("exchanges") or {}
        for exchange_id, cfg in exchanges.items():
            if isinstance(cfg, dict) and cfg.get("enabled", False):
                return str(exchange_id)
        return "binance"

    async def _resolve_exchange(
        self,
        exchange: str,
        symbol: str,
        side: OrderSide,
        quantity: float,
        metadata: Dict,
    ) -> tuple[str, Dict]:
        requested = (exchange or "").lower().strip()
        if requested not in {"auto", "best", "router"}:
            return exchange, metadata

        if self.smart_router is None:
            fallback = self._default_exchange()
            metadata["routing_mode"] = "fallback_no_router"
            metadata["requested_exchange"] = exchange
            return fallback, metadata

        try:
            from execution.binance_executor import OrderSide as RouterOrderSide

            router_side = RouterOrderSide.BUY if side == OrderSide.BUY else RouterOrderSide.SELL
            decision = await self.smart_router.route_order(
                symbol=symbol,
                side=router_side,
                quantity=quantity,
                max_venues=3,
                min_score_threshold=0.1,
            )
            if decision is None:
                fallback = self._default_exchange()
                metadata["routing_mode"] = "fallback_no_decision"
                metadata["requested_exchange"] = exchange
                return fallback, metadata

            selected_exchange = str(getattr(decision.recommended_venue, "value", "") or "")
            if not selected_exchange:
                fallback = self._default_exchange()
                metadata["routing_mode"] = "fallback_invalid_decision"
                metadata["requested_exchange"] = exchange
                return fallback, metadata

            metadata["routing_mode"] = "smart_router"
            metadata["requested_exchange"] = exchange
            metadata["routing_decision"] = {
                "recommended_venue": selected_exchange,
                "confidence": float(getattr(decision, "confidence", 0.0)),
                "expected_avg_price": float(getattr(decision, "expected_avg_price", 0.0)),
                "routes": [
                    {
                        "venue": str(getattr(getattr(route, "venue", None), "value", "")),
                        "quantity": float(getattr(route, "quantity", 0.0)),
                        "expected_fill_price": float(getattr(route, "expected_fill_price", 0.0)),
                        "score": float(getattr(route, "score", 0.0)),
                    }
                    for route in list(getattr(decision, "routes", []))
                ],
            }
            return selected_exchange, metadata
        except Exception as exc:
            fallback = self._default_exchange()
            logger.warning("Smart routing failed: {} — falling back to {}", exc, fallback)
            metadata["routing_mode"] = "fallback_error"
            metadata["routing_error"] = str(exc)
            metadata["requested_exchange"] = exchange
            return fallback, metadata

    def generate_client_order_id(self, exchange: str, symbol: str, side: OrderSide) -> str:
        """Generate unique client order ID"""
        timestamp_ms = int(time.time() * 1000)
        unique_part = str(uuid.uuid4())[:8]
        return f"{exchange}-{symbol}-{side.value[0]}-{timestamp_ms}-{unique_part}"

    async def place_order(
        self,
        exchange: str,
        symbol: str,
        side: OrderSide,
        quantity: float,
        price: float,
        order_type: OrderType = OrderType.LIMIT,
        client_order_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> Tuple[bool, Optional[Order], str]:
        """
        Place order with idempotency and self-trade prevention.
        
        Returns:
            (success: bool, order: Order, reason: str)
        """
        async with self._lock:
            # Check circuit breaker
            if self.circuit_breaker.get_state().value == "OPEN":
                return False, None, "circuit_breaker_open"

            # Use caller-provided client order ID when supplied; otherwise generate one.
            if not client_order_id:
                client_order_id = self.generate_client_order_id(exchange, symbol, side)
            
            # Check idempotency
            if self.idempotency.check_and_set(client_order_id):
                cached_order = self.client_order_map.get(client_order_id)
                if cached_order:
                    logger.info(f"Order {client_order_id} already placed (idempotent retry)")
                    return True, cached_order, "idempotent_retry"

            # Self-trade prevention
            metadata = dict(metadata or {})
            exchange, metadata = await self._resolve_exchange(exchange, symbol, side, quantity, metadata)
            self_trade_reason = self._check_self_trade(exchange, symbol, side)
            if self_trade_reason:
                logger.warning(f"Self-trade prevented for {symbol}: {self_trade_reason}")
                return False, None, self_trade_reason

            # Create order
            order_id = f"ord_{uuid.uuid4().hex[:12]}_{int(time.time() * 1000)}"
            order = Order(
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                status=OrderStatus.PENDING,
                exchange_order_id=None,
                venue=exchange,
                metadata=metadata,
            )

            # Store order
            self.orders[order_id] = order
            self.client_order_map[client_order_id] = order
            self.pending_orders[order_id] = order
            self.idempotency.set_result(client_order_id, order)
            self._record_audit("ORDER_CREATED", order)

            logger.info(
                f"Order created: {client_order_id} {side.value} {quantity} {symbol} @ {price}"
            )
            
            return True, order, "created"

    def _check_self_trade(self, exchange: str, symbol: str, side: OrderSide) -> Optional[str]:
        """Check for self-trade risk"""
        open_orders = [
            o for o in self.orders.values()
            if o.venue == exchange 
            and o.symbol == symbol
            and o.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
        ]

        for order in open_orders:
            if order.side != side:
                age_ms = int(time.time() * 1000) - int(order.created_at * 1000)
                if age_ms < self.self_trade_window_ms:
                    return f"opposing_order_open ({order.client_order_id})"

        return None

    async def confirm_order_submission(
        self,
        client_order_id: str,
        exchange_order_id: str,
    ) -> Optional[Order]:
        """Confirm order submitted to exchange"""
        async with self._lock:
            order = self.client_order_map.get(client_order_id)
            if not order:
                logger.error(f"Order {client_order_id} not found for confirmation")
                return None

            order.exchange_order_id = exchange_order_id
            order.status = OrderStatus.SUBMITTED
            order.submitted_at = int(time.time() * 1000)
            
            self.exchange_order_map[(order.venue, exchange_order_id)] = order
            self._record_audit("ORDER_SUBMITTED", order)
            logger.debug(f"Order {client_order_id} submitted")

            return order

    async def record_fill(
        self,
        client_order_id: str,
        fill_id: str,
        quantity: float,
        price: float,
        fee: float = 0.0,
    ) -> Optional[Order]:
        """Record a fill for an order"""
        async with self._lock:
            order = self.client_order_map.get(client_order_id)
            if not order:
                logger.error(f"Order {client_order_id} not found for fill")
                return None

            fill = OrderFill(
                fill_id=fill_id,
                timestamp=int(time.time() * 1000),
                quantity=quantity,
                price=price,
                fee=fee,
            )

            order.fills.append(fill)
            order.cumulative_quantity += quantity
            order.total_fee += fee
            
            total_value = sum(f.quantity * f.price for f in order.fills)
            order.average_fill_price = total_value / order.cumulative_quantity if order.cumulative_quantity > 0 else 0.0
            
            if abs(order.cumulative_quantity - order.quantity) < 1e-8:
                order.status = OrderStatus.FILLED
                order.filled_at = fill.timestamp
            elif order.cumulative_quantity > 0:
                order.status = OrderStatus.PARTIALLY_FILLED

            self._record_audit("ORDER_FILL", order)
            logger.info(
                f"Fill recorded for {client_order_id}: {quantity} @ {price} (fee={fee})"
            )

            return order

    async def cancel_order(
        self,
        client_order_id: str,
        reason: str = "",
    ) -> Tuple[bool, Optional[Order], str]:
        """Cancel an open order"""
        async with self._lock:
            order = self.client_order_map.get(client_order_id)
            if not order:
                return False, None, "order_not_found"

            if order.status not in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                return False, order, f"order_not_active ({order.status.value})"

            order.status = OrderStatus.CANCELLED
            self._record_audit("ORDER_CANCELLED", order)

            logger.info(f"Order {client_order_id} cancelled: {reason}")

            return True, order, "cancelled"

    def _record_audit(self, event_type: str, order: Order) -> None:
        """Record audit trail entry"""
        entry = {
            "timestamp": int(time.time() * 1000),
            "event_type": event_type,
            "client_order_id": order.client_order_id,
            "exchange_order_id": order.exchange_order_id,
            "symbol": order.symbol,
            "status": order.status.value,
            "cumulative_quantity": order.cumulative_quantity,
            "average_fill_price": order.average_fill_price,
        }

        self.audit_log.append(entry)

        # Enforce size limit
        if len(self.audit_log) > self.max_audit_log_size:
            self.audit_log = self.audit_log[-self.max_audit_log_size:]

    def get_order(self, client_order_id: str) -> Optional[Order]:
        """Get order by client ID"""
        return self.client_order_map.get(client_order_id)

    def get_orders_by_symbol(self, exchange: str, symbol: str) -> List[Order]:
        """Get all orders for a symbol"""
        return [
            o for o in self.orders.values()
            if o.venue == exchange and o.symbol == symbol
        ]

    def get_open_orders(self, exchange: Optional[str] = None) -> List[Order]:
        """Get all open/active orders"""
        orders = [
            o for o in self.orders.values()
            if o.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
        ]
        if exchange:
            orders = [o for o in orders if o.venue == exchange]
        return orders

    def get_filled_orders(self, exchange: Optional[str] = None) -> List[Order]:
        """Get all filled orders"""
        orders = [o for o in self.orders.values() if o.status == OrderStatus.FILLED]
        if exchange:
            orders = [o for o in orders if o.venue == exchange]
        return orders

    def get_stats(self) -> Dict:
        """Get order manager statistics"""
        open_orders = self.get_open_orders()
        filled_orders = self.get_filled_orders()

        total_value_filled = sum(
            o.cumulative_quantity * o.average_fill_price
            for o in filled_orders
        )

        return {
            "total_orders": len(self.orders),
            "open_orders": len(open_orders),
            "filled_orders": len(filled_orders),
            "total_fill_value": total_value_filled,
            "total_fees": sum(o.total_fee for o in filled_orders),
            "idempotency_records": self.idempotency.get_stats()["total_records"],
            "audit_log_size": len(self.audit_log),
        }

    async def run(self) -> None:
        """Start order manager"""
        self._running = True
        logger.info("OrderManager started")
        while self._running:
            await asyncio.sleep(60)

    async def stop(self) -> None:
        """Stop order manager"""
        self._running = False
        logger.info("OrderManager stopped")
