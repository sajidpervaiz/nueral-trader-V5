"""
Order Manager with idempotent orders, lifecycle tracking, and self-trade prevention.
"""

import asyncio
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger


class OrderStatus(Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


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
class Order:
    order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float]
    status: OrderStatus
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0
    avg_fill_price: float = 0.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    exchange_order_id: Optional[str] = None
    venue: Optional[str] = None
    time_in_force: str = "GTC"
    reduce_only: bool = False
    user_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    stages: List[OrderStage] = field(default_factory=list)
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
    - Idempotency keys
    - Lifecycle tracking
    - Self-trade prevention
    - Order state management
    - Venue abstraction
    """

    def __init__(self):
        self.orders: Dict[str, Order] = {}
        self.client_order_map: Dict[str, Order] = {}
        self.exchange_order_map: Dict[str, Order] = {}
        self.pending_orders: Dict[str, Order] = {}
        self.user_orders: Dict[str, Dict[str, Order]] = {}

        self._lock = asyncio.Lock()

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float] = None,
        venue: Optional[str] = None,
        client_order_id: Optional[str] = None,
        user_id: Optional[str] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
    ) -> Order:
        """Create a new order with idempotency key."""
        async with self._lock:
            order_id = f"ord_{uuid.uuid4().hex[:12]}_{int(time.time() * 1000)}"

            if not client_order_id:
                client_order_id = f"cli_{uuid.uuid4().hex[:12]}"

            if client_order_id in self.client_order_map:
                logger.warning(f"Duplicate client_order_id detected: {client_order_id}")
                return self.client_order_map[client_order_id]

            order = Order(
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                status=OrderStatus.PENDING,
                venue=venue,
                user_id=user_id,
                reduce_only=reduce_only,
                time_in_force=time_in_force,
                tags=tags or [],
                metadata=metadata or {},
            )

            order.add_stage(OrderLifecycleStage.VALIDATION)

            self.orders[order_id] = order
            self.client_order_map[client_order_id] = order
            self.pending_orders[order_id] = order

            if user_id:
                if user_id not in self.user_orders:
                    self.user_orders[user_id] = {}
                self.user_orders[user_id][order_id] = order

            logger.info(f"Order created: {order_id} for {symbol}")
            return order

    async def submit_order(self, order: Order, exchange_order_id: str) -> bool:
        """Mark order as submitted to exchange."""
        async with self._lock:
            if order.order_id not in self.orders:
                logger.error(f"Order not found: {order.order_id}")
                return False

            order.status = OrderStatus.SUBMITTED
            order.exchange_order_id = exchange_order_id
            order.add_stage(OrderLifecycleStage.SUBMISSION)

            if exchange_order_id:
                self.exchange_order_map[exchange_order_id] = order

            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]

            logger.info(f"Order submitted: {order.order_id} -> {exchange_order_id}")
            return True

    async def update_order_status(
        self,
        order_id: str,
        status: OrderStatus,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update order status."""
        async with self._lock:
            order = self.get_order(order_id)
            if not order:
                return False

            old_status = order.status
            order.status = status
            order.error_message = error_message
            order.updated_at = time.time()

            if status in [OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.FAILED]:
                if order.order_id in self.pending_orders:
                    del self.pending_orders[order.order_id]

            logger.info(f"Order status updated: {order_id} {old_status} -> {status}")
            return True

    async def update_order_fill(
        self,
        order_id: str,
        fill_qty: float,
        fill_price: float,
    ) -> bool:
        """Update order with fill information."""
        async with self._lock:
            order = self.get_order(order_id)
            if not order:
                return False

            order.update_fill(fill_qty, fill_price)
            logger.debug(f"Order fill updated: {order_id} +{fill_qty} @ {fill_price}")
            return True

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        async with self._lock:
            order = self.get_order(order_id)
            if not order:
                logger.error(f"Order not found: {order_id}")
                return False

            order.status = OrderStatus.CANCELLED
            order.add_stage(OrderLifecycleStage.CANCELLATION)

            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]

            logger.info(f"Order cancelled: {order_id}")
            return True

    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by order ID."""
        return self.orders.get(order_id)

    def get_order_by_client_id(self, client_order_id: str) -> Optional[Order]:
        """Get order by client order ID."""
        return self.client_order_map.get(client_order_id)

    def get_order_by_exchange_id(self, exchange_order_id: str) -> Optional[Order]:
        """Get order by exchange order ID."""
        return self.exchange_order_map.get(exchange_order_id)

    def get_user_orders(self, user_id: str) -> List[Order]:
        """Get all orders for a user."""
        return list(self.user_orders.get(user_id, {}).values())

    def get_pending_orders(self) -> List[Order]:
        """Get all pending orders."""
        return list(self.pending_orders.values())

    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        """Get all orders for a symbol."""
        return [o for o in self.orders.values() if o.symbol == symbol]

    async def check_self_trade_prevention(
        self,
        user_id: str,
        symbol: str,
        side: str,
    ) -> bool:
        """
        Check if order would result in self-trade.

        Returns True if self-trade would occur.
        """
        user_orders = self.get_user_orders(user_id)
        pending_orders = [
            o for o in user_orders
            if o.symbol == symbol
            and o.side != side
            and o.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]
        ]

        if pending_orders:
            logger.warning(
                f"Self-trade prevention triggered for user {user_id} "
                f"on {symbol}. Opposing orders: {[o.order_id for o in pending_orders]}"
            )
            return True

        return False

    async def cleanup_old_orders(self, max_age_hours: int = 24) -> int:
        """Clean up old completed orders."""
        cutoff_time = time.time() - (max_age_hours * 3600)
        orders_to_remove = []

        for order_id, order in self.orders.items():
            if (
                order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]
                and order.updated_at < cutoff_time
            ):
                orders_to_remove.append(order_id)

        for order_id in orders_to_remove:
            order = self.orders.pop(order_id)
            if order.client_order_id:
                self.client_order_map.pop(order.client_order_id, None)
            if order.exchange_order_id:
                self.exchange_order_map.pop(order.exchange_order_id, None)
            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]
            if order.user_id and order.user_id in self.user_orders:
                self.user_orders[order.user_id].pop(order.order_id, None)

        logger.info(f"Cleaned up {len(orders_to_remove)} old orders")
        return len(orders_to_remove)

    def get_order_statistics(self) -> Dict:
        """Get order statistics."""
        status_counts = {}
        for order in self.orders.values():
            status = order.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_orders": len(self.orders),
            "pending_orders": len(self.pending_orders),
            "status_distribution": status_counts,
            "users_count": len(self.user_orders),
        }
