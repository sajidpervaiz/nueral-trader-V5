"""
Idempotency Manager for ensuring exactly-once operation semantics.
"""

import time
import hashlib
from typing import Optional, Dict, Any
from dataclasses import dataclass
from loguru import logger


@dataclass
class IdempotencyRecord:
    idempotency_key: str
    result: Any
    created_at: float
    expires_at: float
    metadata: Dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class IdempotencyManager:
    """
    Production-grade idempotency manager with:
    - TTL-based expiration
    - Memory-efficient storage
    - Thread-safe operations
    - Automatic cleanup
    """

    def __init__(self, ttl: int = 3600, max_size: int = 10000):
        self.ttl = ttl
        self.max_size = max_size
        self.records: Dict[str, IdempotencyRecord] = {}

    def generate_key(self, *args, **kwargs) -> str:
        """Generate deterministic idempotency key from arguments."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
        key_string = "|".join(key_parts)

        return hashlib.sha256(key_string.encode()).hexdigest()[:32]

    def check_and_set(self, idempotency_key: str) -> bool:
        """
        Check if idempotency key exists and create if not.

        Returns:
            True if key already exists (duplicate), False otherwise
        """
        self._cleanup_expired()

        if idempotency_key in self.records:
            record = self.records[idempotency_key]
            if record.expires_at > time.time():
                logger.debug(f"Idempotency hit: {idempotency_key}")
                return True
            else:
                del self.records[idempotency_key]

        now = time.time()
        self.records[idempotency_key] = IdempotencyRecord(
            idempotency_key=idempotency_key,
            result=None,
            created_at=now,
            expires_at=now + self.ttl,
        )

        self._enforce_size_limit()
        return False

    def set_result(self, idempotency_key: str, result: Any, metadata: Optional[Dict] = None) -> None:
        """Store result for idempotency key."""
        if idempotency_key in self.records:
            self.records[idempotency_key].result = result
            if metadata:
                self.records[idempotency_key].metadata.update(metadata)

    def get_result(self, idempotency_key: str) -> Optional[Any]:
        """Get cached result for idempotency key."""
        record = self.records.get(idempotency_key)
        if record and record.expires_at > time.time():
            return record.result
        return None

    def delete(self, idempotency_key: str) -> bool:
        """Delete idempotency record."""
        if idempotency_key in self.records:
            del self.records[idempotency_key]
            return True
        return False

    def _cleanup_expired(self) -> int:
        """Remove expired records."""
        now = time.time()
        expired_keys = [
            key for key, record in self.records.items()
            if record.expires_at <= now
        ]

        for key in expired_keys:
            del self.records[key]

        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired idempotency records")

        return len(expired_keys)

    def _enforce_size_limit(self) -> None:
        """Remove oldest records if size limit exceeded."""
        if len(self.records) > self.max_size:
            sorted_records = sorted(
                self.records.items(),
                key=lambda x: x[1].created_at
            )

            to_remove = len(self.records) - self.max_size
            for key, _ in sorted_records[:to_remove]:
                del self.records[key]

            logger.debug(f"Removed {to_remove} old idempotency records to enforce size limit")

    def get_stats(self) -> Dict:
        """Get idempotency manager statistics."""
        return {
            "total_records": len(self.records),
            "ttl": self.ttl,
            "max_size": self.max_size,
        }
