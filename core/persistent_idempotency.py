import os
import tempfile
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any

from loguru import logger

from .idempotency import IdempotencyManager, IdempotencyRecord


class PersistentIdempotencyManager(IdempotencyManager):
    """
    Idempotency manager with crash-safe file-based persistence.

    Uses atomic write (write to temp file, then os.replace) so a crash
    mid-save cannot corrupt the store.
    """

    def __init__(
        self,
        ttl: int = 3600,
        max_size: int = 10000,
        filepath: str = "idempotency_store.json",
        save_interval: float = 30.0,
    ):
        super().__init__(ttl=ttl, max_size=max_size)
        self.filepath = Path(filepath)
        self._save_interval = save_interval
        self._last_save: float = 0.0
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.filepath.exists():
            return
        try:
            raw = self.filepath.read_text(encoding="utf-8")
            if not raw.strip():
                return
            data = json.loads(raw)
            now = time.time()
            loaded = 0
            for key, rec in data.items():
                if not isinstance(rec, dict):
                    continue
                expires = rec.get("expires_at", 0)
                if expires > now:
                    self.records[key] = IdempotencyRecord(
                        idempotency_key=key,
                        result=rec.get("result"),
                        created_at=rec.get("created_at", now),
                        expires_at=expires,
                        metadata=rec.get("metadata", {}),
                    )
                    loaded += 1
            logger.debug("Idempotency store loaded: {} active records from {}", loaded, self.filepath)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Idempotency store corrupted, starting fresh: {}", exc)
        except OSError as exc:
            logger.warning("Could not read idempotency store {}: {}", self.filepath, exc)

    def _save(self) -> None:
        """Atomic save: write to temp file in same directory, then os.replace."""
        data = {
            k: {
                "result": v.result,
                "created_at": v.created_at,
                "expires_at": v.expires_at,
                "metadata": v.metadata,
            }
            for k, v in self.records.items()
        }
        try:
            self.filepath.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self.filepath.parent),
                suffix=".tmp",
                prefix=".idempotency_",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, str(self.filepath))
            except BaseException:
                # Clean up temp file on any failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Failed to persist idempotency store: {}", exc)

    def _maybe_save(self) -> None:
        self._dirty = True
        now = time.time()
        if now - self._last_save >= self._save_interval:
            self._save()
            self._last_save = now
            self._dirty = False

    def flush(self) -> None:
        """Force an immediate save if there are pending changes."""
        if self._dirty:
            self._save()
            self._last_save = time.time()
            self._dirty = False

    def check_and_set(self, idempotency_key: str) -> bool:
        res = super().check_and_set(idempotency_key)
        self._maybe_save()
        return res

    def set_result(self, idempotency_key: str, result: Any, metadata: Optional[Dict] = None) -> None:
        super().set_result(idempotency_key, result, metadata)
        self._maybe_save()

    def delete(self, idempotency_key: str) -> bool:
        res = super().delete(idempotency_key)
        self._maybe_save()
        return res
