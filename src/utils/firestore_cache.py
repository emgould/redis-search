"""
Firestore-backed persistent cache layer (drop-in replacement for GCS cache).
Stores serialized Python objects as base64-encoded pickled strings in Firestore.

Note: If pickle serialization fails or document exceeds 1MB, the write is skipped
and data falls back to disk/GCS cache layers.
"""

import asyncio
import base64
import logging
import os
import pickle
import time
from typing import Any

from google.cloud import firestore  # type: ignore[import, attr-defined]

logger = logging.getLogger("cache.firestore")

READ_TIMEOUT = int(os.getenv("CACHE_FS_READ_TIMEOUT", "2"))  # 2 second timeout to fail fast
WRITE_RETRIES = int(os.getenv("CACHE_FS_WRITE_RETRIES", "3"))
# Firestore document size limit is 1MB (1,048,576 bytes)
# Use slightly lower limit to account for metadata overhead
MAX_DOCUMENT_SIZE = 1_000_000  # ~1MB with safety margin


def _now() -> float:
    return time.time()


def _encode_pickle(obj: Any) -> str | None:
    """
    Serialize and base64-encode a Python object for Firestore.

    Returns None if serialization fails - caller should fall back to GCS/disk cache.
    """
    try:
        pickled = pickle.dumps(obj)
        return base64.b64encode(pickled).decode("utf-8")
    except (TypeError, AttributeError, pickle.PicklingError) as e:
        logger.warning(f"⚠️ Pickle serialization failed: {e}")
        return None


def _decode_pickle(data: str) -> Any:
    """Decode base64 Firestore data back into a Python object."""
    raw = base64.b64decode(data.encode("utf-8"))
    return pickle.loads(raw)


# ---------------------------------------------------------------------
# Drop-in Cache Interface
# ---------------------------------------------------------------------
class FirestoreCache:
    """Drop-in replacement for GCSCache backend."""

    def __init__(self, collection: str, prefix: str = "", version: str = "") -> None:
        self.collection = collection
        self.prefix = prefix
        self.version = version
        self._db: firestore.Client | None = None  # Lazy initialization

    @property
    def db(self) -> firestore.Client:
        """Lazily initialize Firestore client on first access."""
        if self._db is None:
            self._db = firestore.Client()
        return self._db

    def _build_document_path(self, key: str) -> str:
        """Build hierarchical document path: {prefix}/{version}/{key}"""
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        if self.version:
            parts.append(self.version)
        parts.append(key)
        return "/".join(parts)

    async def read_from_store(self, key: str) -> Any | None:
        """Read cached value from Firestore, fail fast if missing."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, fall back to get_event_loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # Event loop is closing or closed
                logger.warning(f"⚠️ Firestore read aborted for {key} (no event loop available)")
                return None

        def _get() -> Any | None:
            doc_path = self._build_document_path(key)
            doc_ref = self.db.collection(self.collection).document(doc_path)
            doc = doc_ref.get()
            if not doc.exists:
                return None
            data = doc.to_dict()
            if not data or "value" not in data:
                return None
            try:
                return _decode_pickle(data["value"])
            except Exception as e:
                logger.warning(f"⚠️ Failed to unpickle {doc_path}: {e}")
                return None

        try:
            return await asyncio.wait_for(loop.run_in_executor(None, _get), timeout=READ_TIMEOUT)
        except RuntimeError as e:
            # Handle case where event loop is shutting down
            error_msg = str(e)
            if "cannot schedule" in error_msg or "Event loop is closed" in error_msg:
                logger.warning(
                    f"⚠️ Firestore read aborted for {key} (event loop shutting down): {e}"
                )
                return None
            # Re-raise if it's a different RuntimeError
            raise
        except TimeoutError:
            logger.warning(f"⚠️ Firestore read timeout for {key}")
            return None
        except Exception as e:
            logger.warning(f"⚠️ Firestore read failed for {key}: {e}")
            return None

    async def write_to_store(self, key: str, value: Any) -> None:
        """Write cached value to Firestore (awaited, not background task)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, fall back to get_event_loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # Event loop is closing or closed
                logger.warning(f"⚠️ Firestore write aborted for {key} (no event loop available)")
                return

        encoded_value = _encode_pickle(value)

        # Check if serialization failed
        if encoded_value is None:
            logger.warning(
                f"⚠️ Skipping Firestore cache write for {key}: "
                f"failed to serialize object. Data will remain in local/disk cache only."
            )
            return

        size = len(encoded_value)

        # Pre-check: skip write if document would exceed Firestore's 1MB limit
        if size > MAX_DOCUMENT_SIZE:
            logger.warning(
                f"⚠️ Skipping Firestore cache write for {key}: "
                f"data size ({size:,} bytes) exceeds 1MB limit. "
                f"Data will remain in local/disk cache only."
            )
            return

        data = {"value": encoded_value, "timestamp": _now()}

        def _sync_write() -> None:
            """Synchronous Firestore write executed in executor."""
            doc_path = self._build_document_path(key)
            for attempt in range(1, WRITE_RETRIES + 1):
                try:
                    self.db.collection(self.collection).document(doc_path).set(data)
                    logger.debug(f"✅ Cached → {doc_path} ({size:,} bytes)")
                    return
                except Exception as e:
                    error_msg = str(e).lower()
                    # Don't retry if this is a document size error from Firestore
                    if (
                        "document size" in error_msg
                        or "exceeds the maximum" in error_msg
                        or "too large" in error_msg
                        or "1mb" in error_msg
                    ):
                        logger.warning(
                            f"⚠️ Firestore rejected {key} due to size limit: {e}. "
                            f"Data will remain in local/disk cache only."
                        )
                        return  # Don't retry size errors
                    if attempt < WRITE_RETRIES:
                        wait = min(2**attempt, 10)
                        logger.warning(
                            f"⚠️ Firestore write failed for {doc_path} "
                            f"(attempt {attempt}/{WRITE_RETRIES}): {e}. Retrying in {wait}s..."
                        )
                        time.sleep(wait)
            logger.error(f"❌ Firestore write permanently failed for {doc_path}")

        try:
            # Run synchronous write in executor to avoid blocking event loop
            # Short timeout (2s) to avoid blocking responses - if it times out, data stays in memory cache
            await asyncio.wait_for(loop.run_in_executor(None, _sync_write), timeout=2.0)
        except RuntimeError as e:
            # Handle case where event loop is shutting down
            error_msg = str(e)
            if "cannot schedule" in error_msg or "Event loop is closed" in error_msg:
                logger.warning(
                    f"⚠️ Firestore write aborted for {key} (event loop shutting down): {e}"
                )
                return
            # Re-raise if it's a different RuntimeError
            raise
        except TimeoutError:
            logger.warning(f"⚠️ Firestore write timeout for {key} (30s)")
        except Exception as e:
            logger.warning(f"⚠️ Firestore write failed for {key}: {e}")

    async def get(self, key: str) -> Any | None:
        return await self.read_from_store(key)

    async def set(self, key: str, value: Any) -> None:
        await self.write_to_store(key, value)

    async def delete(self, key: str) -> None:
        """Delete a cached entry from Firestore."""
        loop = asyncio.get_event_loop()

        def _delete() -> None:
            try:
                doc_path = self._build_document_path(key)
                self.db.collection(self.collection).document(doc_path).delete()
                logger.debug(f"🗑️ Deleted {doc_path} from Firestore cache")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete {doc_path} from Firestore cache: {e}")

        try:
            await asyncio.wait_for(loop.run_in_executor(None, _delete), timeout=READ_TIMEOUT)
        except TimeoutError:
            logger.warning(f"⚠️ Firestore delete timeout for {key}")
        except Exception as e:
            logger.warning(f"⚠️ Firestore delete failed for {key}: {e}")
