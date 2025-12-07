"""
Unified caching system for in-memory, local (/tmp), and Firestore persistence.
This version replaces all GCS references with Firestore via firestore_cache.FirestoreCache.
"""

import logging
import os
import pickle
from pathlib import Path
from typing import Any

from firestore_cache import FirestoreCache

logger = logging.getLogger("cache.main")


class Cache:
    def __init__(self, prefix: str = "cache", use_cloud_storage: bool = False):
        self.prefix = prefix
        self.use_cloud_storage = use_cloud_storage
        self.cache: dict[str, Any] = {}
        self.logging = logger
        self.firestore_cache: FirestoreCache | None = None

        if self.use_cloud_storage:
            firestore_emulator = os.getenv("FIRESTORE_EMULATOR_HOST")
            auth_emulator = os.getenv("FIREBASE_AUTH_EMULATOR_HOST")
            functions_emulator = os.getenv("FUNCTIONS_EMULATOR")
            is_test_env = os.getenv("ENVIRONMENT", "").lower() == "test"
            gcloud_project = os.getenv("GCLOUD_PROJECT", "")
            is_gcloud_local = "localhost" in gcloud_project or "127.0.0.1" in gcloud_project

            is_emulator_or_test = (
                firestore_emulator
                or auth_emulator
                or functions_emulator
                or is_gcloud_local
                or is_test_env
            )

            if is_emulator_or_test:
                self.use_cloud_storage = False
                msg = (
                    "🧪 Test environment detected - Firestore cache disabled"
                    if is_test_env
                    else (
                        f"🔧 Emulator mode detected - Firestore cache disabled. "
                        f"(FIRESTORE_EMULATOR_HOST={firestore_emulator}, "
                        f"FIREBASE_AUTH_EMULATOR_HOST={auth_emulator}, "
                        f"FUNCTIONS_EMULATOR={functions_emulator})"
                    )
                )
                self.logging.info(msg)
            else:
                # Use default collection and prefix for legacy compatibility
                collection = os.getenv("CACHE_FIRESTORE_COLLECTION", "cache")
                self.firestore_cache = FirestoreCache(
                    collection=collection, prefix=self.prefix, version=""
                )
                self.logging.info(
                    f"☁️ Firestore cache enabled: collection={self.firestore_cache.collection}, "
                    f"prefix={self.prefix}"
                )

        # local cache directory
        self.local_dir = Path("/tmp/cache") / self.prefix
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.logging.info(f"📁 Local cache directory: {self.local_dir}")

    # ---------------------------------------------------------------------
    # Core methods
    # ---------------------------------------------------------------------
    def _get_local_path(self, name: str) -> Path:
        return self.local_dir / f"{name}.pkl"

    async def restore_from_file(self, name: str) -> Any | None:
        """Restore from local cache, then Firestore if missing."""
        filename = self._get_local_path(name)

        if filename.exists():
            try:
                with open(filename, "rb") as f:
                    data = pickle.load(f)
                    return data
            except Exception as e:
                self.logging.warning(f"⚠️ Error reading local cache {filename}: {e}")

        if self.use_cloud_storage and self.firestore_cache is not None:
            try:
                key = f"{self.prefix}/{name}"
                self.logging.info(f"Attempting to restore {key} from Firestore cache")
                entry = await self.firestore_cache.get(key)
                if entry:
                    await self.write_to_file(entry, no_storage=True)
                    return entry
            except Exception as e:
                self.logging.warning(f"⚠️ Firestore restore failed for {name}: {e}")

        return None

    async def write_to_file(self, cacheEntry: Any, no_storage: bool = False) -> None:
        """Write cache to local disk and optionally Firestore."""
        fullpath = self._get_local_path(cacheEntry.filename)
        try:
            with open(fullpath, "wb") as f:
                pickle.dump(cacheEntry, f)
            self.logging.debug(f"💾 Wrote {fullpath} to local cache")
        except Exception as e:
            self.logging.warning(f"⚠️ Failed to write local cache {fullpath}: {e}")
            return

        if self.use_cloud_storage and not no_storage and self.firestore_cache is not None:
            try:
                key = f"{self.prefix}/{cacheEntry.filename}"
                await self.firestore_cache.set(key, cacheEntry)
                self.logging.info(f"☁️ Firestore cache updated for {key}")
            except Exception as e:
                self.logging.warning(f"⚠️ Firestore write failed for {cacheEntry.filename}: {e}")

    async def delete_expired(self, name: str, cacheEntry: Any) -> None:
        """Delete expired cache from disk and Firestore."""
        fullpath = self._get_local_path(name)
        if fullpath.exists():
            try:
                os.remove(fullpath)
                self.logging.info(f"🗑️ Deleted expired cache {fullpath}")
            except Exception as e:
                self.logging.warning(f"⚠️ Could not delete local cache {fullpath}: {e}")

        if self.use_cloud_storage and self.firestore_cache is not None:
            try:
                key = f"{self.prefix}/{cacheEntry.filename}"
                await self.firestore_cache.delete(key)
            except Exception as e:
                self.logging.warning(f"⚠️ Failed to delete Firestore entry {key}: {e}")

    # ---------------------------------------------------------------------
    # Cache interface
    # ---------------------------------------------------------------------
    async def get(self, key: str) -> Any | None:
        """Retrieve cache entry from memory, local, or Firestore."""
        if key in self.cache:
            return self.cache[key]

        entry = await self.restore_from_file(key)
        if entry:
            self.cache[key] = entry
            return entry
        return None

    async def set(self, key: str, value: Any) -> None:
        """Set cache entry and persist."""
        self.cache[key] = value
        await self.write_to_file(value)

    async def delete(self, key: str) -> None:
        """Delete cache entry from memory, local disk, and Firestore."""
        # Remove from memory cache
        if key in self.cache:
            del self.cache[key]
            self.logging.debug(f"🗑️ Removed {key} from memory cache")

        # Delete from local disk
        fullpath = self._get_local_path(key)
        if fullpath.exists():
            try:
                os.remove(fullpath)
                self.logging.info(f"🗑️ Deleted local cache {fullpath}")
            except Exception as e:
                self.logging.warning(f"⚠️ Could not delete local cache {fullpath}: {e}")

        # Delete from Firestore
        if self.use_cloud_storage and self.firestore_cache is not None:
            try:
                firestore_key = f"{self.prefix}/{key}"
                await self.firestore_cache.delete(firestore_key)
                self.logging.info(f"🗑️ Deleted Firestore cache {firestore_key}")
            except Exception as e:
                self.logging.warning(f"⚠️ Failed to delete Firestore entry {firestore_key}: {e}")
