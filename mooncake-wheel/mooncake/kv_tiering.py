"""Agent-aware KV object lifecycle on top of Mooncake Store.

This is a control-plane adapter. Mooncake Store and Transfer Engine remain
responsible for placement, replication, RDMA, host memory, and SSD I/O.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import hashlib
import json
import math
import threading
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol


_SCHEMA_VERSION = 1
_KEY_PREFIX = "__mooncake_kv_v1__"
_OBJECT_NOT_FOUND = -704
_OBJECT_ALREADY_EXISTS = -705


class KVTier(str, enum.Enum):
    HOST = "host"
    SSD = "ssd"
    REMOTE = "remote"


class KVResult(str, enum.Enum):
    OK = "ok"
    MISS = "miss"
    CANCELLED = "cancelled"
    DEADLINE = "deadline"
    BACKPRESSURE = "backpressure"
    QUOTA = "quota"
    INCOMPATIBLE = "incompatible"
    CORRUPT = "corrupt"
    IO_ERROR = "io_error"
    EXPIRED = "expired"


class KVTieringError(RuntimeError):
    """Error with a stable, low-cardinality result code."""

    def __init__(self, result: KVResult, message: str):
        super().__init__(message)
        self.result = result


class KVStoreBackend(Protocol):
    """Small boundary already implemented by MooncakeDistributedStore."""

    def put(self, key: str, value: bytes, *args: Any, **kwargs: Any) -> int: ...

    def get(self, key: str) -> bytes | bytearray | memoryview | None: ...

    def remove(self, key: str, force: bool = False) -> int: ...


def _bounded_text(name: str, value: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{name} must be a non-empty string up to {maximum} bytes")
    if any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} contains a control character")
    return value


@dataclass(frozen=True, slots=True)
class KVIdentity:
    """Exact compatibility identity without prompts or process-local handles."""

    namespace: str
    identity_digest: str
    model_version: str
    tokenizer_version: str
    template_version: str
    render_version: str
    kv_format_version: str
    parallel_layout_version: str

    def __post_init__(self) -> None:
        for item in dataclasses.fields(self):
            object.__setattr__(
                self, item.name, _bounded_text(item.name, getattr(self, item.name))
            )
        digest = self.identity_digest.lower()
        if len(digest) < 16 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("identity_digest must be a hexadecimal digest")
        object.__setattr__(self, "identity_digest", digest)

    def compatibility_tuple(self) -> tuple[str, ...]:
        return tuple(getattr(self, item.name) for item in dataclasses.fields(self))


@dataclass(frozen=True, slots=True)
class KVObjectDescriptor:
    identity: KVIdentity
    token_start: int
    token_end: int
    byte_length: int
    checksum: str
    producer_epoch: int
    created_at_unix_s: float
    ttl_s: float
    lease_until_unix_s: float
    tier: KVTier

    def __post_init__(self) -> None:
        if self.token_start < 0 or self.token_end <= self.token_start:
            raise ValueError("invalid token range")
        if self.byte_length <= 0 or self.producer_epoch < 0:
            raise ValueError("invalid byte length or producer epoch")
        for name in ("created_at_unix_s", "ttl_s", "lease_until_unix_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        checksum = self.checksum.lower()
        if len(checksum) != 64 or any(c not in "0123456789abcdef" for c in checksum):
            raise ValueError("checksum must be a SHA-256 digest")
        object.__setattr__(self, "checksum", checksum)
        if not isinstance(self.tier, KVTier):
            object.__setattr__(self, "tier", KVTier(self.tier))

    @property
    def expires_at_unix_s(self) -> float:
        return self.created_at_unix_s + self.ttl_s

    def expired(self, now: float) -> bool:
        return now >= min(self.expires_at_unix_s, self.lease_until_unix_s)

    def object_id(self) -> str:
        material = {
            "identity": self.identity.compatibility_tuple(),
            "token_start": self.token_start,
            "token_end": self.token_end,
            "byte_length": self.byte_length,
            "checksum": self.checksum,
            "producer_epoch": self.producer_epoch,
        }
        encoded = json.dumps(material, separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    def to_manifest(self, data_key: str) -> bytes:
        body = {
            "schema_version": _SCHEMA_VERSION,
            "identity": dataclasses.asdict(self.identity),
            "token_start": self.token_start,
            "token_end": self.token_end,
            "byte_length": self.byte_length,
            "checksum": self.checksum,
            "producer_epoch": self.producer_epoch,
            "created_at_unix_s": self.created_at_unix_s,
            "ttl_s": self.ttl_s,
            "lease_until_unix_s": self.lease_until_unix_s,
            "tier": self.tier.value,
            "data_key": data_key,
        }
        return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()

    @classmethod
    def from_manifest(cls, payload: bytes) -> tuple["KVObjectDescriptor", str]:
        try:
            body = json.loads(payload)
            if body.pop("schema_version") != _SCHEMA_VERSION:
                raise ValueError("unsupported manifest schema")
            data_key = body.pop("data_key")
            body["identity"] = KVIdentity(**body["identity"])
            body["tier"] = KVTier(body["tier"])
            return cls(**body), _bounded_text("data_key", data_key, 512)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise KVTieringError(KVResult.CORRUPT, "invalid KV manifest") from exc


@dataclass(frozen=True, slots=True)
class KVExpectedLayout:
    identity: KVIdentity
    token_start: int
    token_end: int
    byte_length: int

    def validate(self, descriptor: KVObjectDescriptor) -> None:
        if (
            self.identity.compatibility_tuple()
            != descriptor.identity.compatibility_tuple()
        ):
            raise KVTieringError(KVResult.INCOMPATIBLE, "KV identity mismatch")
        if (self.token_start, self.token_end) != (
            descriptor.token_start,
            descriptor.token_end,
        ):
            raise KVTieringError(KVResult.INCOMPATIBLE, "KV token range mismatch")
        if self.byte_length != descriptor.byte_length:
            raise KVTieringError(KVResult.INCOMPATIBLE, "KV byte length mismatch")


@dataclass(frozen=True, slots=True)
class KVLookup:
    result: KVResult
    descriptor: KVObjectDescriptor | None = None
    lookup_s: float = 0.0


@dataclass(frozen=True, slots=True)
class KVTransfer:
    result: KVResult
    descriptor: KVObjectDescriptor | None = None
    payload: bytes | None = None
    lookup_s: float = 0.0
    queue_s: float = 0.0
    transfer_s: float = 0.0
    restore_s: float = 0.0
    effective_bandwidth_bytes_s: float = 0.0


@dataclass(slots=True)
class KVTieringConfig:
    max_concurrent_io: int = 8
    max_queued_io: int = 64
    max_retries: int = 2
    retry_base_s: float = 0.01
    max_namespace_bytes: int = 64 * 1024**3
    max_object_bytes: int = 8 * 1024**3
    default_deadline_s: float = 30.0
    clock: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic

    def __post_init__(self) -> None:
        positive = (
            self.max_concurrent_io,
            self.max_queued_io,
            self.max_namespace_bytes,
            self.max_object_bytes,
        )
        if any(value <= 0 for value in positive) or self.max_retries < 0:
            raise ValueError("invalid KV tiering limits")


class KVTieringMetrics:
    """Process-local metrics with fixed operation/result/tier labels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._operations: Counter[tuple[str, str, str]] = Counter()
        self._seconds: Counter[tuple[str, str]] = Counter()
        self._bytes: Counter[tuple[str, str]] = Counter()

    def record(
        self, operation: str, result: KVResult, tier: KVTier, **values: float
    ) -> None:
        with self._lock:
            self._operations[(operation, result.value, tier.value)] += 1
            for name, value in values.items():
                target = self._bytes if name.endswith("_bytes") else self._seconds
                target[(operation, name)] += value

    def snapshot(self) -> dict[str, Mapping[tuple[str, ...], float]]:
        with self._lock:
            return {
                "operations": dict(self._operations),
                "seconds": dict(self._seconds),
                "bytes": dict(self._bytes),
            }


@dataclass(slots=True)
class _Operation:
    operation_id: str
    priority: int
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)


class MooncakeKVTierManager:
    """Safe KV lifecycle adapter backed by an initialized Mooncake Store."""

    def __init__(
        self,
        store: KVStoreBackend,
        config: KVTieringConfig | None = None,
        metrics: KVTieringMetrics | None = None,
    ) -> None:
        self._store = store
        self._config = config or KVTieringConfig()
        self.metrics = metrics or KVTieringMetrics()
        self._io_slots = asyncio.Semaphore(self._config.max_concurrent_io)
        self._state_lock = asyncio.Lock()
        self._pending = 0
        self._operations: dict[str, _Operation] = {}
        # Process-local in-flight admission guard only. Mooncake Store remains
        # authoritative for distributed tenant quota and durable capacity.
        self._reserved_bytes: defaultdict[str, int] = defaultdict(int)

    @staticmethod
    def checksum(payload: bytes | bytearray | memoryview) -> str:
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def manifest_key(identity: KVIdentity, token_start: int, token_end: int) -> str:
        scope = hashlib.sha256(
            (identity.namespace + "\0" + identity.identity_digest).encode()
        ).hexdigest()
        return f"{_KEY_PREFIX}/manifest/{scope}/{token_start}-{token_end}"

    async def _enter(self, operation: _Operation, deadline: float) -> float:
        queued_at = self._config.monotonic()
        async with self._state_lock:
            limit = self._config.max_queued_io + self._config.max_concurrent_io
            if self._pending >= limit:
                raise KVTieringError(KVResult.BACKPRESSURE, "KV I/O queue is full")
            self._pending += 1
            self._operations[operation.operation_id] = operation
        try:
            timeout = deadline - self._config.monotonic()
            if timeout <= 0:
                raise TimeoutError
            await asyncio.wait_for(self._io_slots.acquire(), timeout)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            await self._leave(operation, False)
            raise KVTieringError(
                KVResult.DEADLINE, "deadline expired in KV queue"
            ) from exc
        if operation.cancelled.is_set():
            self._io_slots.release()
            await self._leave(operation, False)
            raise KVTieringError(KVResult.CANCELLED, "KV operation cancelled")
        return self._config.monotonic() - queued_at

    async def _leave(self, operation: _Operation, release_slot: bool = True) -> None:
        if release_slot:
            self._io_slots.release()
        async with self._state_lock:
            self._pending = max(0, self._pending - 1)
            self._operations.pop(operation.operation_id, None)

    async def cancel_low_value(self, maximum_priority: int) -> int:
        async with self._state_lock:
            selected = [
                op
                for op in self._operations.values()
                if op.priority <= maximum_priority
            ]
            for operation in selected:
                operation.cancelled.set()
            return len(selected)

    async def _call_store(self, method: str, *args: Any, deadline: float) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._config.max_retries + 1):
            if self._config.monotonic() >= deadline:
                raise KVTieringError(KVResult.DEADLINE, "KV store deadline expired")
            try:
                # Store calls are synchronous C++ operations. If the caller is
                # cancelled, wait for the in-flight call to reach a known state
                # before running two-phase cleanup; otherwise a late put could
                # recreate staging data after cleanup has already completed.
                task = asyncio.create_task(
                    asyncio.to_thread(getattr(self._store, method), *args)
                )
                try:
                    value = await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
                if self._config.monotonic() >= deadline:
                    raise KVTieringError(
                        KVResult.DEADLINE,
                        f"Mooncake Store {method} completed after deadline",
                    )
                return value
            except KVTieringError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt == self._config.max_retries:
                    break
                delay = self._config.retry_base_s * (2**attempt)
                if self._config.monotonic() + delay >= deadline:
                    break
                await asyncio.sleep(delay)
        raise KVTieringError(
            KVResult.IO_ERROR, f"Mooncake Store {method} failed"
        ) from last_error

    async def lookup(
        self, expected: KVExpectedLayout, *, deadline_s: float | None = None
    ) -> KVLookup:
        started = self._config.monotonic()
        deadline = started + (
            self._config.default_deadline_s if deadline_s is None else deadline_s
        )
        key = self.manifest_key(
            expected.identity, expected.token_start, expected.token_end
        )
        try:
            payload = await self._call_store("get", key, deadline=deadline)
            elapsed = self._config.monotonic() - started
            if payload is None:
                return KVLookup(KVResult.MISS, lookup_s=elapsed)
            descriptor, _ = KVObjectDescriptor.from_manifest(bytes(payload))
            if descriptor.expired(self._config.clock()):
                await self.delete(
                    expected,
                    deadline_s=max(0.001, deadline - self._config.monotonic()),
                )
                return KVLookup(KVResult.EXPIRED, descriptor, elapsed)
            expected.validate(descriptor)
            return KVLookup(KVResult.OK, descriptor, elapsed)
        except KVTieringError as exc:
            return KVLookup(
                exc.result,
                lookup_s=self._config.monotonic() - started,
            )

    @staticmethod
    def _same_manifest(
        payload: bytes | bytearray | memoryview,
        descriptor: KVObjectDescriptor,
    ) -> tuple[bool, str | None]:
        try:
            existing_descriptor, existing_data_key = KVObjectDescriptor.from_manifest(
                bytes(payload)
            )
        except KVTieringError:
            return False, None
        return existing_descriptor == descriptor, existing_data_key

    async def _cleanup_unpublished_data(self, manifest_key: str, data_key: str) -> None:
        """Best-effort cleanup that never removes published immutable data."""
        deadline = self._config.monotonic() + min(1.0, self._config.default_deadline_s)
        try:
            manifest = await self._call_store("get", manifest_key, deadline=deadline)
            if manifest is not None:
                try:
                    _, published_data_key = KVObjectDescriptor.from_manifest(
                        bytes(manifest)
                    )
                except KVTieringError:
                    return
                if published_data_key == data_key:
                    return
            await self._call_store("remove", data_key, True, deadline=deadline)
        except (KVTieringError, asyncio.CancelledError):
            # Without a manifest this object is undiscoverable. Store lease and
            # expiry handling is the final cleanup path if this attempt fails.
            pass

    async def demote(
        self,
        descriptor: KVObjectDescriptor,
        payload: bytes | bytearray | memoryview,
        *,
        priority: int = 0,
        deadline_s: float | None = None,
    ) -> KVTransfer:
        data = bytes(payload)
        if (
            len(data) != descriptor.byte_length
            or self.checksum(data) != descriptor.checksum
        ):
            raise KVTieringError(KVResult.CORRUPT, "payload does not match descriptor")
        if descriptor.byte_length > self._config.max_object_bytes:
            raise KVTieringError(KVResult.QUOTA, "KV object exceeds quota")
        if descriptor.expired(self._config.clock()):
            raise KVTieringError(KVResult.EXPIRED, "KV lease is expired")

        operation = _Operation(uuid.uuid4().hex, priority)
        deadline = self._config.monotonic() + (
            self._config.default_deadline_s if deadline_s is None else deadline_s
        )
        queue_s = await self._enter(operation, deadline)
        namespace = descriptor.identity.namespace
        reserved = False
        staging_key = (
            f"{_KEY_PREFIX}/staging/{descriptor.object_id()}/{operation.operation_id}"
        )
        manifest_key = self.manifest_key(
            descriptor.identity, descriptor.token_start, descriptor.token_end
        )
        transfer_started = self._config.monotonic()
        result = KVResult.IO_ERROR
        try:
            async with self._state_lock:
                if (
                    self._reserved_bytes[namespace] + descriptor.byte_length
                    > self._config.max_namespace_bytes
                ):
                    raise KVTieringError(KVResult.QUOTA, "KV namespace quota exceeded")
                self._reserved_bytes[namespace] += descriptor.byte_length
                reserved = True

            status = await self._call_store("put", staging_key, data, deadline=deadline)
            if status not in (0, _OBJECT_ALREADY_EXISTS):
                raise KVTieringError(
                    KVResult.IO_ERROR, f"KV data put returned {status}"
                )
            if operation.cancelled.is_set():
                raise KVTieringError(KVResult.CANCELLED, "KV demotion cancelled")

            verified = await self._call_store("get", staging_key, deadline=deadline)
            if verified is None or len(verified) != descriptor.byte_length:
                raise KVTieringError(KVResult.CORRUPT, "staged KV length mismatch")
            if self.checksum(verified) != descriptor.checksum:
                raise KVTieringError(KVResult.CORRUPT, "staged KV checksum mismatch")
            if operation.cancelled.is_set():
                raise KVTieringError(KVResult.CANCELLED, "KV demotion cancelled")

            manifest_payload = descriptor.to_manifest(staging_key)
            existing_manifest = await self._call_store(
                "get", manifest_key, deadline=deadline
            )
            if existing_manifest is None:
                status = await self._call_store(
                    "put", manifest_key, manifest_payload, deadline=deadline
                )
                if status not in (0, _OBJECT_ALREADY_EXISTS):
                    raise KVTieringError(
                        KVResult.IO_ERROR, f"KV manifest put returned {status}"
                    )
            published_manifest = await self._call_store(
                "get", manifest_key, deadline=deadline
            )
            if published_manifest is None:
                raise KVTieringError(
                    KVResult.IO_ERROR,
                    "KV manifest publication is not visible",
                )
            same_descriptor, published_data_key = self._same_manifest(
                published_manifest, descriptor
            )
            if not same_descriptor:
                raise KVTieringError(
                    KVResult.INCOMPATIBLE,
                    "a different KV object is already published for this range",
                )
            if published_data_key != staging_key:
                await self._cleanup_unpublished_data(manifest_key, staging_key)
            result = KVResult.OK
            transfer_s = self._config.monotonic() - transfer_started
            bandwidth = descriptor.byte_length / transfer_s if transfer_s > 0 else 0.0
            return KVTransfer(
                result,
                descriptor,
                queue_s=queue_s,
                transfer_s=transfer_s,
                effective_bandwidth_bytes_s=bandwidth,
            )
        except asyncio.CancelledError:
            operation.cancelled.set()
            result = KVResult.CANCELLED
            raise
        except KVTieringError as exc:
            result = exc.result
            raise
        finally:
            if result is not KVResult.OK:
                await self._cleanup_unpublished_data(manifest_key, staging_key)
            if reserved:
                async with self._state_lock:
                    self._reserved_bytes[namespace] = max(
                        0, self._reserved_bytes[namespace] - descriptor.byte_length
                    )
            transfer_s = self._config.monotonic() - transfer_started
            self.metrics.record(
                "demote",
                result,
                descriptor.tier,
                queue_s=queue_s,
                transfer_s=transfer_s,
                transferred_bytes=descriptor.byte_length
                if result is KVResult.OK
                else 0,
            )
            await self._leave(operation)

    async def restore(
        self,
        expected: KVExpectedLayout,
        *,
        priority: int = 0,
        deadline_s: float | None = None,
    ) -> KVTransfer:
        operation = _Operation(uuid.uuid4().hex, priority)
        deadline = self._config.monotonic() + (
            self._config.default_deadline_s if deadline_s is None else deadline_s
        )
        queue_s = await self._enter(operation, deadline)
        lookup_s = transfer_s = restore_s = 0.0
        result = KVResult.IO_ERROR
        tier = KVTier.REMOTE
        try:
            lookup_started = self._config.monotonic()
            key = self.manifest_key(
                expected.identity, expected.token_start, expected.token_end
            )
            manifest = await self._call_store("get", key, deadline=deadline)
            lookup_s = self._config.monotonic() - lookup_started
            if manifest is None:
                result = KVResult.MISS
                return KVTransfer(result, lookup_s=lookup_s, queue_s=queue_s)
            descriptor, data_key = KVObjectDescriptor.from_manifest(bytes(manifest))
            tier = descriptor.tier
            if descriptor.expired(self._config.clock()):
                result = KVResult.EXPIRED
                return KVTransfer(
                    result, descriptor, lookup_s=lookup_s, queue_s=queue_s
                )
            expected.validate(descriptor)
            if operation.cancelled.is_set():
                raise KVTieringError(KVResult.CANCELLED, "KV restore cancelled")

            transfer_started = self._config.monotonic()
            payload = await self._call_store("get", data_key, deadline=deadline)
            transfer_s = self._config.monotonic() - transfer_started
            if operation.cancelled.is_set():
                raise KVTieringError(KVResult.CANCELLED, "KV restore cancelled")
            restore_started = self._config.monotonic()
            if payload is None or len(payload) != descriptor.byte_length:
                raise KVTieringError(KVResult.CORRUPT, "restored KV length mismatch")
            data = bytes(payload)
            if self.checksum(data) != descriptor.checksum:
                raise KVTieringError(KVResult.CORRUPT, "restored KV checksum mismatch")
            expected.validate(descriptor)
            restore_s = self._config.monotonic() - restore_started
            result = KVResult.OK
            bandwidth = descriptor.byte_length / transfer_s if transfer_s > 0 else 0.0
            return KVTransfer(
                result,
                descriptor,
                data,
                lookup_s,
                queue_s,
                transfer_s,
                restore_s,
                bandwidth,
            )
        except KVTieringError as exc:
            result = exc.result
            if result in (KVResult.CORRUPT, KVResult.INCOMPATIBLE, KVResult.IO_ERROR):
                return KVTransfer(
                    result, lookup_s=lookup_s, queue_s=queue_s, transfer_s=transfer_s
                )
            raise
        finally:
            self.metrics.record(
                "restore",
                result,
                tier,
                lookup_s=lookup_s,
                queue_s=queue_s,
                transfer_s=transfer_s,
                restore_s=restore_s,
                transferred_bytes=expected.byte_length if result is KVResult.OK else 0,
            )
            await self._leave(operation)

    async def prefetch(self, expected: KVExpectedLayout, **kwargs: Any) -> KVTransfer:
        """Fetch and verify bytes; the inference adapter decides GPU placement."""
        return await self.restore(expected, **kwargs)

    async def delete(
        self,
        expected: KVExpectedLayout,
        *,
        force: bool = False,
        deadline_s: float | None = None,
    ) -> KVResult:
        deadline = self._config.monotonic() + (
            self._config.default_deadline_s if deadline_s is None else deadline_s
        )
        key = self.manifest_key(
            expected.identity, expected.token_start, expected.token_end
        )
        manifest = await self._call_store("get", key, deadline=deadline)
        if manifest is None:
            return KVResult.OK
        descriptor, data_key = KVObjectDescriptor.from_manifest(bytes(manifest))
        expected.validate(descriptor)
        # Unpublish first. Readers that already resolved the manifest still verify
        # the bytes; new readers cannot discover a partially deleted object.
        manifest_status = await self._call_store(
            "remove", key, force, deadline=deadline
        )
        if manifest_status not in (0, _OBJECT_NOT_FOUND):
            raise KVTieringError(KVResult.IO_ERROR, "failed to unpublish KV manifest")
        data_status = await self._call_store(
            "remove", data_key, force, deadline=deadline
        )
        if data_status not in (0, _OBJECT_NOT_FOUND):
            raise KVTieringError(KVResult.IO_ERROR, "failed to remove KV data")
        self.metrics.record("delete", KVResult.OK, descriptor.tier)
        return KVResult.OK


__all__ = [
    "KVExpectedLayout",
    "KVIdentity",
    "KVLookup",
    "KVObjectDescriptor",
    "KVResult",
    "KVStoreBackend",
    "KVTier",
    "KVTieringConfig",
    "KVTieringError",
    "KVTieringMetrics",
    "KVTransfer",
    "MooncakeKVTierManager",
]
