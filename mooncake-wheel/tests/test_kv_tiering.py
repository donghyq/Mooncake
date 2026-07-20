import asyncio
import hashlib
import threading
import time
import unittest

from mooncake.kv_tiering import (
    KVExpectedLayout,
    KVIdentity,
    KVObjectDescriptor,
    KVResult,
    KVTier,
    KVTieringConfig,
    KVTieringError,
    MooncakeKVTierManager,
)


class MemoryStore:
    """Exercises the adapter contract without replacing Mooncake's data path."""

    def __init__(self):
        self.values = {}
        self.lock = threading.Lock()
        self.block_put = None
        self.block_data_get = None
        self.corrupt_staging_reads = False
        self.fail_manifest_put = False

    def put(self, key, value):
        if self.block_put is not None and "/staging/" in key:
            self.block_put.wait()
        if self.fail_manifest_put and "/manifest/" in key:
            return -1
        with self.lock:
            if key in self.values:
                return -705
            self.values[key] = bytes(value)
        return 0

    def get(self, key):
        if self.block_data_get is not None and "/staging/" in key:
            self.block_data_get.wait()
        with self.lock:
            value = self.values.get(key)
        if self.corrupt_staging_reads and value is not None and "/staging/" in key:
            return b"corrupt"
        return value

    def remove(self, key, force=False):
        del force
        with self.lock:
            if key not in self.values:
                return -704
            del self.values[key]
        return 0


def make_identity(**changes):
    values = {
        "namespace": "tenant-hash/workflow-hash",
        "identity_digest": "a" * 64,
        "model_version": "model-1",
        "tokenizer_version": "tokenizer-1",
        "template_version": "template-1",
        "render_version": "render-1",
        "kv_format_version": "kv-fp16-v1",
        "parallel_layout_version": "tp8-pp1-v1",
    }
    values.update(changes)
    return KVIdentity(**values)


def make_descriptor(payload=b"valid-kv", **changes):
    now = time.time()
    values = {
        "identity": make_identity(),
        "token_start": 0,
        "token_end": 128,
        "byte_length": len(payload),
        "checksum": hashlib.sha256(payload).hexdigest(),
        "producer_epoch": 7,
        "created_at_unix_s": now,
        "ttl_s": 60,
        "lease_until_unix_s": now + 60,
        "tier": KVTier.HOST,
    }
    values.update(changes)
    return KVObjectDescriptor(**values)


def expected(descriptor):
    return KVExpectedLayout(
        descriptor.identity,
        descriptor.token_start,
        descriptor.token_end,
        descriptor.byte_length,
    )


class KVTieringTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = MemoryStore()
        self.manager = MooncakeKVTierManager(
            self.store,
            KVTieringConfig(max_concurrent_io=1, max_queued_io=1, retry_base_s=0),
        )

    async def test_complete_commit_is_visible_and_restorable(self):
        payload = b"complete-kv"
        descriptor = make_descriptor(payload)
        transfer = await self.manager.demote(descriptor, payload)
        self.assertEqual(transfer.result, KVResult.OK)

        lookup = await self.manager.lookup(expected(descriptor))
        restored = await self.manager.restore(expected(descriptor))
        self.assertEqual(lookup.result, KVResult.OK)
        self.assertEqual(restored.result, KVResult.OK)
        self.assertEqual(restored.payload, payload)
        self.assertGreaterEqual(restored.effective_bandwidth_bytes_s, 0)

    async def test_partial_write_is_not_published(self):
        payload = b"partial-kv"
        descriptor = make_descriptor(payload)
        self.store.fail_manifest_put = True
        with self.assertRaises(KVTieringError):
            await self.manager.demote(descriptor, payload)
        lookup = await self.manager.lookup(expected(descriptor))
        self.assertEqual(lookup.result, KVResult.MISS)
        self.assertFalse(any("/staging/" in key for key in self.store.values))

    async def test_staging_checksum_failure_is_not_published(self):
        payload = b"checksum-kv"
        descriptor = make_descriptor(payload)
        self.store.corrupt_staging_reads = True
        with self.assertRaisesRegex(KVTieringError, "length mismatch"):
            await self.manager.demote(descriptor, payload)
        self.assertEqual(
            (await self.manager.lookup(expected(descriptor))).result, KVResult.MISS
        )

    async def test_restore_corruption_falls_back(self):
        payload = b"restore-kv"
        descriptor = make_descriptor(payload)
        await self.manager.demote(descriptor, payload)
        manifest_key = self.manager.manifest_key(
            descriptor.identity, descriptor.token_start, descriptor.token_end
        )
        _, data_key = descriptor.from_manifest(self.store.values[manifest_key])
        self.store.values[data_key] = b"bad"
        restored = await self.manager.restore(expected(descriptor))
        self.assertEqual(restored.result, KVResult.CORRUPT)
        self.assertIsNone(restored.payload)

    async def test_version_mismatch_is_rejected(self):
        payload = b"version-kv"
        descriptor = make_descriptor(payload)
        await self.manager.demote(descriptor, payload)
        incompatible = KVExpectedLayout(
            make_identity(model_version="model-2"), 0, 128, len(payload)
        )
        # The content identity locates the candidate, while exact compatibility
        # fields reject it before any data restore.
        self.assertEqual(
            (await self.manager.restore(incompatible)).result,
            KVResult.INCOMPATIBLE,
        )

        manifest_key = self.manager.manifest_key(
            descriptor.identity, descriptor.token_start, descriptor.token_end
        )
        corrupt_identity = make_descriptor(
            payload, identity=make_identity(model_version="model-2")
        )
        self.store.values[manifest_key] = corrupt_identity.to_manifest("unused")
        result = await self.manager.restore(expected(descriptor))
        self.assertEqual(result.result, KVResult.INCOMPATIBLE)

    async def test_expired_object_is_unpublished(self):
        payload = b"expired-kv"
        now = time.time()
        descriptor = make_descriptor(
            payload,
            created_at_unix_s=now - 10,
            ttl_s=20,
            lease_until_unix_s=now + 10,
        )
        await self.manager.demote(descriptor, payload)
        self.manager._config.clock = lambda: now + 11
        lookup = await self.manager.lookup(expected(descriptor))
        self.assertEqual(lookup.result, KVResult.EXPIRED)
        self.assertEqual(
            (await self.manager.lookup(expected(descriptor))).result, KVResult.MISS
        )

    async def test_delete_is_idempotent(self):
        payload = b"delete-kv"
        descriptor = make_descriptor(payload)
        await self.manager.demote(descriptor, payload)
        self.assertEqual(await self.manager.delete(expected(descriptor)), KVResult.OK)
        self.assertEqual(await self.manager.delete(expected(descriptor)), KVResult.OK)

    async def test_concurrent_duplicate_commit_is_idempotent(self):
        payload = b"duplicate-kv"
        descriptor = make_descriptor(payload)
        first, second = await asyncio.gather(
            self.manager.demote(descriptor, payload),
            self.manager.demote(descriptor, payload),
        )
        self.assertEqual(first.result, KVResult.OK)
        self.assertEqual(second.result, KVResult.OK)
        restored = await self.manager.restore(expected(descriptor))
        self.assertEqual(restored.result, KVResult.OK)
        self.assertEqual(restored.payload, payload)
        self.assertEqual(
            sum("/staging/" in key for key in self.store.values),
            1,
        )

    async def test_conflicting_commit_does_not_replace_manifest(self):
        first_payload = b"first-kv"
        first = make_descriptor(first_payload)
        await self.manager.demote(first, first_payload)

        second_payload = b"other-kv"
        second = make_descriptor(second_payload, producer_epoch=8)
        with self.assertRaisesRegex(KVTieringError, "different KV object"):
            await self.manager.demote(second, second_payload)

        restored = await self.manager.restore(expected(first))
        self.assertEqual(restored.result, KVResult.OK)
        self.assertEqual(restored.payload, first_payload)

    async def test_corrupt_manifest_falls_back(self):
        payload = b"manifest-kv"
        descriptor = make_descriptor(payload)
        manifest_key = self.manager.manifest_key(
            descriptor.identity, descriptor.token_start, descriptor.token_end
        )
        self.store.values[manifest_key] = b"not-json"
        lookup = await self.manager.lookup(expected(descriptor))
        restored = await self.manager.restore(expected(descriptor))
        self.assertEqual(lookup.result, KVResult.CORRUPT)
        self.assertEqual(restored.result, KVResult.CORRUPT)
        self.assertIsNone(restored.payload)

    async def test_quota_rejects_large_object(self):
        manager = MooncakeKVTierManager(
            self.store, KVTieringConfig(max_object_bytes=4, max_namespace_bytes=8)
        )
        payload = b"too-large"
        with self.assertRaisesRegex(KVTieringError, "exceeds quota"):
            await manager.demote(make_descriptor(payload), payload)

    async def test_backpressure_and_low_value_cancel(self):
        self.store.block_put = threading.Event()
        payload = b"blocked-kv"
        first = asyncio.create_task(
            self.manager.demote(make_descriptor(payload), payload, priority=-5)
        )
        await asyncio.sleep(0.02)
        second = asyncio.create_task(
            self.manager.demote(
                make_descriptor(payload, producer_epoch=8), payload, priority=10
            )
        )
        await asyncio.sleep(0.02)
        with self.assertRaisesRegex(KVTieringError, "queue is full"):
            await self.manager.demote(
                make_descriptor(payload, producer_epoch=9), payload, priority=10
            )
        self.assertEqual(await self.manager.cancel_low_value(0), 1)
        self.store.block_put.set()
        with self.assertRaisesRegex(KVTieringError, "cancelled"):
            await first
        await second

    async def test_deadline_while_queued(self):
        self.store.block_put = threading.Event()
        payload = b"deadline-kv"
        first = asyncio.create_task(
            self.manager.demote(make_descriptor(payload), payload)
        )
        await asyncio.sleep(0.02)
        with self.assertRaisesRegex(KVTieringError, "deadline expired"):
            await self.manager.demote(
                make_descriptor(payload, producer_epoch=8), payload, deadline_s=0.01
            )
        self.store.block_put.set()
        await first

    async def test_deadline_during_store_call_does_not_publish(self):
        self.store.block_put = threading.Event()
        payload = b"io-deadline-kv"
        descriptor = make_descriptor(payload)
        task = asyncio.create_task(
            self.manager.demote(descriptor, payload, deadline_s=0.01)
        )
        await asyncio.sleep(0.02)
        self.store.block_put.set()
        with self.assertRaisesRegex(KVTieringError, "after deadline"):
            await task
        self.assertEqual(
            (await self.manager.lookup(expected(descriptor))).result,
            KVResult.MISS,
        )

    async def test_restore_can_be_cancelled_without_returning_payload(self):
        payload = b"cancel-restore-kv"
        descriptor = make_descriptor(payload)
        await self.manager.demote(descriptor, payload)
        self.store.block_data_get = threading.Event()
        task = asyncio.create_task(
            self.manager.restore(expected(descriptor), priority=-5)
        )
        await asyncio.sleep(0.02)
        self.assertEqual(await self.manager.cancel_low_value(0), 1)
        self.store.block_data_get.set()
        with self.assertRaisesRegex(KVTieringError, "restore cancelled"):
            await task

    async def test_namespace_quota_bounds_concurrent_admission(self):
        payload = b"quota-kv"
        store = MemoryStore()
        store.block_put = threading.Event()
        manager = MooncakeKVTierManager(
            store,
            KVTieringConfig(
                max_concurrent_io=2,
                max_queued_io=1,
                max_namespace_bytes=len(payload),
                retry_base_s=0,
            ),
        )
        first = asyncio.create_task(manager.demote(make_descriptor(payload), payload))
        await asyncio.sleep(0.02)
        with self.assertRaisesRegex(KVTieringError, "namespace quota"):
            await manager.demote(make_descriptor(payload, producer_epoch=8), payload)
        store.block_put.set()
        await first

    async def test_metrics_use_bounded_labels(self):
        payload = b"metrics-kv"
        descriptor = make_descriptor(payload)
        await self.manager.demote(descriptor, payload)
        await self.manager.restore(expected(descriptor))
        metrics = self.manager.metrics.snapshot()
        self.assertEqual(metrics["operations"][("demote", "ok", "host")], 1)
        self.assertEqual(metrics["operations"][("restore", "ok", "host")], 1)
        self.assertNotIn(descriptor.identity.namespace, repr(metrics["operations"]))


if __name__ == "__main__":
    unittest.main()
