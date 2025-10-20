"""
Cluster-specific tests that verify RedisCluster behavior.
These tests check things like hash tag generation, key slot consistency, etc.
"""

from __future__ import annotations

import asyncio
import typing as t
import unittest

from saq.job import Status
from tests.helpers import cleanup_queue, create_cluster_queue

if t.TYPE_CHECKING:
    from saq.queue import Queue


class TestClusterSpecific(unittest.IsolatedAsyncioTestCase):
    """Tests specifically for Redis Cluster functionality."""

    def setUp(self) -> None:
        self.queue = create_cluster_queue()

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)

    def test_is_cluster_flag(self) -> None:
        """Verify the is_cluster flag is set correctly."""
        self.assertTrue(self.queue.is_cluster)

    def test_from_url_cluster(self) -> None:
        """Verify Queue.from_url works with is_cluster=True."""
        from saq.queue import Queue

        queue = Queue.from_url(
            "redis://redis-30001:30001", is_cluster=True, name="test"
        )
        self.assertTrue(queue.is_cluster)
        self.assertIsInstance(queue.redis, type(self.queue.redis))
        # Cleanup without processing
        asyncio.get_event_loop().run_until_complete(queue.disconnect())

    def test_hash_tag_in_job_id(self) -> None:
        """Verify job IDs contain hash tags in cluster mode."""
        job_id = self.queue.job_id("test-key")
        # Should contain {queue_name} for hash tag
        self.assertIn(f"{{{self.queue.name}}}", job_id)
        self.assertEqual(job_id, f"saq:job:{{{self.queue.name}}}:test-key")

    def test_hash_tag_in_namespace(self) -> None:
        """Verify namespace keys contain hash tags in cluster mode."""
        namespace = self.queue.namespace("test")
        # Should contain {queue_name} for hash tag
        self.assertIn(f"{{{self.queue.name}}}", namespace)
        self.assertEqual(namespace, f"saq:{{{self.queue.name}}}:test")

    def test_hash_tag_in_abort_key(self) -> None:
        """Verify abort keys contain hash tags in cluster mode."""
        abort_key = self.queue.abort_key("test-key")
        # Should contain {queue_name} for hash tag
        self.assertIn(f"{{{self.queue.name}}}", abort_key)
        self.assertEqual(abort_key, f"saq:abort:{{{self.queue.name}}}:test-key")

    def test_keys_same_slot(self) -> None:
        """Verify all queue-related keys land on the same hash slot."""
        # Get hash slots for various keys
        job_key = "test-job"
        job_id = self.queue.job_id(job_key)
        abort_key = self.queue.abort_key(job_key)
        incomplete = self.queue.namespace("incomplete")
        queued = self.queue.namespace("queued")
        active = self.queue.namespace("active")

        # In Redis Cluster, keys with the same hash tag should have the same slot
        # We can't easily test the actual slot without diving into redis-py internals,
        # but we can verify the hash tag is present and consistent. This ensures that all keys for a given queue are on the same slot.
        hash_tag = f"{{{self.queue.name}}}"

        self.assertIn(hash_tag, job_id)
        self.assertIn(hash_tag, abort_key)
        self.assertIn(hash_tag, incomplete)
        self.assertIn(hash_tag, queued)
        self.assertIn(hash_tag, active)

    def test_pubsub_disabled(self) -> None:
        """Verify PubSub is disabled in cluster mode."""
        self.assertTrue(self.queue.is_cluster)
        self.assertIsNone(self.queue._pubsub)  # pylint: disable=protected-access

    # We no-op pubsub in LangSmith services, hence this test.
    async def test_listen_notify_noop_in_cluster(self) -> None:
        """Verify listen() and notify() are no-ops in cluster mode."""
        job = await self.queue.enqueue("test")
        assert job is not None

        called = False

        def callback(_job_key: str, _status: Status) -> bool:
            nonlocal called
            called = True
            return True

        # listen should return immediately without calling the callback
        await self.queue.listen([job.key], callback, timeout=0.1)
        # notify should also be a no-op
        await self.queue.notify(job)
        self.assertFalse(called)

    async def test_pipeline_no_transaction(self) -> None:
        """Verify pipelines don't use transactions in cluster mode."""
        # This is more of an integration test - if transactions were used,
        # operations across different slots would fail
        job1 = await self.queue.enqueue("test1")
        job2 = await self.queue.enqueue("test2")
        assert job1 is not None
        assert job2 is not None

        # These operations should succeed even though they're in a pipeline
        await self.queue.finish(job1, Status.COMPLETE, result=1)
        await self.queue.finish(job2, Status.COMPLETE, result=2)

        # Verify both finished
        refreshed1 = await self.queue.job(job1.key)
        refreshed2 = await self.queue.job(job2.key)
        assert refreshed1 is not None
        assert refreshed2 is not None
        self.assertEqual(refreshed1.status, Status.COMPLETE)
        self.assertEqual(refreshed2.status, Status.COMPLETE)

    async def test_multiple_jobs_mget(self) -> None:
        """Verify mget works correctly with multiple jobs in cluster mode."""
        # Enqueue several jobs
        jobs = []
        for i in range(5):
            job = await self.queue.enqueue(f"test{i}")
            assert job is not None
            jobs.append(job)

        # Get info with jobs enabled - this uses mget internally
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["queued"], 5)
        self.assertEqual(len(info["jobs"]), 5)

    async def test_scripts_work_in_cluster(self) -> None:
        """Verify Lua scripts work correctly in cluster mode."""
        # Test enqueue script
        job1 = await self.queue.enqueue("test1")
        assert job1 is not None
        self.assertEqual(await self.queue.count("queued"), 1)

        # Test schedule script
        import time

        scheduled_time = time.time() + 10
        job2 = await self.queue.enqueue("test2", scheduled=scheduled_time)
        assert job2 is not None
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 2)

        # Test cleanup/sweep script (though it might not find anything to sweep)
        swept = await self.queue.sweep()
        self.assertIsInstance(swept, list)

    async def test_abort_in_cluster(self) -> None:
        """Verify abort functionality works in cluster mode."""
        # Test aborting a queued job
        job = await self.queue.enqueue("test")
        assert job is not None

        await self.queue.abort(job, "test abort")
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)

        # Test aborting an active job - abort key should persist
        job2 = await self.queue.enqueue("test")
        assert job2 is not None
        await self.queue.dequeue()

        await self.queue.abort(job2, "test abort active")
        self.assertEqual(await self.queue.count("active"), 0)
        # Verify abort key was set for active job
        abort_value = await self.queue.redis.get(self.queue.abort_key(job2.key))
        self.assertEqual(abort_value, b"test abort active")

    async def test_stats_in_cluster(self) -> None:
        """Verify stats work correctly in cluster mode."""
        stats = await self.queue.stats()
        self.assertIn("complete", stats)
        self.assertIn("failed", stats)
        self.assertIn("retried", stats)
        self.assertIn("aborted", stats)
        self.assertIn("uptime", stats)

    async def test_multiple_queues_different_slots(self) -> None:
        """Verify different queues can coexist (they'll be on different slots)."""
        queue1 = create_cluster_queue(name="queue1")
        queue2 = create_cluster_queue(name="queue2")

        self.addAsyncCleanup(cleanup_queue, queue1)
        self.addAsyncCleanup(cleanup_queue, queue2)

        # Enqueue to both queues
        job1 = await queue1.enqueue("test1")
        job2 = await queue2.enqueue("test2")

        assert job1 is not None
        assert job2 is not None

        # Verify they're independent
        self.assertEqual(await queue1.count("queued"), 1)
        self.assertEqual(await queue2.count("queued"), 1)

        # Different hash tags
        self.assertIn("{queue1}", job1.id)
        self.assertIn("{queue2}", job2.id)

    async def test_empty_mget_safe(self) -> None:
        """Verify empty mget calls don't fail in cluster mode."""
        # This tests the guard in info() when there are no worker stats
        info = await self.queue.info(jobs=False)
        self.assertEqual(info["workers"], {})

    async def test_retry_in_cluster(self) -> None:
        """Verify retry functionality works in cluster mode."""
        job = await self.queue.enqueue("test", retries=2)
        assert job is not None

        dequeued = await self.queue.dequeue()
        assert dequeued is not None

        await self.queue.retry(dequeued, "test error")
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(self.queue.retried, 1)

    async def test_batch_context_manager_in_cluster(self) -> None:
        """Verify batch context manager works in cluster mode."""
        try:
            async with self.queue.batch():
                job = await self.queue.enqueue("test")
                assert job is not None
                raise ValueError("test error")
        except ValueError:
            pass

        # Job should be aborted
        if job is None:
            self.fail("Job is None")
        refreshed = await self.queue.job(job.key)
        assert refreshed is not None
        self.assertEqual(refreshed.status, Status.ABORTED)


class TestClusterMultiQueue(unittest.IsolatedAsyncioTestCase):
    """Test scenarios with multiple queues in cluster mode."""

    async def asyncSetUp(self) -> None:
        self.queue1 = create_cluster_queue(name="queue1")
        self.queue2 = create_cluster_queue(name="queue2")

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)

    async def test_isolated_operations(self) -> None:
        """Verify operations on one queue don't affect another."""
        # Enqueue jobs
        job1a = await self.queue1.enqueue("test1a")
        job1b = await self.queue1.enqueue("test1b")
        job2a = await self.queue2.enqueue("test2a")

        assert job1a is not None
        assert job1b is not None
        assert job2a is not None

        # Check counts
        self.assertEqual(await self.queue1.count("queued"), 2)
        self.assertEqual(await self.queue2.count("queued"), 1)

        # Abort from queue1
        await self.queue1.abort(job1a, "abort")

        # Should only affect queue1
        self.assertEqual(await self.queue1.count("queued"), 1)
        self.assertEqual(await self.queue2.count("queued"), 1)

    async def test_same_key_different_queues(self) -> None:
        """Verify same job key in different queues creates different jobs."""
        same_key = "identical-key"

        job1 = await self.queue1.enqueue("test", key=same_key)
        job2 = await self.queue2.enqueue("test", key=same_key)

        assert job1 is not None
        assert job2 is not None

        # Different job IDs due to different queue names in hash tag
        self.assertNotEqual(job1.id, job2.id)
        self.assertIn("{queue1}", job1.id)
        self.assertIn("{queue2}", job2.id)

        # Both should exist
        self.assertEqual(await self.queue1.count("queued"), 1)
        self.assertEqual(await self.queue2.count("queued"), 1)
