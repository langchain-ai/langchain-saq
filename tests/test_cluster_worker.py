"""
Cluster-specific worker tests.
Tests worker functionality that may behave differently in cluster mode.
"""

from __future__ import annotations

import asyncio
import typing as t
import unittest

from saq.job import Job, Status
from saq.worker import Worker
from tests.helpers import cleanup_queue, create_cluster_queue

if t.TYPE_CHECKING:
    from saq.types import Context, Function


async def simple_task(_ctx: Context, *, value: int) -> int:
    return value * 2


async def sleeper(_ctx: Context, *, duration: float = 0.1) -> str:
    await asyncio.sleep(duration)
    return "done"


async def error_task(_ctx: Context) -> t.NoReturn:
    raise ValueError("intentional error")


functions: list[Function] = [simple_task, sleeper, error_task]


class TestClusterWorker(unittest.IsolatedAsyncioTestCase):
    """Test worker operations in cluster mode."""

    def setUp(self) -> None:
        self.queue = create_cluster_queue()
        self.worker = Worker(self.queue, functions=functions, dequeue_timeout=0.01)

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)

    async def enqueue(self, function: str, **kwargs: t.Any) -> Job:
        job = await self.queue.enqueue(function, **kwargs)
        assert job is not None
        return job

    async def test_process_single_job(self) -> None:
        """Verify worker can process a single job in cluster mode."""
        job = await self.enqueue("simple_task", value=5)
        await self.worker.process()

        await job.refresh()
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, 10)

    async def test_process_multiple_jobs(self) -> None:
        """Verify worker can process multiple jobs in cluster mode."""
        job1 = await self.enqueue("simple_task", value=1)
        job2 = await self.enqueue("simple_task", value=2)
        job3 = await self.enqueue("simple_task", value=3)

        # Process all jobs
        await self.worker.process()
        await self.worker.process()
        await self.worker.process()

        # Verify all completed
        await job1.refresh()
        await job2.refresh()
        await job3.refresh()

        self.assertEqual(job1.status, Status.COMPLETE)
        self.assertEqual(job2.status, Status.COMPLETE)
        self.assertEqual(job3.status, Status.COMPLETE)
        self.assertEqual(job1.result, 2)
        self.assertEqual(job2.result, 4)
        self.assertEqual(job3.result, 6)

    async def test_error_handling_with_retry(self) -> None:
        """Verify error handling and retry works in cluster mode."""
        job = await self.enqueue("error_task", retries=2)

        # First attempt - should retry
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)
        self.assertEqual(job.attempts, 1)

        # Second attempt - should fail
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.FAILED)
        self.assertEqual(job.attempts, 2)
        self.assertIn("ValueError", job.error or "")

    async def test_abort_mechanism(self) -> None:
        """Verify abort mechanism works in cluster mode."""
        job = await self.enqueue("sleeper", duration=10)

        # Start processing in background
        task = asyncio.create_task(self.worker.process())
        await asyncio.sleep(0.05)  # Let it start

        # Abort the job
        await job.abort("manual abort")

        # Worker should detect and abort
        await self.worker.abort(0.001)
        await asyncio.sleep(0.05)

        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
        self.assertEqual(job.error, "manual abort")

        # Cleanup
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_stats_update(self) -> None:
        """Verify stats updates work in cluster mode."""
        job1 = await self.enqueue("simple_task", value=1)
        job2 = await self.enqueue("error_task", retries=1)

        await self.worker.process()  # Complete job1
        await self.worker.process()  # Fail job2

        await job1.refresh()
        await job2.refresh()

        self.assertEqual(job1.status, Status.COMPLETE)
        self.assertEqual(job2.status, Status.FAILED)

        # Check stats
        stats = await self.queue.stats()
        self.assertEqual(stats["complete"], 1)
        self.assertEqual(stats["failed"], 1)

    async def test_concurrent_workers(self) -> None:
        """Verify multiple workers can process jobs concurrently in cluster mode."""
        worker2 = Worker(self.queue, functions=functions, dequeue_timeout=0.01)

        # Enqueue multiple jobs
        jobs = [await self.enqueue("simple_task", value=i) for i in range(5)]

        # Process with multiple workers concurrently
        await asyncio.gather(
            self.worker.process(),
            self.worker.process(),
            worker2.process(),
            worker2.process(),
        )

        # All should be processed (some might still be queued)
        completed = 0
        for job in jobs:
            await job.refresh()
            if job.status == Status.COMPLETE:
                completed += 1

        # At least some should be completed
        self.assertGreater(completed, 0)

    async def test_job_with_scheduled_time(self) -> None:
        """Verify scheduled jobs work in cluster mode."""
        import time

        future_time = time.time() + 100

        job = await self.enqueue("simple_task", value=5, scheduled=future_time)

        # Should not be in queued list yet
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)

        # Process should not find it
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)  # Still queued, not active

    async def test_retry_with_delay(self) -> None:
        """Verify retry delay works in cluster mode."""
        import time

        job = await self.enqueue("error_task", retries=3, retry_delay=100.0)

        # First attempt
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        # Should be scheduled in the future
        scheduled_time = await self.queue.redis.zscore(
            self.queue.namespace("incomplete"), job.id
        )
        if scheduled_time is None:
            self.fail("Scheduled time is None")
        self.assertTrue(scheduled_time > time.time() + 50)  # At least 50s in future


class TestClusterWorkerMultiQueue(unittest.IsolatedAsyncioTestCase):
    """Test worker scenarios with multiple queues in cluster mode."""

    async def asyncSetUp(self) -> None:
        self.queue1 = create_cluster_queue(name="worker_queue1")
        self.queue2 = create_cluster_queue(name="worker_queue2")
        self.worker1 = Worker(self.queue1, functions=functions, dequeue_timeout=0.01)
        self.worker2 = Worker(self.queue2, functions=functions, dequeue_timeout=0.01)

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)

    async def test_workers_process_own_queues(self) -> None:
        """Verify workers only process jobs from their own queues."""
        job1 = await self.queue1.enqueue("simple_task", value=1)
        job2 = await self.queue2.enqueue("simple_task", value=2)
        assert job1 is not None
        assert job2 is not None

        # Worker1 should only process from queue1
        await self.worker1.process()

        await job1.refresh()
        await job2.refresh()

        self.assertEqual(job1.status, Status.COMPLETE)
        self.assertEqual(job2.status, Status.QUEUED)  # Still queued

        # Worker2 processes from queue2
        await self.worker2.process()
        await job2.refresh()
        self.assertEqual(job2.status, Status.COMPLETE)

    async def test_stats_isolated_per_queue(self) -> None:
        """Verify stats are isolated per queue in cluster mode."""
        # Process jobs on different queues
        job1 = await self.queue1.enqueue("simple_task", value=1)
        job2 = await self.queue2.enqueue("error_task", retries=1)
        assert job1 is not None
        assert job2 is not None

        await self.worker1.process()
        await self.worker2.process()

        stats1 = await self.queue1.stats()
        stats2 = await self.queue2.stats()

        # Queue1 should have 1 complete, 0 failed
        self.assertEqual(stats1["complete"], 1)
        self.assertEqual(stats1["failed"], 0)

        # Queue2 should have 0 complete, 1 failed
        self.assertEqual(stats2["complete"], 0)
        self.assertEqual(stats2["failed"], 1)
