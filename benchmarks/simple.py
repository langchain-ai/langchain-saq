import asyncio
import sys
import time

from funcs import *


SEM = asyncio.Semaphore(20)
N = 10_000


async def sem_task(task):
    async with SEM:
        return await task


async def bench_saq(cluster: bool = False):
    from saq import Queue, Worker

    async def enqueue(func, count):
        await asyncio.gather(
            *[asyncio.create_task(queue.enqueue(func)) for _ in range(count)]
        )

    if cluster:
        queue = Queue.from_url("redis://localhost:30001", is_cluster=True)
    else:
        queue = Queue.from_url("redis://localhost:6379")
    worker = Worker(queue=queue, functions=[noop, sleeper], concurrency=10)

    now = time.time()
    await enqueue("noop", N)
    print(f"SAQ enqueue {N} {time.time() - now}")

    now = time.time()
    task = asyncio.create_task(worker.start())

    while await queue.count("incomplete"):
        await asyncio.sleep(0.1)
    print(f"SAQ process {N} noop {time.time() - now}")

    await enqueue("sleeper", 1000)
    now = time.time()
    while await queue.count("incomplete"):
        await asyncio.sleep(0.1)
    print(f"SAQ process 1000 sleep {time.time() - now}")

async def main():
    print("=" * 60)
    print("Running SAQ Benchmark (Regular Mode)")
    print("=" * 60)
    await bench_saq(cluster=False)
    print()

    print("=" * 60)
    print("Running SAQ Benchmark (Cluster Mode)")
    print("=" * 60)
    try:
        await bench_saq(cluster=True)
    except Exception as e:
        print(f"SAQ Cluster mode failed: {e}")
        print("Make sure Redis cluster is running (see redis-docker-compose.yaml)")
    print()


if __name__ == "__main__":
    asyncio.run(main())
