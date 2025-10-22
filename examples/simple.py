import asyncio
import random
import time

from saq import CronJob, Queue


async def sleeper(ctx, *, a):
    await asyncio.sleep(a)
    return {"a": a}


async def adder(ctx, *, a, b):
    await asyncio.sleep(1)
    return a + b


async def cron_job(ctx):
    print("executing cron job")


settings = {
    "functions": [sleeper, adder],
    "concurrency": 100,
    "cron_jobs": [CronJob(cron_job, cron="* * * * * */5")],
}


async def enqueue(func, **kwargs):
    queue = Queue.from_url("redis://localhost")
    for _ in range(10000):
        await queue.enqueue(func, **{k: v() for k, v in kwargs.items()})


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue("sleeper", a=random.random))
    asyncio.run(enqueue("adder", a=random.random, b=random.random))
    print(time.time() - now)
