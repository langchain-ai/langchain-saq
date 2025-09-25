import typing as t
from typing import cast

from redis.asyncio.cluster import RedisCluster
from redis.asyncio.client import Redis

from saq.queue import Queue


def create_queue(**kwargs: t.Any) -> Queue:
    return Queue.from_url("redis://localhost:6379", **kwargs)


def create_cluster_queue(name: str = "default", **kwargs: t.Any) -> Queue:
    redis = cast(Redis, RedisCluster.from_url("redis://localhost:30001"))
    return Queue(
        redis,
        is_cluster=True,
        name=name,
        **kwargs,
    )


async def cleanup_queue(queue: Queue) -> None:
    await queue.redis.flushdb()
    await queue.disconnect()
