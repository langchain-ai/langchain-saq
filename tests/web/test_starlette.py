from __future__ import annotations

import typing as t
import unittest

import httpx

from saq.web.starlette import saq_web

from .test_aiohttp import TestAiohttpWeb


@unittest.skip("disable starlette tests, we don't use the web interfaces")
class TestStarletteWeb(TestAiohttpWeb):
    async def get_application(self) -> t.Any:
        return saq_web("", queues=[self.queue1, self.queue2])

    async def shutdown_application(self) -> None:
        pass

    async def get_test_client(self) -> t.Any:
        return httpx.AsyncClient(base_url="http://test")

    def status_code(self, resp: t.Any) -> int:
        return resp.status_code

    async def json(self, resp: t.Any) -> t.Any:
        return resp.json()
