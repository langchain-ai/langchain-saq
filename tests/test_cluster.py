

import unittest


class TestCluster(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        pass

    async def asyncTearDown(self) -> None:
        pass

    async def test_cluster(self) -> None:
        self.assertEqual(2, 2)