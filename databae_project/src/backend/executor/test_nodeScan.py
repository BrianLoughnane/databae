import unittest
from src.backend.executor.nodeScan import Scan

EOF = 'end of fun'

class TestScan(unittest.TestCase):
    def setUp(self):
        self._list = list(range(2))
        self._input = (ii for ii in self._list)

    def test_constructor(self):
        instance = Scan(self._input)
        self.assertEquals(
          instance._input,
          self._input
        )

    def test_next(self):
        instance = Scan(self._input)
        self.assertEquals(
          instance.next(),
          self._list[0]
        )

        self.assertEquals(
          instance.next(),
          self._list[1]
        )

        self.assertEquals(
          instance.next(),
          EOF
        )

    # def test_init(self):
        # self.assertEquals(
          # Scan(self._input).init(),
          # None
        # )

    # def test_close(self):
        # self.assertEquals(Scan().close(), None)
