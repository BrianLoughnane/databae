import unittest
from src.backend.executor.nodeIterator import Iterator

class TestIterator(unittest.TestCase):
    def setUp(self):
        self._list = list(range(2))
        self._input = (ii for ii in self._list)

    def test_init(self):
        instance = Iterator(self._input)
        self.assertEquals(
          instance._input,
          self._input
        )

    def test_next(self):
        instance = Iterator(self._input)
        self.assertEquals(instance.__next__(), None)

    def test_close(self):
        instance = Iterator(self._input)
        self.assertEquals(instance.__close__(), None)
