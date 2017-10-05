import unittest
from src.backend.executor.nodeSelection import Selection

class TestSelection(unittest.TestCase):
    def setUp(self):
        self._list = list(range(10))
        self._input = (ii for ii in self._list)
        self._predicate = lambda n: n < 2

    def test_init(self):
        instance = Selection(
          self._input, self._predicate)

        self.assertEquals(
          instance._input,
          self._input
        )

        self.assertEquals(
          instance._predicate,
          self._predicate
        )

    def test_next(self):
        instance = Selection(
          self._input, self._predicate)

        self.assertEquals(
          instance.__next__(),
          self._list[0]
        )

        self.assertEquals(
          instance.__next__(),
          self._list[1]
        )

        self.assertEquals(
          instance.__next__(),
          instance.EOF
        )

    def test_close(self):
        instance = Selection(
          self._input, self._predicate)
        self.assertEquals(instance.__close__(), None)

