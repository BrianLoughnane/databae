import unittest

from executor.nodeScan import Scan

FILE_PATH = 'src/backend/executor/'

class TestScan(unittest.TestCase):
    def setUp(self):
        self._list = list(range(3))
        self._input = (ii for ii in self._list)

    def test_init(self):
        instance = Scan(self._input)
        self.assertEquals(instance._input, self._input)
        self.assertEquals(instance.schema, self._list[0])

    def test_next(self):
        instance = Scan(self._input)
        self.assertEquals(
          instance.__next__(),
          self._list[1]
        )

        self.assertEquals(
          instance.__next__(),
          self._list[2]
        )

        self.assertEquals(
          instance.__next__(),
          instance.EOF
        )

    def test_close(self):
        instance = Scan(self._input)
        self.assertEquals(instance.__close__(), None)
