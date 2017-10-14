import unittest

from executor.nodeScan import Scan

FILE_PATH = 'src/backend/executor/'

class TestScan(unittest.TestCase):
    def setUp(self):
        self.values = list(range(2))

    def test_init(self):
        instance = Scan(self.values)
        self.assertEquals(instance.values, self.values)

    def test_next(self):
        instance = Scan(self.values)

        self.assertEquals(
          next(instance),
          self.values[0]
        )

        self.assertEquals(
          next(instance),
          self.values[1]
        )

        self.assertEquals(
          next(instance),
          instance.EOF
        )

    def test_close(self):
        instance = Scan(self.values)
        self.assertEquals(instance.__close__(), None)
