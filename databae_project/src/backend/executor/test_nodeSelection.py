import unittest

from src.backend.executor.nodeScan import Scan
from src.backend.executor.nodeSelection import Selection

class TestSelection(unittest.TestCase):
    def setUp(self):
        self._data = [
          ('id', 'name', 'age', 'major'),
          (1, 'Brian', 30, 'eng'),
          (2, 'Jason', 33, 'econ'),
          (3, 'Christie', 28, 'accounting'),
          (4, 'Gayle', 33, 'edu'),
          (5, 'Carolyn', 33, 'econ'),
          (6, 'Michael', 65, 'law'),
          (7, 'Lori', 62, 'business')
        ]
        self._input = Scan((ii for ii in self._data))
        self._predicate = lambda _row: _row[3] == 'econ'

    def test_init(self):
        '''
        sets pointer to child node
        sets pointer to predicate
        '''
        instance = Selection(self._input, self._predicate)

        self.assertEquals(instance._input, self._input)
        self.assertEquals(instance._predicate, self._predicate)

    def test_next(self):
        instance = Selection(
          self._input,
          self._predicate,
        )

        first_passing = instance.__next__()
        first_passing__expected = self._data[2]
        self.assertEquals(
            first_passing,
            first_passing__expected
        )

        self.assertEquals(
          instance.__next__(),
          self._data[5]
        )

        self.assertEquals(
          instance.__next__(),
          instance.EOF
        )

    def test_close(self):
        instance = Selection(
          self._input,
          self._predicate
        )
        self.assertEquals(instance.__close__(), None)

