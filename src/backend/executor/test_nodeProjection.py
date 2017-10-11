import unittest

from mock import patch

from executor.nodeScan import Scan
from executor.nodeProjection import Projection

class TestProjection(unittest.TestCase):
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
        self._projector = lambda _row: [_row[1]]

    def test_init(self):
        '''
        sets pointer to child node
        sets pointer to predicate
        '''
        instance = Projection(self._projector, self._input)

        self.assertEquals(instance._input, self._input)
        self.assertEquals(instance._projector, self._projector)

    @patch.object(Scan, '__close__')
    def test_next(self, scan_close_method):
        instance = Projection(self._projector, self._input)

        self.assertEquals(
          next(instance),
          ['name']
        )

        self.assertEquals(
          next(instance),
          ['Brian']
        )

        next(instance) # 2
        next(instance) # 3
        next(instance) # 4
        next(instance) # 5
        next(instance) # 6
        next(instance) # 7

        self.assertFalse(scan_close_method.called)
        self.assertEquals(
          next(instance),
          instance.EOF
        )
        self.assertTrue(scan_close_method.called)

    def test_close(self):
        instance = Projection(self._projector, self._input)
        self.assertEquals(instance.__close__(), None)

