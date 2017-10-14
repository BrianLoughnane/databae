import unittest

from mock import patch

from executor.nodeScan import Scan
from executor.nodeSelection import Selection

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
        self._input = Scan([ii for ii in self._data])
        self._predicate = lambda _row: _row[3] == 'econ'

    def test_init(self):
        '''
        sets pointer to child node
        sets pointer to predicate
        '''
        instance = Selection(self._predicate)
        instance._inputs = [self._input]

        self.assertEquals(instance._predicate, self._predicate)

    @patch.object(Scan, '__close__')
    def test_next(self, scan_close_method):
        instance = Selection(self._predicate)
        instance._inputs = [self._input]

        self.assertEquals(
            next(instance),
            self._data[2]
        )

        self.assertEquals(
            next(instance),
            self._data[5]
        )

        self.assertFalse(scan_close_method.called)

        self.assertEquals(
            next(instance),
            instance.EOF
        )

        self.assertTrue(scan_close_method.called)

    def test_close(self):
        instance = Selection(self._predicate)

        self.assertEquals(instance.__close__(), None)

