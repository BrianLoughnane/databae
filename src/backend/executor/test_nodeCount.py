import unittest

from executor.helpers import Print

from executor.execExpr import tree, execute
from executor.nodeCount import Count
from executor.nodeScan import Scan
from executor.nodeProjection import Projection

class TestCount(unittest.TestCase):
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

    def test_next(self):
        result = list(execute(tree(
            [Count(),
                [Projection(lambda a: a),
                    [Scan(self._data, pop_headers=True)],
                ],
            ]
        )))
        expected = [
            (7,)
        ]
        self.assertEquals(result, expected)

    def test_eof(self):
        main = tree(
            [Count(),
                [Projection(lambda a: a),
                    [Scan([1,2,3], pop_headers=False)],
                ],
            ]
        )
        self.assertEquals(next(main), (3,))
        result = next(main)
        self.assertEquals(result, Count.EOF)
