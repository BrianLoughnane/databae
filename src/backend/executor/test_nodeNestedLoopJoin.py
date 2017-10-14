import unittest

from executor.nodeNestedLoopJoin import NestedLoopJoin
from executor.nodeScan import Scan
from executor.nodeFileScan import FileScan

SAMPLE_MOVIES = 'test_files/sample_movies.csv'
SAMPLE_RATINGS = 'test_files/sample_ratings.csv'

class TestNestedLoopJoin(unittest.TestCase):
    def setUp(self):
        self.students = [
          ('id', 'name', 'age', 'major'),
          (1, 'Brian', 30, 'eng'),
          (2, 'Jason', 33, 'econ'),
          (3, 'Christie', 28, 'accounting'),
          (4, 'Gayle', 33, 'edu'),
          (5, 'Carolyn', 33, 'econ'),
          (6, 'Michael', 65, 'law'),
          (7, 'Lori', 62, 'business')
        ]
        self.major_gpas = [
          ('id', 'major', 'gpa'),
          (1, 'eng', 4),
          (2, 'econ', 2),
          (3, 'accounting', 3),
          (4, 'edu', 4.5),
          (6, 'law', 5),
          (7, 'business', 4)
        ]
        self._input1 = Scan([ii for ii in self.students])
        self._input2 = Scan([ii for ii in self.major_gpas])

        # join students with the average gpa for their major
        self.theta = lambda _row1, _row2: _row1[3] == _row2[1]

        self.expected_joins = [
          (1, 'Brian', 30, 'eng', 1, 'eng', 4),
          (2, 'Jason', 33, 'econ', 2, 'econ', 2),
          (3, 'Christie', 28, 'accounting', 3, 'accounting', 3),
          (4, 'Gayle', 33, 'edu', 4, 'edu', 4.5),
          (5, 'Carolyn', 33, 'econ', 2, 'econ', 2),
          (6, 'Michael', 65, 'law', 6, 'law', 5),
          (7, 'Lori', 62, 'business', 7, 'business', 4)
        ]

    def test_next(self):
        # pop off headers
        next(self._input1)
        next(self._input2)

        # some simple cases
        instance = NestedLoopJoin(self.theta)
        instance._inputs = (self._input1, self._input2)

        self.assertEquals(
            next(instance),
            self.expected_joins[0]
        )
        self.assertEquals(
            next(instance),
            self.expected_joins[1]
        )

    def test_next__all(self):
        # pop off headers
        next(self._input1)
        next(self._input2)

        # all cases (checks for proper EOF handling)
        instance = NestedLoopJoin(self.theta)
        instance._inputs = (self._input1, self._input2)

        for expected in self.expected_joins:
            self.assertEquals(
                next(instance),
                expected,
            )

    def test__filescan(self):
        _input1 = FileScan(SAMPLE_MOVIES)
        _input2 = FileScan(SAMPLE_RATINGS)
        self.theta = lambda _row1, _row2: _row1[0] == _row2[1]

        # pop off headers
        next(_input1)
        next(_input2)

        instance = NestedLoopJoin(self.theta)
        instance._inputs = (_input1, _input2)

        result = next(instance)
        expected = (
            '2', 'Jumanji (1995)',
                'Adventure|Children|Fantasy',
            '1', '2', '3.5', '1112486027'
        )
        self.assertEquals(result, expected)

