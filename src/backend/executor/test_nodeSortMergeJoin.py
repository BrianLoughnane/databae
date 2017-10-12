import unittest

from executor.nodeScan import Scan
from executor.nodeSort import Sort
from executor.nodeSortMergeJoin import SortMergeJoin
from executor.nodeFileScan import FileScan

SAMPLE_MOVIES = 'test_files/sample_movies.csv'
SAMPLE_RATINGS = 'test_files/sample_ratings.csv'

class TestSortMergeJoin(unittest.TestCase):
    def setUp(self):
        self.students = [
          ('id', 'name', 'age', 'major'),
          (3, 'Christie', 28, 'accounting'),
          (7, 'Lori', 62, 'business'),
          (2, 'Jason', 33, 'econ'),
          (5, 'Carolyn', 33, 'econ'),
          (4, 'Gayle', 33, 'edu'),
          (1, 'Brian', 30, 'eng'),
          (6, 'Michael', 65, 'law'),
        ]
        self.major_gpas = [
          ('id', 'major', 'gpa'),
          (3, 'accounting', 3),
          (7, 'business', 4),
          (2, 'econ', 2),
          (4, 'edu', 4.5),
          (1, 'eng', 4),
          (6, 'law', 5),
        ]

        self._input1 = Scan([ii for ii in self.students])
        self._input2 = Scan([ii for ii in self.major_gpas])

        self.projection1 = lambda r: r[3]
        self.projection2 = lambda r: r[1]

        # join students with the average gpa for their major
        self.theta = lambda _row1, _row2: _row1[3] == _row2[1]

        self.expected_joins = [
          (3, 'Christie', 28, 'accounting', 3, 'accounting', 3),
          (7, 'Lori', 62, 'business', 7, 'business', 4),
          (2, 'Jason', 33, 'econ', 2, 'econ', 2),
          (5, 'Carolyn', 33, 'econ', 2, 'econ', 2),
          (4, 'Gayle', 33, 'edu', 4, 'edu', 4.5),
          (1, 'Brian', 30, 'eng', 1, 'eng', 4),
          (6, 'Michael', 65, 'law', 6, 'law', 5),
        ]

    def test_next(self):
        # pop off headers
        next(self._input1)
        next(self._input2)

        # some simple cases
        instance = SortMergeJoin(
            self.theta,
            self.projection1, self.projection2,
            self._input1, self._input2)
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
        instance = SortMergeJoin(
            self.theta,
            self.projection1, self.projection2,
            self._input1, self._input2)

        for expected in self.expected_joins:
            result = next(instance)
            self.assertEquals(
                result,
                expected,
            )

    def test__filescan(self):
        def projection(index):
            def inner(row):
                try:
                    return int(row[index])
                except:
                    return row[index]
            return inner
        # self.projection1 = lambda _row: int(_row[0])
        # self.projection2 = lambda _row: float(_row[1])

        self._input1 = Sort(
            projection(0),
            FileScan(SAMPLE_MOVIES)
        )
        self._input2 = Sort(
            projection(1),
            FileScan(SAMPLE_RATINGS)
        )

        self.theta = lambda _row1, _row2: _row1[0] == _row2[1]

        # pop off headers
        next(self._input1)
        next(self._input1)
        next(self._input2)

        instance = SortMergeJoin(
            self.theta,
            self.projection1, self.projection2,
            self._input1, self._input2)

        result = next(instance)

        expected = [
            '2', 'Jumanji (1995)',
                'Adventure|Children|Fantasy',
            '1', '2', '3.5', '1112486027'
        ]

        self.assertEquals(result, expected)

