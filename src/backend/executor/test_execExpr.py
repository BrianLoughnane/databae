import unittest

from executor.execExpr import (
    execute, parse_and_execute, tree
)
from executor.nodeNestedLoopJoin import NestedLoopJoin

FILE_PATH = 'test_files/sample_movies.csv'
FULL_FILE_PATH = 'test_files/ml-20m/movies.csv'

class TestExecute(unittest.TestCase):
    def setUp(self):
        self._data = [
          ('id', 'name', 'age', 'major'),
          ('1', 'Brian', '30', 'eng'),
          ('2', 'Jason', '33', 'econ'),
          ('3', 'Christie', '28', 'accounting'),
          ('4', 'Gayle', '33', 'edu'),
          ('5', 'Carolyn', '33', 'econ'),
          ('6', 'Michael', '65', 'law'),
          ('7', 'Lori', '62', 'business')
        ]
        self.majors_gpa = [
          ('id', 'major', 'gpa'),
          (3, 'accounting', 3),
          (7, 'business', 4),
          (2, 'econ', 2),
          (4, 'edu', 4.5),
          (1, 'eng', 4),
          (6, 'law', 5),
        ]

        self.expected_joins = [
          (1, 'Brian', 30, 'eng', 1, 'eng', 4),
          (2, 'Jason', 33, 'econ', 2, 'econ', 2),
          (3, 'Christie', 28, 'accounting', 3, 'accounting', 3),
          (4, 'Gayle', 33, 'edu', 4, 'edu', 4.5),
          (5, 'Carolyn', 33, 'econ', 2, 'econ', 2),
          (6, 'Michael', 65, 'law', 6, 'law', 5),
          (7, 'Lori', 62, 'business', 7, 'business', 4)
        ]


    def test_none(self):
        result = parse_and_execute([])
        expected = None
        self.assertEquals(result, expected)

    def test_selection__equals(self):
        result = parse_and_execute([
            ["SELECTION", ["id", "EQUALS", "1"]],
            ["SCAN", self._data],
        ])
        expected = [
          self._data[1],
        ]
        self.assertEquals(result, expected)

    def test_selection__equals__and(self):
        result = parse_and_execute([
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          self._data[2],
          self._data[5],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection(self):
        result = parse_and_execute([
            ["PROJECTION", ["name"]],
            ["SELECTION", [
                "age", "EQUALS", "33",
                "AND",
                "major", "EQUALS", "econ"
            ]],
            ["SCAN", self._data],
        ])
        expected = [
          [self._data[2][1]],
          [self._data[5][1]],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort(self):
        result = parse_and_execute([
            ["SORT", ["name"]],
            ["PROJECTION", ["name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["Carolyn"],
          ["Jason"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["SORT", ["name"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["5", "Carolyn"],
          ["2", "Jason"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["SORT", ["id"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["2", "Jason"],
          ["5", "Carolyn"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__duplicates(self):
        result = parse_and_execute([
            ["PROJECTION", ["age"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["33"],
          ["33"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort__duplicates(self):
        result = parse_and_execute([
            ["SORT", ["age"]],
            ["PROJECTION", ["age"]],
            ["SELECTION", [
              "age", "EQUALS", "33",
              "AND",
              "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["33"],
          ["33"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort__distinct(self):
        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["age"]],
            ["PROJECTION", ["age"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["33"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["major"]],
            ["PROJECTION", ["major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["econ"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["Carolyn"],
          ["Jason"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name", "major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["Carolyn", "econ"],
          ["Jason", "econ"],
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name", "major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ["Carolyn", "econ"],
          ["Jason", "econ"],
        ]
        self.assertEquals(result, expected)

    def test_filescan(self):
        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["PROJECTION", ["movieId", "title"]],
            ["SELECTION", ["movieId", "EQUALS", "33"]],
            ["FILESCAN", [FILE_PATH]],
        ])
        expected = [
          ['33', 'Wings of Courage (1995)'],
        ]
        self.assertEquals(result, expected)

    def test_filescan_sort(self):
        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["SORT", ["genres", "title"]],
            ["PROJECTION", ["movieId", "title", "genres"]],
            ["SELECTION", []],
            ["FILESCAN", [FILE_PATH]],
        ])
        # TODO implement Limit, then come back to this
        # expected = [
          # ['33', 'Wings of Courage (1995)'],
        # ]
        # self.assertEquals(result, expected)

    # def test_nested_loop_join(self):
        # theta = lambda _row: _row[3] == _row[1]
        # result = execute(tree([
            # NestedLoopJoin(theta), [
                # Scan(self._data),
                # Scan(self.majors_gpa),
            # ]
        # ]))
        # self.assertEquals(result, self.expected_joins)

