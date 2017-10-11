import unittest

from executor.execExpr import execute

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

    def test_none(self):
        result = execute([])
        expected = None
        self.assertEquals(result, expected)

    def test_selection__equals(self):
        result = execute([
            ["SELECTION", ["id", "EQUALS", "1"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          self._data[1],
        ]
        self.assertEquals(result, expected)

    def test_selection__equals__and(self):
        result = execute([
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          self._data[2],
          self._data[5],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection(self):
        result = execute([
            ["PROJECTION", ["name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          [self._data[2][1]],
          [self._data[5][1]],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort(self):
        result = execute([
            ["SORT", ["name"]],
            ["PROJECTION", ["name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["Carolyn"],
          ["Jason"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["SORT", ["name"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["5", "Carolyn"],
          ["2", "Jason"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["SORT", ["id"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["2", "Jason"],
          ["5", "Carolyn"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__duplicates(self):
        result = execute([
            ["PROJECTION", ["age"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["33"],
          ["33"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort__duplicates(self):
        result = execute([
            ["SORT", ["age"]],
            ["PROJECTION", ["age"]],
            ["SELECTION", [
              "age", "EQUALS", "33",
              "AND",
              "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["33"],
          ["33"],
        ]
        self.assertEquals(result, expected)

    def test_selection__projection__sort__distinct(self):
        result = execute([
            ["DISTINCT", [""]],
            ["SORT", ["age"]],
            ["PROJECTION", ["age"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["33"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["DISTINCT", [""]],
            ["SORT", ["major"]],
            ["PROJECTION", ["major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["econ"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["Carolyn"],
          ["Jason"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name", "major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["Carolyn", "econ"],
          ["Jason", "econ"],
        ]
        self.assertEquals(result, expected)

        result = execute([
            ["DISTINCT", [""]],
            ["SORT", ["name"]],
            ["PROJECTION", ["name", "major"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", [(ii for ii in self._data)]],
        ])
        expected = [
          ["Carolyn", "econ"],
          ["Jason", "econ"],
        ]
        self.assertEquals(result, expected)

    def test_filescan(self):
        result = execute([
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
        result = execute([
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

