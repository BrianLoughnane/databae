import unittest

from executor.execExpr import execute

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
          [self._data[5][1]],
          [self._data[2][1]],
        ]
        self.assertEquals(result, expected)


