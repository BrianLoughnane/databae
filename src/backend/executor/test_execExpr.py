import unittest

from executor.execExpr import (
    execute, parse_and_execute, tree
)
from executor.helpers import (
    Debug, Print
)

import operators

from executor.nodeBPlusTree import BPlusTree
from executor.nodeCount import Count
from executor.nodeIndexScan import IndexScan
from executor.nodeScan import Scan
from executor.nodeSelection import Selection
from executor.nodeSort import Sort
from executor.nodeProjection import Projection
from executor.nodeNestedLoopJoin import NestedLoopJoin
from executor.nodeHashJoin import HashJoin
from executor.nodeSortMergeJoin import SortMergeJoin

FILE_PATH = 'test_files/sample_movies.csv'
FULL_FILE_PATH = 'test_files/ml-20m/movies.csv'

class TestExecute(unittest.TestCase):
    '''
    to print out an input stream, add this above it:
      ["PRINT", []],
    to debub an input stream, add this above it:
      ["DEBUG", []],
    '''
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
          ('3', 'accounting', '3'),
          ('7', 'business', '4'),
          ('2', 'econ', '2'),
          ('4', 'edu', '4.5'),
          ('1', 'eng', '4'),
          ('6', 'law', '5'),
        ]

    def test_tree__one(self):
        scan_node = Scan(self._data)
        result = tree([scan_node])
        expected = scan_node
        self.assertEquals(result, expected)

        result = scan_node._inputs
        expected = []
        self.assertEquals(result, expected)

    def test_tree__two(self):
        selection_node = Selection(lambda a: a)
        scan_node = Scan(self._data)

        result = tree([
            selection_node, [
                scan_node
            ]
        ])
        expected = selection_node
        self.assertEquals(result, expected)

        self.assertIn(
            scan_node,
            selection_node._inputs,
        )
        self.assertEquals(
            len(selection_node._inputs),
            1
        )

    def test_tree__nested(self):
        projection_node = Projection(lambda a: a)
        selection_node = Selection(lambda a: a)
        scan_node = Scan([1,2,3])

        result = tree(
            [projection_node,
                [selection_node,
                    [scan_node]
                ]
            ]
        )

        self.assertEquals(result, projection_node)

        self.assertIn(
            selection_node,
            projection_node._inputs,
        )
        self.assertEquals(
            len(projection_node._inputs),
            1
        )

        self.assertIn(
            scan_node,
            selection_node._inputs,
        )
        self.assertEquals(
            len(selection_node._inputs),
            1
        )

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
          ("Carolyn",),
          ("Jason",),
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["SORT", ["name"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ("5", "Carolyn",),
          ("2", "Jason",),
        ]
        self.assertEquals(result, expected)

        result = parse_and_execute([
            ["SORT", ["id"]],
            ["PROJECTION", ["id", "name"]],
            ["SELECTION", ["age", "EQUALS", "33", "AND", "major", "EQUALS", "econ"]],
            ["SCAN", self._data],
        ])
        expected = [
          ("2", "Jason",),
          ("5", "Carolyn",),
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
          ("33",),
          ("33",),
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
          ("33",),
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
          ("econ",),
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
          ("Carolyn",),
          ("Jason",),
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
          ("Carolyn", "econ",),
          ("Jason", "econ",),
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
          ("Carolyn", "econ",),
          ("Jason", "econ",),
        ]
        self.assertEquals(result, expected)

    def test_filescan(self):
        result = parse_and_execute([
            ["DISTINCT", [""]],
            ["PROJECTION", ["movieId", "title"]],
            ["SELECTION", ["movieId", "EQUALS", "33"]],
            ["FILESCAN", FILE_PATH],
        ])
        expected = [
          ['33', 'Wings of Courage (1995)'],
        ]
        self.assertEquals(result, expected)

    # def test_filescan_sort(self):
        # result = parse_and_execute([
            # ["DISTINCT", [""]],
            # ["SORT", ["genres", "title"]],
            # ["PROJECTION", ["movieId", "title", "genres"]],
            # ["SELECTION", []],
            # ["FILESCAN", FILE_PATH],
        # ])
        # TODO implement Limit, then come back to this
        # expected = [
          # ['33', 'Wings of Courage (1995)'],
        # ]
        # self.assertEquals(result, expected)

    def test_nested_loop_join(self):
        expected_joins = [
          ('1', 'Brian', '30', 'eng', '1', 'eng', '4'),
          ('2', 'Jason', '33', 'econ', '2', 'econ', '2'),
          ('3', 'Christie', '28', 'accounting', '3', 'accounting', '3'),
          ('4', 'Gayle', '33', 'edu', '4', 'edu', '4.5'),
          ('5', 'Carolyn', '33', 'econ', '2', 'econ', '2'),
          ('6', 'Michael', '65', 'law', '6', 'law', '5'),
          ('7', 'Lori', '62', 'business', '7', 'business', '4')
        ]

        theta = lambda _row1, _row2: _row1[3] == _row2[1]

        result = list(execute(tree(
            [NestedLoopJoin(theta),
                [Scan(self._data, pop_headers=True)],
                [Scan(self.majors_gpa, pop_headers=True)],
            ])))

        self.assertEquals(result, expected_joins)

    def test_hash_join(self):
        expected_joins = [
          ('3', 'Christie', '28', 'accounting', '3', 'accounting', '3'),
          ('7', 'Lori', '62', 'business', '7', 'business', '4'),
          ('2', 'Jason', '33', 'econ', '2', 'econ', '2'),
          ('5', 'Carolyn', '33', 'econ', '2', 'econ', '2'),
          ('4', 'Gayle', '33', 'edu', '4', 'edu', '4.5'),
          ('1', 'Brian', '30', 'eng', '1', 'eng', '4'),
          ('6', 'Michael', '65', 'law', '6', 'law', '5'),
        ]

        projection1 = lambda r: r[3]
        projection2 = lambda r: r[1]

        result = list(execute(tree(
            [HashJoin(projection1, projection2),
                [Scan(self._data, pop_headers=True)],
                [Scan(self.majors_gpa, pop_headers=True)],
            ])))

        self.assertEquals(result, expected_joins)

    def test_sort_merge_join(self):
        expected_joins = [
          ('3', 'Christie', '28', 'accounting', '3', 'accounting', '3'),
          ('7', 'Lori', '62', 'business', '7', 'business', '4'),
          ('2', 'Jason', '33', 'econ', '2', 'econ', '2'),
          ('5', 'Carolyn', '33', 'econ', '2', 'econ', '2'),
          ('4', 'Gayle', '33', 'edu', '4', 'edu', '4.5'),
          ('1', 'Brian', '30', 'eng', '1', 'eng', '4'),
          ('6', 'Michael', '65', 'law', '6', 'law', '5'),
        ]

        theta = lambda _row1, _row2: _row1[3] == _row2[1]
        projection1 = lambda r: r[3]
        projection2 = lambda r: r[1]

        result = list(execute(tree(
            [SortMergeJoin(theta, projection1, projection2),
                [Sort(projection1),
                    [Scan(self._data, pop_headers=True)]],
                [Sort(projection2),
                    [Scan(self.majors_gpa, pop_headers=True)]],
            ]
        )))

        self.assertEquals(result, expected_joins)

    def test_mem_index_scan__count(self):
        schema = ('id', 'name', 'age', 'major')
        data = [
          (1, 'Brian', '30', 'eng'),
          (2, 'Jason', '33', 'econ'),
          (3, 'Christie', '28', 'accounting'),
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          (6, 'Michael', '65', 'law'),
          (7, 'Lori', '62', 'business')
        ]

        # index on id
        projection = lambda r: r[0]

        operator = operators.GreaterThan('id', 4, schema)

        result = list(execute(tree(
            [Count(),
                [BPlusTree(data, projection).search(operator)]
            ]
        )))
        expected = [
          (3,)
        ]
        self.assertEquals(result, expected)

    def test_mem_index_scan__projection(self):
        schema = ('id', 'name', 'age', 'major')
        data = [
          (1, 'Brian', '30', 'eng'),
          (2, 'Jason', '33', 'econ'),
          (3, 'Christie', '28', 'accounting'),
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          (6, 'Michael', '65', 'law'),
          (7, 'Lori', '62', 'business')
        ]

        # index on id
        projection = lambda r: r[0]

        operator = operators.GreaterThan('id', 4, schema)

        result = list(execute(tree(
            [Projection(lambda r: r[1]),
                [BPlusTree(data, projection).search(operator)]
            ]
        )))
        expected = [
          ('Carolyn'),
          ('Michael'),
          ('Lori')
        ]
        self.assertEquals(result, expected)

    def test_mem_index_scan__sort(self):
        schema = ('id', 'name', 'age', 'major')
        data = [
          (1, 'Brian', 30, 'eng'),
          (2, 'Jason', 33, 'econ'),
          (3, 'Christie', 28, 'accounting'),
          (4, 'Gayle', 33, 'edu'),
          (5, 'Carolyn', 33, 'econ'),
          (6, 'Michael', 65, 'law'),
          (7, 'Lori', 62, 'business')
        ]

        # index on age
        projection = lambda r: r[2]
        operator = operators.LessThan('age', 34, schema)

        result = list(execute(tree(
            [Sort(lambda r: (r[1], r[0])),
                [Projection(lambda r: (r[1], r[2])),
                    [BPlusTree(data, projection).search(operator)]
                ]
            ]
        )))
        expected = [
          ('Christie', '28'),
          ('Brian', '30'),
          ('Carolyn', '33'),
          ('Gayle', '33'),
          ('Jason', '33'),
        ]
        self.assertEquals(result, expected)

    def test_mem_index_scan__sort__merge_join(self):
        # index on age
        schema = ('id', 'name', 'age', 'major')
        data = [
          (1, 'Brian', 30, 'eng'),
          (2, 'Jason', 33, 'econ'),
          (3, 'Christie', 28, 'accounting'),
          (4, 'Gayle', 33, 'edu'),
          (5, 'Carolyn', 33, 'econ'),
          (6, 'Michael', 65, 'law'),
          (7, 'Lori', 62, 'business')
        ]
        projection = lambda r: r[2]
        operator = operators.LessThan('age', 34, schema)

        # index on gpa
        schema2 = ('id', 'major', 'gpa')
        data2 = [
          ('3', 'accounting', 3),
          ('7', 'business', 4),
          ('2', 'econ', 2),
          ('4', 'edu', 4.5),
          ('1', 'eng', 4),
          ('6', 'law', 5),
        ]
        projection2 = lambda r: r[2]
        operator2 = operators.GreaterThan('gpa', 3, schema2)

        theta = lambda r1, r2: r1[1] == r2[0]

        # select d.name, d.major, d2.major, d2.gpa
        # from data d, data2 d2
        # where d.major = d2.major
        # and d.age < 34
        # and d2.gpa > 3

        # since it's sorted on major, edu comes first
        # since sort uses csv reader, everything's a string
        expected_joins = [
          ('Gayle','edu', 'edu', '4.5'),
          ('Brian','eng', 'eng', '4'),
        ]

        result = list(execute(tree(
            [SortMergeJoin(theta, lambda r: r[1], lambda r: r[0]),
                [Sort(lambda r: r[1]),
                    [Projection(lambda r: (r[1], r[3])),
                        [BPlusTree(data, projection).search(operator)]
                    ]
                ],
                [Sort(lambda r: r[0]),
                    [Projection(lambda r: (r[1], r[2])),
                        [BPlusTree(data2, projection2).search(operator2)]
                    ]
                ]
            ]
        )))
        self.assertEquals(result, expected_joins)

    # def test_index_scan(self):
        # result = list(execute(tree(
            # [Count(),
                # [Projection(lambda r: r.ratings_rating),
                    # [IndexScan(
                      # index='ratings_movie_id_index',
                      # cond=(EQUALS, 5000))]]
            # ]
        # )))

