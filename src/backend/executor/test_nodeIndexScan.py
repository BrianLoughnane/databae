import unittest

from executor.execExpr import tree, execute
from executor.nodeScan import Scan
from executor.nodeIterator import Iterator
from executor.nodeBPlusTree import BPlusTree, LeafNode, LinkedList
# from executor.nodeIndexScan import MemIndexScan
from operators import Equals, LessThan, GreaterThan

class TestBPlusTree(unittest.TestCase):
    def setUp(self):
        self.schema = ('id', 'name', 'age', 'major')
        # index on id (column 1)
        self.data = [
          (1, 'Brian', '30', 'eng'),
          (2, 'Jason', '33', 'econ'),
          (3, 'Christie', '28', 'accounting'),
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          (6, 'Michael', '65', 'law'),
          (7, 'Lori', '62', 'business'),
          (8, 'Zoe', '4', 'funny business'),
          (9, 'Andy', '4', 'funny business'),
          (10, 'Flanagan', '44', 'funny business'),
        ]

        # assumes data is sorted on the index key

        # create a bunch of leaf note "pages" representing heap
        leaf_nodes = LeafNode.create_pages_from_data(self.data)

        # create a linked list over the heap pages
        lst = LinkedList(leaf_nodes)

        # projection gets indexed column value
        projection = lambda r: r[0]

        # create a tree over the indexed vals in the list(B+ tree)
        self.tree = BPlusTree(lst, projection)

    def test_search__equals(self):
        operator = Equals('id', 5, self.schema)
        search = self.tree.search(operator)
        result = next(search)
        expected = (5, 'Carolyn', '33', 'econ')
        self.assertEquals(result, expected)

    def test_search__equals__max(self):
        operator = Equals('id', 10, self.schema)
        search = self.tree.search(operator)
        result = next(search)
        expected = (10, 'Flanagan', '44', 'funny business')
        self.assertEquals(result, expected)

    def test_search__less_than(self):
        operator = LessThan('id', 6, self.schema)
        search = self.tree.search(operator)
        result = list(search)
        expected = [
          (1, 'Brian', '30', 'eng'),
          (2, 'Jason', '33', 'econ'),
          (3, 'Christie', '28', 'accounting'),
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          Iterator.EOF,
        ]
        self.assertEquals(result[:6], expected)

    def test_search__greater_than(self):
        operator = GreaterThan('id', 3, self.schema)
        search = self.tree.search(operator)
        result = list(search)
        expected = [
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          (6, 'Michael', '65', 'law'),
          (7, 'Lori', '62', 'business'),
          (8, 'Zoe', '4', 'funny business'),
          (9, 'Andy', '4', 'funny business'),
          (10, 'Flanagan', '44', 'funny business'),
          Iterator.EOF,
        ]
        self.assertEquals(result[:8], expected)

    def test_insert(self):
        '''
        note - for some reason nosetests is inserting this data
        before the above search tests, and breaking them if
        run together
        '''
        # -------------
        operator = Equals('id', 11, self.schema)

        search = self.tree.search(operator)
        result = next(search)
        expected = Iterator.EOF
        self.assertEquals(result, expected)

        new_tuple = (11, 'Flanahan', '45', 'funny business')
        self.tree.insert(new_tuple)

        search = self.tree.search(operator)
        result = next(search)
        expected = new_tuple
        self.assertEquals(result, expected)

        # -------------

        operator = Equals('id', 6.5, self.schema)

        search = self.tree.search(operator)
        result = next(search)
        expected = Iterator.EOF
        self.assertEquals(result, expected)

        new_tuple = (6.5, 'Jenkins', '54', 'funky business')
        self.tree.insert(new_tuple)

        search = self.tree.search(operator)
        result = next(search)
        expected = new_tuple

        self.assertEquals(result, expected)

        # -------------

        operator = Equals('id', 9.8, self.schema)

        search = self.tree.search(operator)
        result = next(search)
        expected = Iterator.EOF
        self.assertEquals(result, expected)

        new_tuple = (9.8, 'Stewie', '510', 'galavanting')
        self.tree.insert(new_tuple)

        search = self.tree.search(operator)
        result = next(search)
        expected = new_tuple

        self.assertEquals(result, expected)


