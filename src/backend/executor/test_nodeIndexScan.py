from math import inf
import unittest

from executor.execExpr import tree, execute
from executor.nodeScan import Scan
from executor.nodeIterator import Iterator
# from executor.nodeIndexScan import MemIndexScan
from operators import Equals, LessThan, GreaterThan

DELIMITER = 3

class Node():
    def __init__(self, values):
        self.values = values
        self._next = None
        self._prev = None

    def __iter__(self):
        return iter(self.values)

    @staticmethod
    def create_pages_from_data(data):
        pages = []
        while data:
            page_records = []
            for ii in range(DELIMITER):
                try:
                    page_records.append(data.pop(0))
                except IndexError:
                    if page_records:
                        pages.append(Node(page_records))
                    break
            pages.append(Node(page_records))
        return pages

class LinkedList():
    def __init__(self, nodes):
        first_node = nodes[0]
        last_node = nodes[0]

        for node in nodes[1:]:
            node._prev = last_node
            last_node._next = node
            last_node = node

        self.first_node = first_node
        self.last_node = last_node

    def get_first_node(self):
        return self.first_node

class IndexPage():
    '''
    `values` is an array of (value, pointer) tuples
    where value is the minval for the node that
    pointer is pointing at
    '''
    def __init__(self, values=[]):
        self.values = values
        self.parent = None

    def __iter__(self):
        return iter(self.values)

    def add(self, tup):
        self.values.append(tup)
        self.values.sort(key=lambda t: t[0])

    def has_room(self):
        return len(self.values) < DELIMITER

    def split(self):
        middle_index = int(len(self.values) / 2)

        min_vals = self.values[:middle_index]
        max_vals = self.values[middle_index:]

        min_index = IndexPage(values=min_vals)
        max_index = IndexPage(values=max_vals)

        return min_index, max_index

    def minval(self):
        return self.values[0][0]

    def minpointer(self):
        return self.values[0][1]

    def get_tuple(self):
        return (self.minval(), self)

class BPlusTree():
    def __init__(self, lst, projection):
        self.projection = projection
        self.create_index_tree_over_linked_list(lst)

    def create_index_tree_over_linked_list(self, lst):
        self.root = index_page = IndexPage()

        # add initial tuple (-Infinity, -> lst.first_node) to index
        node = lst.get_first_node()
        pair = (-inf, node)

        self.add_tuple_to_index(pair, index_page)

        # for node in linked list, starting at 2nd node:
        while True:
            node = node._next
            if not node:
                break

            # get min value
            minval = self.projection(node.values[0])
            pair = (minval, node)

            self.add_tuple_to_index(pair, index_page)

        return index_page

    def add_tuple_to_index(self, pair, index_page):
        if index_page.has_room():
            index_page.add(pair)
        else:
            index_page_min, index_page_max = index_page.split()
            min_tuple = index_page_min.get_tuple()
            max_tuple = index_page_max.get_tuple()

            if index_page.parent:
                self.add_tuple_to_index(max_tuple, index_page.parent)
            else:
                new_parent = IndexPage(values=[min_tuple, max_tuple])
                index_page_min.parent = new_parent
                index_page_max.parent = new_parent
                self.root = new_parent

    def iterate_over_leaf(self, operator, node):
        value = operator.get_value()

        for record in node:
            record_value = self.projection(record)
            if operator.check(record):
                yield record

        if operator.isGreaterThan:
            if not node._next:
                yield Iterator.EOF
            else:
                yield from self.iterate_over_leaf(
                    operator, node._next)

        if operator.isLessThan and record_value < value:
            if not node._next:
                yield Iterator.EOF

            else:
                yield from self.iterate_over_leaf(
                    operator, node._next)

        yield Iterator.EOF


    def search(self, operator, node=None):
        value = operator.get_value()

        current_node = node or self.root

        if isinstance(current_node, Node):
            return self.iterate_over_leaf(operator, current_node)

        elif operator.isLessThan:
            # for less than, start at first leaf node
            return self.search(
                operator,
                node=current_node.minpointer()
            )

        elif operator.isEquals or operator.isGreaterThan:
            # go to pointer prior to first key > value
            previous_pointer = None
            for key, pointer in current_node:
                if key > value:
                    return self.search(
                        operator,
                        node=(previous_pointer or pointer)
                    )
                previous_pointer = pointer
            return self.search(
                operator,
                node=previous_pointer,
            )

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
        leaf_nodes = Node.create_pages_from_data(self.data)

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

    def test_search__less_than(self):
        operator = LessThan('id', 7, self.schema)
        search = self.tree.search(operator)
        result = list(search)
        expected = [
          (1, 'Brian', '30', 'eng'),
          (2, 'Jason', '33', 'econ'),
          (3, 'Christie', '28', 'accounting'),
          (4, 'Gayle', '33', 'edu'),
          (5, 'Carolyn', '33', 'econ'),
          (6, 'Michael', '65', 'law'),
          Iterator.EOF,
        ]
        self.assertEquals(result[:7], expected)

