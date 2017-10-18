from math import inf
import unittest

from executor.execExpr import tree, execute
from executor.nodeScan import Scan
from executor.nodeIterator import Iterator
# from executor.nodeIndexScan import MemIndexScan
from operators import Equals, LessThan, GreaterThan

# note - delimiter must be >= 3
DELIMITER = 3

class Node():
    def __init__(self, values=[]):
        self.values = values
        self.parent = None

class LeafNode(Node):
    def __init__(self, values):
        super().__init__(values=values)
        self._next = None
        self._prev = None

    def __iter__(self):
        return iter(self.values)

    def add(self, record, projection):
        self.values.append(record)
        self.values.sort(key=projection)

    def update(self, index, transform):
        original = self.values[index]
        new = tuple(transform([*original]))
        self.values[index] = new

    def has_room(self):
        return len(self.values) < DELIMITER

    def split(self):
        middle_index = int(len(self.values) / 2)
        min_vals = self.values[:middle_index]
        max_vals = self.values[middle_index:]

        self.values = min_vals
        max_node = LeafNode(max_vals)

        _next = self._next

        _next._prev = max_node
        max_node._next = _next

        self._next = max_node

        return self, max_node

    def minval(self):
        return self.values[0][0]

    def get_tuple(self):
        return (self.minval(), self)

    @staticmethod
    def create_pages_from_data(data):
        pages = []
        while data:
            page_records = []
            for ii in range(DELIMITER):
                try:
                    page_records.append(data.pop(0))
                except IndexError:
                    break
            pages.append(LeafNode(page_records))
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

    def print(self):
        node = self.first_node

        while True:
            if not node:
                break
            print(node.values)
            node = node._next

class IndexNode(Node):
    '''
    `values` is an array of (value, pointer) tuples
    where value is the minval for the node that
    pointer is pointing at
    '''
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

        self.values = min_vals
        max_index = IndexNode(values=max_vals)

        # reassign parents for all pointers in max page
        for key, pointer in max_vals:
            pointer.parent = max_index

        return self, max_index

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
        self.root = index_page = IndexNode()

        # add initial tuple (-Infinity, -> lst.first_node) to index
        node = lst.get_first_node()
        pair = (-inf, node)
        self.add_tuple_to_index(pair, index_page)
        node.parent = index_page

        # for node in linked list, starting at 2nd node:
        while True:
            node = node._next
            if not node:
                break

            # get min value
            minval = self.projection(node.values[0])
            pair = (minval, node)

            index_page = self.add_tuple_to_index(pair, index_page)

        return index_page

    def add_record_to_node(self, record, node):
        if node.has_room():
            node.add(record, self.projection)

        else:
            _, node_page_max = node.split()
            node_page_max.add(record, self.projection)
            max_tuple = node_page_max.get_tuple()
            self.add_tuple_to_index(max_tuple, node.parent)

    def add_tuple_to_index(self, pair, index_page):
        key, node = pair
        if index_page.has_room():
            index_page.add(pair)
            node.parent = index_page
            return index_page

        else:
            # split
            index_page_min, index_page_max = index_page.split()

            # add tuple to index_page_max
            self.add_tuple_to_index(pair, index_page_max)

            min_tuple = index_page_min.get_tuple()
            max_tuple = index_page_max.get_tuple()

            if index_page_min.parent:
                self.add_tuple_to_index(
                    max_tuple, index_page_min.parent)

            else:
                # if splitting root node
                new_parent = IndexNode(values=[min_tuple, max_tuple])
                index_page_min.parent = new_parent
                index_page_max.parent = new_parent
                self.root = new_parent

            return index_page_max

    def _yield(self, value):
        yield value

    def _yield_from(self, value):
        yield from value

    def _node_selector(self, node, operator):
        for ii, record in enumerate(node):
            record_value = self.projection(record)
            if operator.check(record):
                yield value

    def _node_updater(self, node, operator, update):
        for ii, record in enumerate(node):
            record_value = self.projection(record)
            if operator.check(record):
                node.update(ii, update)
        return record_value

    def iterate_over_leaf(self, operator, node, update=None):
        value = operator.get_value()

        if update:
            last_record_value = self._node_updater(node, operator, update)
        else:
            last_record_value = self._node_selector(node, operator)

        if operator.isGreaterThan:
            if not node._next:
                self._yield(Iterator.EOF)
            else:
                self._yield_from(self.iterate_over_leaf(
                    operator, node._next, update=update))

        if operator.isLessThan and last_record_value < value:
            if not node._next:
                self._yield(Iterator.EOF)

            else:
                self._yield_from(self.iterate_over_leaf(
                    operator, node._next, update=update))

        self._yield(Iterator.EOF)

    def get_min_leaf_node(self, node=None):
        node = node or self.root

        if isinstance(node, LeafNode):
            return node

        return self.get_min_leaf_node(node=node.minpointer())

    def get_leaf_node_for_value(self, value, node=None):
        node = node or self.root

        if isinstance(node, LeafNode):
            return node

        # go to pointer prior to first key > value
        previous_pointer = None
        for key, pointer in node:
            if key > value:
                return self.get_leaf_node_for_value(
                    value,
                    node=(previous_pointer or pointer)
                )
            previous_pointer = pointer
        return self.get_leaf_node_for_value(
            value,
            node=(previous_pointer or pointer)
        )

    def search(self, operator, node=None, update=None):
        value = operator.get_value()
        node = node or self.root

        if isinstance(node, LeafNode):
            return self.iterate_over_leaf(
                operator, node, update=update)

        elif operator.isEquals or operator.isGreaterThan:
            leaf_node = self.get_leaf_node_for_value(value)
            return self.iterate_over_leaf(
                operator, leaf_node, update=update)

        elif operator.isLessThan:
            # for less than, start at first leaf node
            min_leaf_node = self.get_min_leaf_node()
            return self.iterate_over_leaf(
                operator, min_leaf_node, update=update)

    def update(self, operator, callback):
        self.search(operator, update=callback)

    def insert(self, record):
        index_value = self.projection(record)
        leaf_node = self.get_leaf_node_for_value(index_value)
        self.add_record_to_node(record, leaf_node)

    def leaves(self):
        node = self.get_min_leaf_node()
        nodes = [node]
        while True:
            node = node._next
            if not node:
                break
            nodes.append(node)
        return nodes

    def print_leaves(self):
        node = self.get_min_leaf_node()
        while True:
            if not node:
                break
            print(node.values)
            node = node._next

    def print(self, node=None):
        node = node or self.root

        print('node')
        print(node)
        print('node.values')
        print(node.values)

        for key, pointer in node:
            try:
                print('key')
                print(key)
                print('pointer')
                print(pointer)
                self.print(node=pointer)
            except ValueError:
                pass


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

    def test_update(self):
        '''
        record = (10, 'Flanagan', '44', 'funny business')
        '''
        operator = Equals('id', 10, self.schema)

        def update(record):
           record[2] = '100'
           return record

        search = self.tree.update(operator, update)
        search = self.tree.search(operator)
        result = next(search)
        expected = (10, 'Flanagan', '100', 'funny business')

        self.assertEquals(result, expected)
