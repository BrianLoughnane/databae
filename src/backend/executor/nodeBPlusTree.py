from math import inf

from executor.nodeIterator import Iterator

# note - delimiter must be >= 3
DELIMITER = 3

class Node():
    def __init__(self, values):
        self.values = values
        self._next = None
        self._prev = None
        self.parent = None

class LeafNode(Node):
    def __init__(self, values):
        self.values = values
        self._next = None
        self._prev = None
        self.parent = None

    def __iter__(self):
        return iter(self.values)

    def add(self, record, projection):
        self.values.append(record)
        self.values.sort(key=projection)

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

class BPlusTree(Iterator):
    def __init__(self, data, projection):
        # assumes data is sorted on the index key

        # create a bunch of leaf note "pages" representing heap
        leaf_nodes = LeafNode.create_pages_from_data(data)

        # create a linked list over the heap pages
        lst = LinkedList(leaf_nodes)

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

        node = node or self.root

        if isinstance(node, LeafNode):
            return self.iterate_over_leaf(operator, node)

        elif operator.isEquals or operator.isGreaterThan:
            leaf_node = self.get_leaf_node_for_value(value)
            return self.iterate_over_leaf(operator, leaf_node)

        elif operator.isLessThan:
            # for less than, start at first leaf node
            min_leaf_node = self.get_min_leaf_node()
            return self.iterate_over_leaf(operator, min_leaf_node)

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

