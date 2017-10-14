from functools import partial, reduce

from executor.nodeDistinct import Distinct
from executor.nodeFileScan import FileScan
from executor.nodeIterator import Iterator
from executor.nodeProjection import Projection
from executor.nodeScan import Scan
from executor.nodeSelection import Selection
from executor.nodeSort import Sort

class Print(Iterator):
    def __init__(self, null):
        super().__init__()

    def __next__(self):
        _input = self._inputs[0]
        _next = next(_input)
        print(str(_input), _next)
        return _next

class Debug(Iterator):
    def __init__(self, null):
        super().__init__()

    def __next__(self):
        if len(self._inputs) == 1:
            _input = self._inputs[0]
            _next = next(_input)
        if len(self._inputs) == 2:
            _input1 = self._inputs[0]
            _input2 = self._inputs[1]
            _next1 = next(_input)
            _next2 = next(_input)
        import ipdb; ipdb.set_trace();
        return _next

PRINT = False

def tree(pipeline):
    '''
    Takes a nested list structure of instantiated
    nodes and links them together.

    The first node of the list will take subsequent
    items as inputs

    [Projection(predicate),
        [NestedLoopJoin(theta),
            [Sort(projection),
                [Scan(relationA)],
                [Scan(relationB)],
            ],
        ],
        [NestedLoopJoin(theta),
            [Sort(projection),
                [Scan(relationC)],
                [Scan(relationD)],
            ],
        ],
    ]

    '''
    parent = pipeline[0]
    for children in pipeline[1:]:
        parent._inputs.append(tree(children))
    return parent

def execute(tree):
    for _next in tree:
        if _next == Iterator.EOF:
            tree.__close__()
            break
        yield _next

def parse_and_execute(representation):
    '''
    Given a high-level representation of a single table
    query, be able to execute it and return the result.
    The input should be in this format:

    [
      ["PROJECTION", ["id", "name"]],
      ["SELECTION", ["id", "EQUALS", "5000"]],
      ["FILESCAN", ["movies"]]
    ]

    First, turn it into a "pipeline":

    [Projection(predicate),
        [Selection(theta),
            [FileScan(projection)],
        ],
    ]

    Then a tree, with pointers to provider nodes
    as "inputs" []

    Finally, execute() creates a generator over the
    topmost node in the tree
    '''

    name_map = {
      "DISTINCT": Distinct,
      "FILESCAN": FileScan,
      "PROJECTION": Projection,
      "SCAN": Scan,
      "SELECTION": Selection,
      "SORT": Sort,
      # for debugging
      "PRINT": Print,
      "DEBUG": Debug,
    }

    if not representation:
        return None
    representation.reverse()

    # create a leaf node for no other reason than
    # to get the schema
    leaf_representation = representation[0]
    leaf_name = leaf_representation[0]
    leaf_args = leaf_representation[1]
    leaf_class = name_map.get(leaf_name)
    leaf_node = leaf_class(leaf_args)
    schema = next(leaf_node)
    leaf_node.__close__()

    # build up pipeline...

    # get classes

    node_classes = [
      name_map.get(node_name)
      for node_name, args in representation
    ]

    # get the state of the schema for each node

    parsed_schema = [schema]
    counter = -1
    for node_name, args in representation:
      counter += 1
      node_class = node_classes[counter]
      schema = node_class.parse_schema(list(schema), args)
      parsed_schema.append(schema)

    # determine the arguments passed to instantiate
    # the nodes, based on the schema state

    parsed_args = []
    counter = -1
    for node_name, args in representation:
      counter += 1
      node_class = node_classes[counter]
      schema = parsed_schema[counter]
      args = node_class.parse_args(list(schema), args)
      parsed_args.append(args)

    # build up a pipeline of instantiated nodes

    pipeline = []
    counter = -1
    for node_name, args in representation:
        counter += 1
        node_class = node_classes[counter]
        node_args = parsed_args[counter]
        node_instance = node_class(node_args)
        pipeline = [node_instance, pipeline] \
            if pipeline else [node_instance]

    values = list(execute(tree(pipeline)))

    # not needed really, just to see output:
    if PRINT:
        for value in values:
            print(value)

    return values

