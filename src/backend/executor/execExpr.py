from functools import partial, reduce

from executor.nodeDistinct import Distinct
from executor.nodeFileScan import FileScan
from executor.nodeIterator import Iterator
from executor.nodeProjection import Projection
from executor.nodeScan import Scan
from executor.nodeSelection import Selection
from executor.nodeSort import Sort

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
        parent.inputs.append(tree(children))
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

    turns into
    [Projection(predicate),
        [Selection(theta),
            [FileScan(projection)],
        ],
    ]
    '''
    name_map = {
      "DISTINCT": Distinct,
      "FILESCAN": FileScan,
      "PROJECTION": Projection,
      "SCAN": Scan,
      "SELECTION": Selection,
      "SORT": Sort,
    }

    if not representation:
        return None

    # create a leaf node for no other reason than
    # to get the schema
    leaf_representation = representation[-1]
    leaf_name = leaf_representation[0]
    leaf_args = leaf_representation[1]
    leaf_class = name_map.get(leaf_name)
    leaf_node = leaf_class(leaf_args)
    schema = next(leaf_node)
    leaf_node.__close__()

    # build up pipeline
    original_pipeline = [[]]
    pipeline = original_pipeline[0]
    for node_name, args in representation:
        pipeline.append([])
        pipeline = pipeline[-1]

        node_class = name_map.get(node_name)
        parsed_args = node_class.parse_args(list(schema), args)
        schema = node_class.parse_schema(list(schema), args)
        pipeline.append(node_class(parsed_args))

    pipeline = original_pipeline[0][0]

    values = list(execute(tree(pipeline)))

    # not needed really, just to see output:
    if PRINT:
        for value in values:
            print(value)

    return values

