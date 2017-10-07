from functools import partial, reduce

from executor.nodeIterator import Iterator
from executor.nodeProjection import Projection
from executor.nodeScan import Scan
from executor.nodeSelection import Selection

def execute(representation):
    '''
    Given a high-level representation of a single table
    query, be able to execute it and return the result.
    The input should be in this format:

    [
      ["PROJECTION", ["id", "name"]],
      ["SELECTION", ["id", "EQUALS", "5000"]],
      ["FILESCAN", ["movies"]]
    ]

    With an output of (5000, "Medium Cool (1969)")`
    '''
    name_map = {
      "PROJECTION": Projection,
      "SCAN": Scan,
      "SELECTION": Selection,
    }

    if not representation:
        return None

    representation.reverse()

    leaf_representation = representation.pop(0)
    leaf_name = leaf_representation[0]
    leaf_args = leaf_representation[1]
    leaf_class = name_map.get(leaf_name)
    leaf_node = leaf_class(leaf_args[0])

    schema = leaf_node.get_schema()

    pipeline = []
    for node_name, args in representation:
        node_class = name_map.get(node_name)
        parsed_args = node_class.parse_args(list(schema), args)
        schema = node_class.parse_schema(list(schema), args)
        pipeline.append(partial(node_class, parsed_args))

    master_generator = reduce(
        lambda accum, next_item: next_item(accum),
        pipeline,
        leaf_node
    )

    values = []
    while True:
        _next = master_generator.__next__()
        if _next == Iterator.EOF:
            master_generator.__close__()
            break
        values.append(_next)
    return values

