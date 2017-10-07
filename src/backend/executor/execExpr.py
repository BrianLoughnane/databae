from functools import partial, reduce

from executor.nodeScan import Scan
from executor.nodeSelection import Selection

EOF = 'end of fun'

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
    # reverse of the list (not in place)
    # reversed_representation = representation[::-1]

    name_map = {
      "SCAN": Scan,
      "SELECTION": Selection,
    }

    if not representation:
        return None

    leaf_representation = representation.pop(-1)
    leaf_name = leaf_representation[0]
    leaf_args = leaf_representation[1]
    leaf_class = name_map.get(leaf_name)
    leaf_node = leaf_class(leaf_args[0])

    pipeline = []
    for node_name, args in representation:
        node_class = name_map.get(node_name)
        parsed_args = node_class.parse_args(args)
        pipeline.append(partial(node_class, parsed_args))

    master_generator =  reduce(lambda a,b: a(b), pipeline)(leaf_node)

    values = []

    while True:
        _next = master_generator.__next__()
        if _next == EOF:
            master_generator.__close__()
            break
        values.append(_next)

    return values

