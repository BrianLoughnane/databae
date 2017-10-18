from functools import partial

from executor.nodeIterator import Iterator
from operators import operator_map

class Projection(Iterator):
    def __init__(self, _projector):
        super().__init__()
        self._projector = _projector

    def __next__(self):
        _input = self._inputs[0]

        _next = next(_input)

        if _next == self.EOF:
          self.close_input(_input)
          return self.EOF
        return(self._projector(_next))

    @staticmethod
    def parse_args(schema, args):
        '''
        parses language arguments into constructor arguments
        '''
        schema_indices = [
            schema.index(arg)
            for arg in args
        ]
        def projector(schema_indices, row):
          return [
              value for index, value in enumerate(row)
              if index in schema_indices
          ]
        return partial(projector, schema_indices)

    @staticmethod
    def parse_schema(schema, args):
        '''
        parses an existing schema into a new schema, based on the
        constructor args being passed

        Projecter alters schema to whatever args are passed
        '''
        return args



