from executor.nodeIterator import Iterator

class NestedLoopJoin(Iterator):
    def __init__(self, theta):
        self.theta = theta

    def __next__(self):
        if not hasattr(self, '_iterable'):
            self._iterable = self.get_iterable()
        return next(self._iterable)

    def __close__(self):
        pass

    def get_iterable(self):
        _input1 = self.inputs[0]
        _input2 = self.inputs[1]

        for record1 in _input1:
            if record1 is self.EOF:
                yield self.EOF
            for record2 in _input2:
                if record2 is self.EOF:
                    _input2.reset()
                    break
                if self.theta(record1, record2):
                    yield record1 + record2

    @staticmethod
    def parse_args(schema, args):
        import ipdb; ipdb.set_trace();
        # TODO
        pass

        # schema_indices = [
            # schema.index(arg)
            # for arg in args
        # ]
        # return lambda row: tuple((row[schema_index] for schema_index in schema_indices))

