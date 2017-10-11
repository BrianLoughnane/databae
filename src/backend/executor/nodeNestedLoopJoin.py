from executor.nodeIterator import Iterator

class NestedLoopJoin(Iterator):
    def __init__(self, theta, _input1, _input2):
        self._input1 = _input1
        self._input2 = _input2
        self.theta = theta

    def __next__(self):
        import ipdb; ipdb.set_trace();
        for record1 in self._input1:
            for record2 in self._input2:
                if self.theta(record1, record2):
                    return record1 + record2
            self._input2.reset()

    def __close__(self):
        pass

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

