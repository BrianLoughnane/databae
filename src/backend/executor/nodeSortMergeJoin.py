from executor.nodeIterator import Iterator
from executor.nodeSort import Sort

class SortMergeJoin(Iterator):
    def __init__(self, theta, _input1, _input2):
        '''
        Assumes sorted input streams
        '''
        self._input1 = _input1
        self._input2 = _input2
        self.theta = theta

        # self._iterable = self.func()
        self.buffers = {
            self._input1: next(self._input1),
            self._input2: next(self._input2),
        }

    def _get_next_from_buffer(self):
        '''
        Returns the lowest of the in memory values, and replaces it with
        the next value of the input that it came from.
        '''
        items = list(self.buffers.items())
        filtered_items = list(filter(lambda t: t[1] != self.EOF, items))
        sorted_items = sorted(filtered_items, key=lambda item: self._sort(item[1]))
        try:
            scanner_tuple = sorted_items.pop(0)
        except IndexError:
            return self.EOF
        scanner, _next = scanner_tuple
        self.buffers[scanner] = next(scanner)
        return _next

    def func(self):
        for record1 in self._input1:
            if record1 is self.EOF:
                yield self.EOF
            for record2 in self._input2:
                if record2 is self.EOF:
                    self._input2.reset()
                    break
                if self.theta(record1, record2):
                    yield record1 + record2

    def __next__(self):
        return next(self._iterable)

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

