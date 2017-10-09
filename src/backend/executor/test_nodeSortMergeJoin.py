from executor.nodeIterator import Iterator
from executor.nodeSort import Sort

class SortMergeJoin(Iterator):
    def __init__(self, _equality_predicate, _input1, _input2):
        '''
        Takes 2 input streams and an equality predicate.

        The predicate parameters are (row_from_input1, row_from_input2)

        Idea is to return a join of the 2 streams if the
        the predicate returns True.

        __init__
        - instantiates Sort nodes on each input.
        - pulls the next row from each node into memory

        __next__
        - run equality predicate on the 2 in memory rows
        - if passing
          - return a joined tuple
        - if failing
          - throw away the smaller of the rows and replace
            it with the next item of the respective sort node
          - rerun equality predicate
        '''

    def __next__(self):
        '''
        '''
        _next = self._get_next_from_buffer()
        if _next is self.EOF:
            self._input.__close__()
        return _next

    def __close__(self):
        pass


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
        self.buffers[scanner] = scanner.__next__()
        return _next

    def _write_partition(self, partition):
        '''
        Writes 1 partition's worth of rows to a temporary file.
        Returns the name of the temporary file path.
        '''
        tmp_filename = 'tmp/%s' % uuid.uuid4()
        with open(tmp_filename, 'w') as _file:
            writer = csv.writer(_file, delimiter=',')
            writer.writerows(partition)
        return tmp_filename

    def _get_sorted_partition(self):
        '''
        Pulls 1 partition's worth of rows into memory and sorts it.

        Returns a tuple with the sorted values, as well as a flag
        indicating if the EOF was reached.
        '''
        eof = False
        values = []
        for ii in range(0, self.PARTITION_LIMIT):
            _next = self._input.__next__()
            if _next == self.EOF:
                eof = True
                break
            values.append(_next)
        return eof, sorted(values, key=self._sort)

    @staticmethod
    def parse_args(schema, args):
        schema_indices = [
            schema.index(arg)
            for arg in args
        ]

        return lambda row: tuple((row[schema_index] for schema_index in schema_indices))
