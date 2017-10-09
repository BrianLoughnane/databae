import csv
import os
import uuid

from executor.nodeIterator import Iterator
from executor.nodeFileScan import FileScan

class Sort(Iterator):
    '''
    __next__
    When next() is called, the lowest of in memory values is
    returned (after being replaced with the respective file's
    next value).

    Once all FileScans are empty, return self.EOF
    '''

    # B-1 pages worth of tuples data.
    # I don't know what that number is, so I'm going to make it up as 1000 records
    PARTITION_LIMIT = 1000

    def __init__(self, _sort, _input):
        '''
        Pull in B-1 pages worth of tuples data, quick sort,
        write out to a partition, repeat until EOF.

        Instantiate FileScan nodes over the partitions.

        A list holds the next values for each partion in memory
        '''
        self._input = _input
        self._sort = _sort

        # drain input stream, 1 partition at a time
        partition_paths = []
        scanners = []
        while True:
            eof, partition = self._get_sorted_partition()
            partition_path = self._write_partition(partition)
            partition_scanner = FileScan(partition_path)
            partition_paths.append(partition_path)
            scanners.append(partition_scanner)
            if eof:
                break

        self.partition_paths = partition_paths

        self.buffers = {}
        for scanner in scanners:
            self.buffers[scanner] = scanner.__next__()

    def _get_next_from_buffer(self):
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
        eof = False
        values = []
        for ii in range(0, self.PARTITION_LIMIT):
            _next = self._input.__next__()
            if _next == self.EOF:
                eof = True
                break
            values.append(_next)
        return eof, sorted(values, key=self._sort)

    def __next__(self):
        _next = self._get_next_from_buffer()
        if _next is self.EOF:
            self._input.__close__()
        return _next

    def __close__(self):
        '''
        delete all temporary files
        '''
        for path in self.partition_paths:
            os.remove(path)

    @staticmethod
    def parse_args(schema, args):
        schema_indices = [
            schema.index(arg)
            for arg in args
        ]

        return lambda row: tuple((row[schema_index] for schema_index in schema_indices))
