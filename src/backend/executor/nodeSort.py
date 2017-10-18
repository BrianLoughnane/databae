import csv
import os
import uuid

from executor.nodeIterator import Iterator
from executor.nodeFileScan import FileScan

class Sort(Iterator):
    # B-1 pages worth of tuples data.
    # I don't know what that number is, so I'm going to make it up as 1000 records
    PARTITION_LIMIT = 1000

    def __init__(self, sort_key):
        '''
        Pull in B-1 pages worth of tuples data, quick sort,
        write out to a partition, repeat until EOF.

        Instantiate FileScan nodes over the partitions.

        A list holds the next values for each partion in memory
        '''
        super().__init__()
        self.sort_key = sort_key
        self.initialized = False

    def build_partitions_and_buffers(self):
        '''
        Retrieves input records into memory until a limit
        is reached, then sorts the records and writes them
        to a partion.

        Repeats until input is empty, then pulls the next
        from each partition into the "buffers" structure.
        '''
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
            self.buffers[scanner] = next(scanner)

        self.initialized = True

    def _get_next_from_buffer(self):
        '''
        Returns the lowest of the in memory values, and replaces it with
        the next value of the input that it came from.
        '''
        items = list(self.buffers.items())
        filtered_items = list(
            filter(lambda t: t[1] != self.EOF, items))
        sorted_items = sorted(
            filtered_items, key=lambda item: self.sort_key(item[1]))

        try:
            scanner_tuple = sorted_items.pop(0)
        except IndexError:
            return self.EOF
        scanner, _next = scanner_tuple
        self.buffers[scanner] = next(scanner)

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
        _input = self._inputs[0]
        eof = False
        values = []
        for ii in range(0, self.PARTITION_LIMIT):
            _next = next(_input)
            if _next == self.EOF:
                eof = True
                break
            values.append(_next)
        return eof, sorted(values, key=self.sort_key)

    def __next__(self):
        '''
        Returns the lowest of the in memory values.
        '''
        _input = self._inputs[0]
        if not self.initialized:
            self.build_partitions_and_buffers()

        _next = self._get_next_from_buffer()
        if _next is self.EOF:
            _input.__close__()

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
