import csv

from executor.nodeIterator import Iterator

# lines implementation - automatic (python handles IO)

class FileScan(Iterator):
    # python returns empty string for end of file
    END_OF_FILE = ''

    def __init__(self, file_name):
        self.file_name = file_name
        self.num_reads = 0

    def __next__(self):
        '''
        The magical python will only go to disc if the next
        line is not already in the buffer pool.

        Another piece of magic is that once we're past block 1,
        calling next over all of those records (in the for loop)
        won't take up memory because they are not returned to a variable.
        '''
        with open(self.file_name, 'r') as _file:
            reader = csv.reader(_file, delimiter=',')

            for line_number in range(0, self.num_reads):
                try:
                    next(reader)
                except StopIteration:
                    return self.EOF

            try:
                next_line = next(reader)
            except StopIteration:
                return self.EOF

            self.num_reads += 1
            return next_line

    def __close__(self):
        pass

    def get_schema(self):
        with open(self.file_name, 'r') as _file:
            reader = csv.reader(_file, delimiter=',')
            return next(reader)

# lines manual implementation

# class FileScan(Iterator):
    # '''
    # Opens up a file and passes Nb lines of the file
    # up to the parent from it's .next() method, where
    # Nb is the number of lines that a single block of
    # disk can hold.

    # my_table = Scan('filename')
    # '''
    # LINE_READS = 1000

    # def __init__(self, file_name):
        # self.file_name = file_name
        # self.reads = 0
        # self._finished = False

        # self._generate_lines()

    # def _generate_lines(self):
        # start_position = self.reads * self.LINE_READS
        # end_position = start_position + self.LINE_READS
        # _range = set(range(start_position, end_position))

        # lines = []
        # with open(self.file_name, 'r') as _file:
            # line_number = 0
            # while True:
                # if line_number > end_position:
                    # break

                # line = _file.readline()

                # if line == END_OF_FILE:
                    # lines.append(self.EOF)
                    # break

                # if line_number in _range:
                    # lines.append(line)

                # line_number += 1

        # self.reads += 1
        # self.lines = lines

    # def __next__(self):
        # if self._finished:
            # return END_OF_FILE

        # line = self.lines.pop(0)
        # if not self.lines:
            # if line is END_OF_FILE:
                # self._finished = True
            # else:
                # self._generate_lines()
        # return self._parse(line)

    # def _parse(self, line):
        # data = line.split('\n')[0]
        # parsed = re.split(r',(?!\s)', data)
        # return parsed

    # def __close__(self):
        # pass


# TODO -- bytes implementation -- should have the same API as lines impl

# class FileScan(Iterator):
    # '''
    # Opens up a file and passes Nb lines of the file
    # up to the parent from it's .next() method, where
    # Nb is the number of lines that a single block of
    # disk can hold.

    # my_table = Scan('filename')
    # '''
    # BYTE_READS = 1000

    # def __init__(self, file_name):
        # self.file_name = file_name
        # self.reads = 0

    # def __next__(self):
        # start_position = self.reads * self.BYTE_READS
        # with open(self.file_name, 'r') as _file:
            # _file.seek(start_position)
            # return _file.read(self.BYTE_READS)

    # def __close__(self):
        # self._file.close()

