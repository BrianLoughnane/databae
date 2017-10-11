import csv

from executor.nodeIterator import Iterator

# lines implementation - automatic (python handles IO)

class FileScan(Iterator):
    # python returns empty string for end of file
    END_OF_FILE = ''

    def __init__(self, filename):
        self.filename = filename
        self.reset()

    def __next__(self):
        '''
        The magical python will only go to disc if the next
        line is not already in the buffer pool.
        '''
        try:
            next_line = next(self._iterator)
        except StopIteration:
            return self.EOF
        return next_line

    def __close__(self):
        self._file.close()

    def reset(self):
        file_connection = open(self.filename, 'r')
        self._iterator = csv.reader(
          file_connection, delimiter=',')

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
