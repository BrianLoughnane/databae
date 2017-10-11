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
        self.file_connection.close()

    def reset(self):
        self.file_connection = open(
          self.filename, 'r')
        self._iterator = csv.reader(
          self.file_connection, delimiter=',')

