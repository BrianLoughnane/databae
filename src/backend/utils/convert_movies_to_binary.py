import sys
import struct
import csv

# what is the schema of the movies table?
# 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
# int, varchar(), varchar()
# s     s           s

def typed(row):
    casts = [int, str, str]
    for index, value in enumerate(row):
        row[index] = casts[index](value)
    return row

class Writer(object):
    def write(filename):
        with open(filename, 'r') as fin,
                open('%s.table' % filename, 'wb') as out:

            reader = csv.reader(fin, delimiter=',')

            # pop off headers
            next(reader)

            for line in reader:
                vals = typed(list(line))
                out_line = struct.pack('IxPxP', *vals)
                out.write(out_line)

# filename = sys.argv[0]
# write_it(filename)

from utils.convert_movies_to_binary import Writer
