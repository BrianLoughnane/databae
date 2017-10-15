from executor.nodeIterator import Iterator

class IndexScan(Iterator):
    '''
    Given some in memory data structure,
    build a b+ tree over it so that reads don't
    have to iterate over every record.
    '''
    def __init__(self, tree, operand):
        super().__init__()
        self.tree = tree
        self.operand = operand

    def __next__(self):
        '''
        This is kind of like selection.  The leaf
        nodes should be a linked list of things
        containing arrays (representing pages of
        data)
        '''
# MemIndexScan
#        Start recursion here.  Input is page.
#          Start with the root page
#        If page is leaf:
#          for record in page:
#             if theta:
#               yield record
#             condition is ==
#               if val > operand, break
#             condition is >, >=
#               if not next page, break
#               else page = next page
#             condition is <, <=
#               if val =, > operand, break
#           return EOF
#        for each key, pointer tuple in page:
#          if key > operand
#              recurse(previous pointer)
#
#
#
# FileIndexScan
#
#
#        open the index file
#        seek to address of root page
#        read address to determine location
#        seek to location of root page
#
#        Start recursion here.  Input is "page",
#          which in this case is some number of bytes
#
#        Starting with the root "page"
#        If address points to another file?
#        (not sure how we know this^^)
#          Open other file, seek to address
#          for record in page:
#          (where record is a bunch
#          of bytes and page is a group of these
#          bunches of bytes...)
#             read record
#             if theta(record):
#               yield record
#             condition is ==
#               if val > operand, break, close file
#             condition is >, >=
#               if not next page, break, close file
#               else page = next page
#               TODO I'm not sure how the actual data
#               keeps state of where the next page is
#               seems like tight coupling to put index
#               info in the data
#             condition is <, <=
#               if val =, > operand, break, close file
#           return EOF

#        for each value, address in page:
#        (where key is value and pointer is an
#        address to elsewhere in the file)
#          if value > operand
#              recurse(previous address)



# so the file will have a bunch of binary encoded values and addresses to elsewhere in the page.  That elsewhere will also be just a bunch of binary encoded values and addresses.  Eventually an address will point to another file, which is the heap file containing the actual data.
#
#
#
#
#
#
#
#
#
