class Iterator():
    EOF = 'end of fun'

    def __init__(self):
        self._inputs = []

    def __next__(self):
        raise ValueError('Not Implemented')

    def __close__(self):
        raise ValueError('Not Implemented')

    def __iter__(self):
        return self

    @staticmethod
    def parse_args(schema, args):
        '''
        Use this static method to transform the arguments that will
        be passed to the constructor by the executor.

        i.e. if schema = ['id', 'name', 'height'] and args = ['id', 'name']
        you may want to return a lambda that will select the row values based
        on the indices of the args in the schema like so:
            indices = [schema.index(arg) for arg in args]
            return lambda row: [row[ii] for ii in indices]
        '''
        return args

    @staticmethod
    def parse_schema(schema, args):
        '''
        Use this static method to return any schema transformations
        that will occur by including this node in a pipeline.

        i.d. for `select name from movies;`, we may go from a schema
        of ('id', 'name', 'year', 'rating') to ('name')
        '''
        return schema

