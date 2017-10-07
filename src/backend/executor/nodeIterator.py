class Iterator():
    EOF = 'end of fun'

    def __init__(self):
        raise ValueError('Not Implemented')

    def __next__(self):
        raise ValueError('Not Implemented')

    def __close__(self):
        raise ValueError('Not Implemented')

    @staticmethod
    def parse_args(schema, args):
        return args

    @staticmethod
    def parse_schema(schema, args):
        return schema

