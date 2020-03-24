from collections import namedtuple
import rx.operators as ops
from rx.scheduler import ImmediateScheduler
import rxsci.io.file as file
import rxsci.framing.line as line


def type_parser(type_repr):
    if type_repr in ['int']:
        return int
    elif type_repr in ['bool']:
        return lambda i: i == 'True'
    else:
        return lambda i: i


def create_line_parser(dtype, separator=","):
    ''' creates a parser for csv lines

    Args:
        dtype: A list of (name, type) tuples.
        separator: Token used to separate each columns
    '''
    columns = [t[0] for t in dtype]
    columns_parser = [type_parser(i[1]) for index, i in enumerate(dtype)]
    columns_len = len(columns)
    item = namedtuple('x', columns)

    def parse_line(line):
        parts = line.split(separator)
        if len(parts) != columns_len:
            raise ValueError(
                "invalid number of columns: expected {}, found {} on: {}".format(
                    columns_len, len(parts), line)
            )

        parts = [columns_parser[index](i) for index, i in enumerate(parts)]
        return item(*parts)

    return parse_line


def load(parse_line, skip=0):
    ''' Loads a csv observable.

    The source observable must emit one csv row per item

    Args:
        parse_line: A line parser, e.g. created with create_line_parser
        skip: number of items to skip before parsing

    Returns:
        An observable of namedtuple items, where each key is a csv column
    '''
    def _load(source):
        return source.pipe(
            ops.skip(skip),
            ops.map(parse_line),
        )

    return _load


def load_from_file(filename, parse_line, skip=1):
    ''' Loads a csv file.

    This factory loads the provided file and returns its content as an
    observable emitting one item per line.

    Args:
        parse_line: A line parser, e.g. created with create_line_parser
        skip: number of lines to skip before parsing

    Returns:
        An observable of namedtuple items, where each key is a csv column
    '''

    return file.read(filename, size=64*1024).pipe(
        line.unframe(),
        load(parse_line, skip=skip),
    )
