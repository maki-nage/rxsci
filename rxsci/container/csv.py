from collections import namedtuple
from datetime import datetime, timezone
from dateutil.parser import isoparse

import rx.operators as ops
from rx.scheduler import ImmediateScheduler
import rxsci.io.file as file
import rxsci.framing.line as line


def type_parser(type_repr):
    if type_repr in ['int']:
        return int
    elif type_repr in ['bool']:
        return lambda i: i == 'True'
    elif type_repr == 'posix_timestamp':
        return lambda i: datetime.fromtimestamp(int(i), tz=timezone.utc)
    elif type_repr == 'iso_datetime':
        return lambda i: isoparse(i)
    else:
        return lambda i: i


def create_line_parser(dtype, none_values=[], separator=","):
    ''' creates a parser for csv lines

    Args:
        dtype: A list of (name, type) tuples.
        none_values: [Optional] Values to consider as None values
        separator: [Optional] Token used to separate each columns
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

        parsed_parts = []
        for index, i in enumerate(parts):
            if i in none_values:
                parsed_parts.append(None)
            else:
                parsed_parts.append(columns_parser[index](i))

        return item(*parsed_parts)

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


def load_from_file(filename, parse_line, skip=1, encoding=None):
    ''' Loads a csv file.

    This factory loads the provided file and returns its content as an
    observable emitting one item per line.

    Args:
        filename: Path of the file to read
        parse_line: A line parser, e.g. created with create_line_parser
        skip: [Optional] Number of lines to skip before parsing
        encoding [Optional] Encoding used to parse the text content

    Returns:
        An observable of namedtuple items, where each key is a csv column
    '''

    return file.read(filename, size=64*1024, encoding=encoding).pipe(
        line.unframe(),
        load(parse_line, skip=skip),
    )
