from collections import namedtuple
from datetime import datetime, timezone
from dateutil.parser import isoparse

import rx
import rx.operators as ops
import rxsci.io.file as file
import rxsci.framing.line as line


def parse_iso_date(i):
    try:
        return isoparse(i)
    except ValueError as e:
        raise ValueError("{}: {}".format(e, i))


def type_parser(type_repr):
    if type_repr in ['int']:
        return int
    if type_repr in ['float']:
        return float
    elif type_repr in ['bool']:
        return lambda i: i == 'True'
    elif type_repr == 'posix_timestamp':
        return lambda i: datetime.fromtimestamp(int(i), tz=timezone.utc)
    elif type_repr == 'iso_datetime':
        return parse_iso_date
    elif type_repr == 'str':
        return lambda i: i
    else:
        raise TypeError("unknown column type: {}".format(type_repr))


def create_line_parser(dtype, none_values=[], separator=",",
                       ignore_error=False):
    ''' creates a parser for csv lines

    Args:
        dtype: A list of (name, type) tuples.
        none_values: [Optional] Values to consider as None values
        separator: [Optional] Token used to separate each columns
        ignore_error: [Optional] when set to True, any line that does not
            match the provided number of columns raise an error an stop
            the parsing. When set to False, error lines are skipped.
    '''
    columns = [t[0] for t in dtype]
    columns_parser = [type_parser(i[1]) for index, i in enumerate(dtype)]
    columns_len = len(columns)
    item = namedtuple('x', columns)

    def parse_line(line):
        parts = line.split(separator)
        if len(parts) != columns_len:
            error = "invalid number of columns: expected {}, found {} on: {}".format(
                columns_len, len(parts), line)
            if ignore_error is True:
                print(error)
                return None
            else:
                raise ValueError(error)

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
            ops.filter(lambda i: i is not None),
        )

    return _load


def load_from_file(filename, parse_line, skip=1, encoding=None,
                   transport_params=None):
    ''' Loads a csv file.

    This factory loads the provided file and returns its content as an
    observable emitting one item per line.

    Args:
        filename: Path of the file to read
        parse_line: A line parser, e.g. created with create_line_parser
        skip: [Optional] Number of lines to skip before parsing
        encoding [Optional] Encoding used to parse the text content
        transport_params: [Optional] When smart-open is used, then this
            parameter is used to provide additional configuration information

    Returns:
        An observable of namedtuple items, where each key is a csv column
    '''

    return file.read(
        filename, size=64*1024, encoding=encoding,
        transport_params=transport_params).pipe(
        line.unframe(),
        load(parse_line, skip=skip),
    )


def dump(header=True, separator=",", newline='\n'):
    ''' dumps an observable to csv.

    The source observable must emit one csv row per item

    Args:
        header: [Optional] indicates whether a header line must be added.
        separator: [Optional] Token used to separate each columns.
        newline: [Optional] Character(s) used for end of line.

    Returns:
        An observable of namedtuple items, where each key is a csv column.
    '''
    def _dump(source):
        def on_subscribe(observer, scheduler):
            first = True

            def on_next(i):
                nonlocal first
                if first is True and header is True:
                    first = False
                    columns = i._fields
                    columns = separator.join([str(c) for c in columns])
                    columns += newline
                    observer.on_next(columns)

                line = separator.join([str(f) for f in i])
                line += newline
                observer.on_next(line)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rx.create(on_subscribe)

    return _dump


def dump_to_file(filename, header=True, separator=",",
                 newline='\n', encoding=None,
                 transport_params=None):
    ''' dumps each item to a csv file.

    Args:
        filename: Path of the file to read
        header: [Optional] indicates whether a header line must be added.
        separator: [Optional] Token used to separate each columns.
        newline: [Optional] Character(s) used for end of line.
        encoding [Optional] Encoding used to parse the text content
        transport_params: [Optional] When smart-open is used, then this
            parameter is used to provide additional configuration information

    Returns:
        An empty observable that completes on success when the source
        observable completes or completes on error if there is an error
        while writing the csv file.
    '''
    def _dump_to_file(source):
        print("dump transport params: {}".format(transport_params))
        mode = None
        if encoding is not None:
            mode = 'wb'
        return source.pipe(
            dump(header=header, separator=separator, newline=newline),
            ops.map(lambda i: i.encode(encoding) if encoding is not None else i),
            file.write(
                file=filename,
                mode=mode,
                transport_params=transport_params
            ),
        )

    return _dump_to_file
