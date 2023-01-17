import typing
import json
import csv
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


def parse_int(i):
    if len(i) == 0:
        return None
    return int(i)


def parse_float(i):
    return float(i)


def parse_decimal(ii):
    if len(ii) == 0:
        return None
    try:
        s = ii.split(".")
        i = int(s[0])
        if len(s) > 1:
            r = int(s[1])
            r = r / (10 ** len(s[1]))
        else:
            r = 0

        return float(i) + r
    except Exception:
        #print("parse error on {}: {}".format(ii, e))
        return float(ii)


def type_parser(type_repr):
    if type_repr in ['int', int]:
        return parse_int
    if type_repr in ['float', float]:
        return parse_decimal
        #return parse_float
    elif type_repr in ['bool', bool]:
        return lambda i: i == 'True'
    elif type_repr == 'posix_timestamp':
        return lambda i: datetime.fromtimestamp(int(i), tz=timezone.utc)
    elif type_repr == 'iso_datetime':
        return parse_iso_date
    elif type_repr in ['str', str]:
        return lambda i: i
    else:
        raise TypeError("unknown column type: {}".format(type_repr))


def create_schema_factory(dtype, schema_name='x'):
    if not isinstance(dtype, list):
        columns = [f for f in dtype._fields]
        types = [dtype.__annotations__[f] for f in dtype._fields]
        Item = dtype
    else:
        columns = [t[0] for t in dtype]
        types = [i[1] for _, i in enumerate(dtype)]
        Item = namedtuple(schema_name, columns)
    globals()[Item.__name__] = Item
    return Item, columns, types


class CsvDataFile():
    def __init__(self):
        self.data = None

    def set_data(self, i):
        self.data = i

    def __iter__(self):
        return self

    def __next__(self):
        return self.data


def merge_escape_parts(parts, separator, escapechar):
    try:
        merged_parts = []
        agg = None
        for t in parts:
            if t == '"':
                if agg is None:
                    agg = ['"']
                else:
                    agg.append('"')
                    merged_parts.append(separator.join(agg))
                    agg = None
            elif len(t) > 0 and t[0] == '"' and t[-1] == '"' and t[-2] != escapechar and agg is None:
                merged_parts.append(t)
            elif len(t) > 0 and t[-1] == '"' and t[-2] != escapechar and agg is not None:
                agg.append(t)
                merged_parts.append(separator.join(agg))
                agg = None
            elif len(t) > 0 and t[0] == '"' and agg is None:
                agg = [t]
            elif agg is not None:
                agg.append(t)
            else:
                merged_parts.append(t)

        return merged_parts
    except Exception as e:
        print(e)
        print(parts)
        print(merged_parts)
        raise e


def create_line_parser(
    dtype, none_values=[],
    separator=",", escapechar="\\",
    ignore_error=False, schema_name='x'
):
    ''' creates a parser for csv lines

    Args:
        dtype: A list of (name, type) tuples, or a typing.NamedTuple class.
        none_values: [Optional] Values to consider as None values
        separator: [Optional] Token used to separate each columns
        ignore_error: [Optional] when set to True, any line that does not
            match the provided number of columns raise an error an stop
            the parsing. When set to False, error lines are skipped.

    Returns:
        A Parsing function, that can parse text lines as specified in the
        parameters.
    '''
    Item, columns, types = create_schema_factory(dtype, schema_name)
    columns_parser = [type_parser(t) for t in types]
    columns_len = len(columns)

    #csv_file = CsvDataFile()
    #reader = csv.reader(csv_file)

    def parse_column(index, i):
        return columns_parser[index](i)

    def split(line, separator):
        return line.split(separator)

    """
    def parse_line_as_csv(line):
        csv_file.set_data(line)
        parts = next(reader)
        if len(parts) != columns_len:
            error = "invalid number of columns: expected {}, found {} on: {}".format(
                columns_len, len(parts), line)
            if ignore_error is True:
                print(error)
                return None
            else:
                raise ValueError(error)

        for index, i in enumerate(parts):
            if i in none_values:
                parts[index] = None
            else:
                parts[index] = parse_column(index, i)
        return Item(*parts)
        #return parts

    def parse_line_as_json(line):
        line = "[{}]".format(line)
        parts = json.loads(line)
        if len(parts) != columns_len:
            error = "invalid number of columns: expected {}, found {} on: {}".format(
                columns_len, len(parts), line)
            if ignore_error is True:
                print(error)
                return None
            else:
                raise ValueError(error)

        return Item(*parts)
        #return parts
    """

    def parse_line(line):
        try:
            parts = split(line, separator)
            if len(parts) != columns_len:
                parts = merge_escape_parts(parts, separator, escapechar)
                if len(parts) != columns_len:
                    error = "invalid number of columns: expected {}, found {} on: {}".format(
                        columns_len, len(parts), line)
                    raise ValueError(error)

            for index, i in enumerate(parts):
                if len(i) > 0 and i[0] == '"' and i[-1] == '"':
                    i = i[1:-1]
                    i = i.replace(f'{escapechar}"', '"')
                if i in none_values:
                    parts[index] = None
                else:
                    parts[index] = parse_column(index, i)

        except Exception as e:
            if ignore_error is True:
                print("{}, \nignoring this line".format(e))
                return None
            else:
                raise e
        #return item(*parsed_parts)
        return Item(*parts)
        #return parts

    #if separator == ',':
    #    print("parsing as json")
    #    return parse_line_as_json
    return parse_line
    #return parse_line_as_csv


def load(parse_line, skip=0):
    ''' Loads a csv observable.

    The source observable must emit one csv row per item
    The source must be an Observable.

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


def load_from_file(filename, parse_line, skip=1, encoding=None):
    ''' Loads a csv file.

    This factory loads the provided file and returns its content as an
    observable emitting one item per line.

    Args:
        filename: Path of the file to read or a file object
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


def dump(header=True, separator=",", escapechar="\\", newline='\n'):
    ''' dumps an observable to csv.

    The source must be an Observable.

    Args:
        header: [Optional] indicates whether a header line must be added.
        separator: [Optional] Token used to separate each columns.
        newline: [Optional] Character(s) used for end of line.

    Returns:
        An observable string items, where each item is a csv line.
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

                ii = []
                for f in i:
                    if type(f) not in [int, float, bool, str, type(None)]:
                        f = str(f)
                    if type(f) is str:
                        f = f.replace('"', f'{escapechar}"')
                        f = '"{}"'.format(f)
                    elif f is None:
                        f = ''
                    else:
                        f = str(f)
                    ii.append(f)

                line = separator.join(ii)
                line += newline
                observer.on_next(line)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )
        return rx.create(on_subscribe)

    return _dump


def dump_to_file(
    filename, header=True,
    separator=",", escapechar="\\",
    newline='\n', encoding=None
):
    ''' dumps each item to a csv file.

    The source must be an Observable.

    Args:
        filename: Path of the file to read or a file object
        header: [Optional] indicates whether a header line must be added.
        separator: [Optional] Token used to separate each columns.
        newline: [Optional] Character(s) used for end of line.
        encoding [Optional] Encoding used to parse the text content

    Returns:
        An empty observable that completes on success when the source
        observable completes or completes on error if there is an error
        while writing the csv file.
    '''
    def _dump_to_file(source):
        mode = None
        if encoding is not None:
            mode = 'wb'
        return source.pipe(
            dump(
                header=header,
                separator=separator, escapechar=escapechar,
                newline=newline
            ),
            ops.map(lambda i: i.encode(encoding) if encoding is not None else i),
            file.write(
                file=filename,
                mode=mode,
            ),
        )

    return _dump_to_file
