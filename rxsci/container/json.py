import typing
import json

import rx
import rx.operators as ops
import rxsci.io.file as file
import rxsci.framing.line as line


def load(skip=0, ignore_error=False):
    ''' Loads a json observable.

    The source observable must emit one JSON string per item
    The source must be an Observable.

    Args:
        skip: number of items to skip before parsing
        ignore_error: Ignore errors while parsing JSON

    Returns:
        An observable of dicts corresponding to the source json content.
    '''
    def load_json(i):
        try:
            i = json.loads(i)
        except Exception as e:
            if ignore_error is True:
                print(f"{e}: Ignoring this object.")
                return None
            else:
                raise e
        return i

    def _load(source):
        return source.pipe(
            ops.skip(skip),
            ops.map(load_json),
            ops.filter(lambda i: i is not None),
        )

    return _load


def load_from_file(filename, lines=True, skip=0, encoding=None):
    ''' Loads a json file.

    This factory loads the provided file. The format of the returned observable
    depends on the *lines* parameter.

    Args:
        filename: Path of the file to read or a file object
        lines: Parse file as a JSON Line when set to True, as a single JSON object otherwise.
        skip: [Optional] Number of lines to skip before parsing
        encoding [Optional] Encoding used to parse the text content

    Returns:
        An observable of namedtuple items, where each key is a csv column
    '''

    if lines is True:
        return file.read(filename, size=64*1024, encoding=encoding).pipe(
            line.unframe(),
            load(skip=skip),
        )
    else:
        return file.read(filename, encoding=encoding).pipe(
            load(skip=skip),
        )


def dump(newline='\n'):
    ''' dumps an observable to JSON.

    If several the source observable emits several items, then they are framed
    as JSON line.
    The source must be an Observable.

    Args:
        newline: [Optional] Character(s) used for end of line.

    Returns:
        An observable string items, where each item is a csv line.
    '''
    def _dump(source):
        def on_subscribe(observer, scheduler):

            def on_next(i):
                line = json.dumps(i)
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
    filename,
    newline='\n',
    encoding=None
):
    ''' dumps each item to a JSON file.

    The source must be an Observable.

    Args:
        filename: Path of the file to read or a file object
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
            dump(newline=newline),
            ops.map(lambda i: i.encode(encoding) if encoding is not None else i),
            file.write(
                file=filename,
                mode=mode,
            ),
        )

    return _dump_to_file
