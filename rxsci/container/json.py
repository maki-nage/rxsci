import typing
serialization_as_string = True
try:
    import orjson as json
    serialization_as_string = False
except Exception:
    import json
import rx
import rx.operators as ops

import rxsci as rs
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
            if len(i) > 0:
                i = json.loads(i)
            else:
                return None
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


def load_from_file(
    filename,
    lines=True, skip=0,
    ignore_error=False,
    encoding='utf-8',
    compression=None,
    open_obj=open,
):
    ''' Loads a json file.

    This factory loads the provided file. The format of the returned observable
    depends on the *lines* parameter.

    The open_obj function must return a file-like object. Its prototype is:
        open_obj(filename: str, mode: str, encoding: str) -> file-like object

    Args:
        filename: Path of the file to read or a file object
        lines: Parse file as a JSON Line when set to True, as a single JSON object otherwise.
        skip: [Optional] Number of lines to skip before parsing
        ignore_error: Ignore errors while parsing MessagePack
        encoding [Optional] Encoding used to parse the text content
        compression [Optional]: 'gzip' or 'zstd'
        open_obj: A function to open the source file.

    Returns:
        An observable of objects.
    '''
    compressions = {
        'gzip': rs.compression.z.decompress,
        'zstd': rs.compression.zstd.decompress,
    }
    pipe_ops = []

    # decompress
    if compression:
        pipe_ops.append(compressions[compression]())

    if lines is True:
        return file.read(filename, mode='rb', size=64*1024, open_obj=open_obj) \
            .pipe(*pipe_ops) \
            .pipe(
                rs.data.decode(encoding),
                line.unframe(),
                load(skip=skip, ignore_error=ignore_error),
        )
    else:
        return file.read(filename, size=-1, mode='rb', open_obj=open_obj) \
            .pipe(*pipe_ops) \
            .pipe(
                rs.data.decode(encoding),
                load(skip=skip, ignore_error=ignore_error),
        )


def dump(newline='\n'):
    ''' dumps an observable to JSON.

    If the source observable emits several items, then they are framed
    as JSON line.
    The source must be an Observable.

    Args:
        newline: [Optional] Character(s) used for end of line.

    Returns:
        An observable of string items, where each item is a JSON string.
    '''
    def _dump(source):
        def on_subscribe(observer, scheduler):

            def on_next(i):
                line = json.dumps(i)
                if serialization_as_string is False:
                    line = line.decode()
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
    encoding='utf-8',
    compression=None,
    open_obj=open,
):
    ''' dumps each item to a JSON file.

    The source must be an Observable.

    The open_obj function must return a file-like object. Its prototype is:
        open_obj(filename: str, mode: str, encoding: str) -> file-like object

    Args:
        filename: Path of the file to read or a file object
        newline: [Optional] Character(s) used for end of line.
        encoding [Optional] Encoding used to parse the text content
        open_obj: A function to open the source file.

    Returns:
        An empty observable that completes on success when the source
        observable completes or completes on error if there is an error
        while writing the csv file.
    '''
    compressions = {
        'gzip': rs.compression.z.compress,
        'zstd': rs.compression.zstd.compress,
    }

    def _dump_to_file(source):

        if compression:
            return source.pipe(
                dump(newline=newline),
                rs.data.encode(encoding),
                compressions[compression](),
                file.write(
                    file=filename,
                    mode='wb',
                    open_obj=open_obj,
                ),
            )
        else:
            return source.pipe(
                dump(newline=newline),
                rs.data.encode(encoding),
                file.write(
                    file=filename,
                    mode='wb',
                    open_obj=open_obj,
                ),
            )
    return _dump_to_file
