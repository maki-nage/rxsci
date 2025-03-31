import rx
from rx.scheduler import CurrentThreadScheduler
from rx.disposable import CompositeDisposable, Disposable


def read(file, mode='r', size=None, encoding=None, open_obj=open):
    ''' Reads the content of a file

        When provided, the open_obj function must have the following prototype:
        open(filename, mode, encoding)

    Args:
        file: The path of the file to read, or a file object
        mode: [Optional] how the file must be opened. either 'r' to read text or 'rb' to
            read binary
        size: [Optional] If set file if read by chunks of this size
        encoding: [Optional] The text encoding to use when reading in text mode
        open_obj: [Optional] A custom function used to open the provided file.

    Returns:
        An observable where each item is a chunk of data, or the whole
        file if no size has been set.
    '''
    def on_subscribe(observer, scheduler_):
        disposed = False
        _scheduler = scheduler_ or CurrentThreadScheduler.singleton()

        def _action(_, __):
            def read_data(f):
                if size is None:
                    data = f.read(size)
                    observer.on_next(data)
                else:
                    data = f.read(size)
                    while not disposed and len(data) > 0:
                        observer.on_next(data)
                        data = f.read(size)

            try:
                if type(file) is str:
                    with open_obj(file, mode, encoding=encoding) as f:
                        read_data(f)
                else:
                    read_data(file)

                observer.on_completed()

            except Exception as e:
                observer.on_error(e)

        def _dispose():
            nonlocal disposed
            disposed = True

        disp = Disposable(_dispose)
        return CompositeDisposable(_scheduler.schedule(_action), disp)

    return rx.create(on_subscribe)


def write(file, mode=None, encoding=None, open_obj=open):
    ''' Writes the content of a file

    When provided, the open_ibj function must have the following prototype:
        open(filename, mode, encoding)

    The source must be an Observable.

    Args:
        file: the path of the file to read
        mode: how the file must be opened. either 'r' to read text or 'rb' to
            read binary
        size: [Optional] If set file if read by chunks of this size
        encoding: [Optional] text encoding to use when reading in text mode
        open_obj: [Optional] A custom function used to open the provided file.

    Returns:
        An observable where eeach item is a chunk of data, or the while
        file if no size has been set.
    '''
    mode = mode or 'wb'

    def _write(source):
        def on_subscribe(observer, scheduler):
            try:
                f = file
                if type(file) is str:
                    f = open_obj(file, mode, encoding=encoding)

            except Exception as e:
                observer.on_error(e)

            def on_next(i):
                f.write(i)

            def on_completed():
                if type(file) is str:
                    f.close()
                observer.on_completed()

            def on_error(e):
                if type(file) is str:
                    f.close()
                observer.on_error(e)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=on_error,
                scheduler=scheduler,
            )

        return rx.create(on_subscribe)

    return _write
