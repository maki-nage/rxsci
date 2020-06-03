import rx
try:
    from smart_open import open
    #import boto3
    #import logging
    #boto3.set_stream_logger('botocore.endpoint', logging.DEBUG)

    #logger = logging.getLogger('smart_open')
    #logger.setLevel(logging.DEBUG)

    #handler = logging.StreamHandler()
    #handler.setLevel(logging.DEBUG)
    #logger.addHandler(handler)
    #requests_logger.addHandler(handler)
except Exception:
    pass


def read(file, mode='r', size=None, encoding=None, transport_params=None):
    ''' Reads the content of a file

    Args:
        file: the path of the file to read
        mode: how the file must be opened. either 'r' to read text or 'rb' to
            read binary
        size: [Optional] If set file if read by chunks of this size
        encoding: [Optional] text encoding to use when reading in text mode
        transport_params: [Optional] When smart-open is used, then this
            parameter is used to provide additional configuration information

    Returns:
        An observable where eeach item is a chunk of data, or the while
        file if no size has been set.
    '''
    def on_subscribe(observer, scheduler):
        try:
            kwargs = {}
            if transport_params is not None:
                kwargs['transport_params'] = {
                    'resource_kwargs': transport_params,
                }
            with open(file, mode, encoding=encoding, **kwargs) as f:
                if size is None:
                    data = f.read(size)
                    observer.on_next(data)
                    observer.on_completed()
                else:
                    data = f.read(size)
                    while len(data) > 0:
                        observer.on_next(data)
                        data = f.read(size)

                    observer.on_completed()

        except Exception as e:
            observer.on_error(e)

    return rx.create(on_subscribe)


def write(file, mode='wb', encoding=None, transport_params=None):
    ''' Writes the content of a file

    Args:
        file: the path of the file to read
        mode: how the file must be opened. either 'r' to read text or 'rb' to
            read binary
        size: [Optional] If set file if read by chunks of this size
        encoding: [Optional] text encoding to use when reading in text mode
        transport_params: [Optional] When smart-open is used, then this
            parameter is used to provide additional configuration information

    Returns:
        An observable where eeach item is a chunk of data, or the while
        file if no size has been set.
    '''
    def _write(source):
        def on_subscribe(observer, scheduler):
            kwargs = {}
            if transport_params is not None:
                kwargs['transport_params'] = {
                    'resource_kwargs': transport_params,
                    'min_part_size': 10 * 1024**2
                }
            try:
                f = open(file, mode, encoding=encoding, **kwargs)
            except Exception as e:
                observer.on_error(e)

            def on_next(i):
                f.write(i)

            def on_completed():
                f.close()
                observer.on_completed()

            def on_error(e):
                f.close()
                observer.on_error(e)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=on_error,
            )

        return rx.create(on_subscribe)

    return _write
