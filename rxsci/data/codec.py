import codecs

import rx


def encode(encoding='utf8', incremental=True):
    '''Encode strings to bytes

    Args:
        encoding: [Optional] The encoding to use.
        incremental: [Optional] When True, encode all items incrementaly,
            otherwise encode them as independent data.

    Returns:
        An Observable of bytes.
    '''
    def _encode(source):
        def on_subscribe(observer, scheduler):
            if incremental:
                encoder = codecs.getincrementalencoder(encoding)()
            
            def on_next(i):
                if incremental:
                    data = encoder.encode(i)
                else:
                    data = i.encode(encoding)
                observer.on_next(data)

            def on_completed():
                if incremental:
                    data = encoder.encode('', final=True)
                    observer.on_next(data)
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)
    
    return _encode


def decode(encoding='utf8', incremental=True):
    '''Decode bytes to strings

    Args:
        encoding: [Optional] The encoding to use.
        incremental: [Optional] When True, decode all items incrementaly,
            otherwise decode them as independent data.

    Returns:
        An Observable of strings.
    '''
    def _decode(source):
        def on_subscribe(observer, scheduler):
            if incremental:
                decoder = codecs.getincrementaldecoder(encoding)()
            
            def on_next(i):
                if incremental:
                    data = decoder.decode(i)
                else:
                    data = i.decode(encoding)
                observer.on_next(data)

            def on_completed():
                if incremental:
                    data = decoder.decode(b'', final=True)
                    observer.on_next(data)
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)
    
    return _decode