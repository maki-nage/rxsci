import rx
import rx.operators as ops


def on_subscribe(post_action):
    def _on_subscribe(source):
        def subscribe(observer, scheduler):
            disposable = source.subscribe(observer, scheduler=scheduler)
            post_action()
            return disposable
        return rx.create(subscribe)

    return _on_subscribe


def tee_map(*args):
    '''Processes several operators chains simultaneously on the same source
    observable. This operator allows to do multiple processing on the same
    source, and combine the results as a single tuple object.

    Args:
        args: list of operators that will process the source observable
            concurrently

    Returns:
        An observable containing tuples of the items emitted by each branch of
        the tee.
    '''
    tee_args = args

    def _tee_map(source):
        def connect():
            connectable.connect()

        connectable = source.pipe(
            ops.publish()
        )

        args = [arg(connectable) for arg in tee_args]
        return rx.zip(*args).pipe(
            on_subscribe(connect),
        )

    return _tee_map
