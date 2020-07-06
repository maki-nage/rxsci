import rx
import rx.operators as ops
from rx.disposable import CompositeDisposable


def connect_on_subscribe(connectable):
    def _connect_on_subscribe(source):
        def subscribe(observer, scheduler):
            disposable = source.subscribe(observer, scheduler=scheduler)
            disposable2 = connectable.connect()
            return CompositeDisposable(disposable2, disposable)
        return rx.create(subscribe)

    return _connect_on_subscribe


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
    def _tee_map(source):
        connectable = source.pipe(
            ops.publish()
        )

        return rx.zip(*[arg(connectable) for arg in args]).pipe(
            connect_on_subscribe(connectable),
        )

    return _tee_map
