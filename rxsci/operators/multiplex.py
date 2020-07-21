import rx
import rxsci as rs


def _mux():
    def __mux(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                observer.on_next(rs.OnNextMux(None, i))

            def on_error(e):
                observer.on_next(rs.OnErrorMux(None, e))
                observer.on_error(e)

            def on_completed():
                observer.on_next(rs.OnCompletedMux(None))
                observer.on_completed()

            observer.on_next(rs.OnCreateMux(None))
            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=on_error,
                scheduler=scheduler,
            )
        return rs.MuxObservable(on_subscribe)

    return __mux


def _demux():
    def _flatten(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    observer.on_next(i.item)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )
        return rx.create(on_subscribe)

    return _flatten


def multiplex(pipeline):
    '''Transforms an Observable to a MuxObservable

    All operations done in pipeline operate on MuxObservable.

    Args:
        pipeline: The pipeline that will process the multiplexed items.

    Returns:
        An Observable where the source items have been multiplexed to a
        MuxObservable, then processed by the pipeline transorfations, and
        finally de-multiplexed to an Observable.
    '''
    return rx.pipe(
        _mux(),
        pipeline,
        _demux(),
    )
