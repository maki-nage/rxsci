import rx
from rx.disposable import CompositeDisposable
import rxsci as rs


def mux_observable():
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


def demux_observable():
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


def demux_mux_observable(outer_group):
    def _demux(source):        
        def on_subscribe(observer, scheduler):            
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    observer.on_next(rs.OnNextMux(i.key[1], i.item))
                '''
                elif type(i) is rs.OnCreateMux:
                    observer.on_next(rs.OnCreateMux(i.key[1]))
                elif type(i) is rs.OnCompletedMux:
                    observer.on_next(rs.OnCompletedMux(i.key[1]))
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key[1], i.error))
                else:
                    observer.on_next(TypeError("flatten_aggregate: unknow item type: {}".format(type(i))))
                '''

            def on_next_outer(i):
                observer.on_next(i)

            disposable = CompositeDisposable()
            disposable.add(outer_group.subscribe(
                on_next=on_next_outer,
                scheduler=scheduler,                
            ))
            disposable.add(source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            ))
            return disposable
        return rx.create(on_subscribe)

    return _demux


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
        mux_observable(),
        pipeline,
        demux_observable(),
    )
