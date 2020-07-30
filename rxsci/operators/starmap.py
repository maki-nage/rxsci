import rx.operators as ops
import rxsci as rs
from .map import map as map


def map_mux(mapper):
    def _map(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        ii = mapper(i.item)
                        observer.on_next(rs.OnNextMux(i.key, ii))
                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e))
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _map


def starmap(mapper):
    return map(lambda i: mapper(*i))
