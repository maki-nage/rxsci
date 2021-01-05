import rxsci as rs
import rx
import rx.operators as ops


def flat_map_mux():
    def _flat_map(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:                    
                        for ii in i.item:
                            observer.on_next(i._replace(item=ii))
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _flat_map


def flat_map():
    """Projects each element of iterable source items as a new item.

    .. marble::
        :alt: flat_map

        --1,2,3-4,5,6-|
        [ flat_map()  ]
        --1-2-3-4-5-6-|

    Sources:
        An Observable or a MuxObservable

    Returns:
        An observable whose items are the result of
        iterating on each items of the source observable.
    """
    def _flat_map(source):
        if isinstance(source, rs.MuxObservable):
            return flat_map_mux()(source)
        else:
            return ops.flat_map(lambda i: rx.from_(i))(source)

    return _flat_map
