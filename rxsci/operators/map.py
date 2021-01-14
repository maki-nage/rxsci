import rxsci as rs
import rx.operators as ops


def map_mux(mapper):
    def _map(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        ii = mapper(i.item)
                        observer.on_next(i._replace(item=ii))
                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e, i.store))
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


def map(mapper):
    """Projects each element of an observable into a new form.

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: map

        ---1---2---3---4--->
        [   map(i: i*2)    ]
        ---2---4---6---8--->

    Args:
        mapper: A transform function to apply to each source element.

    Returns:
        An observable emitting the result of invoking the transform function on
        each source items.
    """
    def _map(source):
        if isinstance(source, rs.MuxObservable):
            return map_mux(mapper)(source)
        else:
            return ops.map(mapper)(source)

    return _map
