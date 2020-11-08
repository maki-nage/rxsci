import rxsci as rs
import rx.operators as ops


def filter_mux(predicate):
    def _filter(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        emit = predicate(i.item)
                        if emit is True:
                            observer.on_next(i)
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
    return _filter


def filter(predicate):
    """Filters the items of an observable sequence based on a
    predicate.

    .. marble::
        :alt: filter

        ----1---2---3---4---|
        [   filter(i: i>2)  ]
        ------------3---4---|

    Examples:
        >>> rs.operators.filter(lambda value: value < 10)

    Args:
        predicate: A function to test each source item for a
            condition.

    Source:
        An Observable or a MuxObservable.

    Returns:
        An observable that emits items from the
        source observable that satisfy the condition. The type of the returned
        observable is the same than the source observable.
    """

    def _filter(source):
        if isinstance(source, rs.MuxObservable):
            return filter_mux(predicate)(source)
        else:
            return ops.filter(predicate)(source)

    return _filter
