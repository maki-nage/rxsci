import rx
import rxsci as rs
import rx.operators as ops


def scan_mux(accumulator, seed, reduce):
    def _scan(source):
        def on_subscribe(observer, scheduler):
            state = {}

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        acc = accumulator(state[i.key], i.item)
                        state[i.key] = acc
                        if reduce is False:
                            observer.on_next(rs.OnNextMux(i.key, acc))
                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e))
                elif type(i) is rs.OnCreateMux:
                    state[i.key] = seed
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    if reduce is True:
                        observer.on_next(rs.OnNextMux(i.key, state[i.key]))
                    observer.on_next(i)
                    del state[i.key]
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key, i.error))
                    del state[i.key]
                else:
                    observer.on_next(TypeError("scan: unknow item type: {}".format(type(i))))

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _scan


def scan(accumulator, seed, reduce=False):
    """Computes an accumulate value on each item of the source observable.

    Applies an accumulator function over an observable sequence and
    returns each intermediate result.

    .. marble::
        :alt: scan

        -1--2--3--4--5--6--|
        [    scan(acc+i)   ]
        -1--3--6--10-15-21-|

    Examples:
        >>> rs.operators.scan(lambda acc, i: acc + i, seed=0, reduce=False)

    Args:
        accumulator: A function called on each item, that accumulates
            tranformation results.
        seed: The initial value of the accumulator
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable sequence containing the accumulated values.
    """

    def _scan(source):
        if isinstance(source, rs.MuxObservable):
            return scan_mux(accumulator, seed, reduce)(source)
        else:
            if reduce is False:
                return ops.scan(accumulator, seed)(source)
            else:
                return rx.pipe(
                    ops.scan(accumulator, seed),
                    ops.last(),
                )

    return _scan
