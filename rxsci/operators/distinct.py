import rx
import rx.operators as ops
import rxsci as rs


def distinct(key_mapper=None):
    """Returns an observable sequence that contains only distinct
    elements according to the key_mapper. Usage of
    this operator should be considered carefully due to the maintenance
    of an internal lookup structure which can grow large.

    .. marble::
        :alt: distinct

        -0-1-2-1-3-4-2-0---|
        [    distinct()    ]
        -0-1-2---3-4-------|

    This operator is similar to the RxPY distinct operator but with much
    better performance thanks to the usage of sets. Meanwhile this
    implementation does not allow to specify a comparer: The key must be
    hashable.

    Source:
        An Observable or a MuxObservable

    Args:
        key_mapper: [Optional]  A function to compute the comparison
            key for each element.

    Returns:
        An operator function that takes an observable source and
        returns an observable sequence only containing the distinct
        elements, based on a computed key value, from the source
        sequence.
    """
    def _distinct(source):
        def on_subscribe(observer, scheduler=None):
            state = None

            def on_next(x):
                nonlocal state

                if type(x) is rs.OnNextMux:
                    i = x.item
                    key = i

                    if key_mapper:
                        try:
                            key = key_mapper(i)
                        except Exception as ex:
                            observer.on_error(ex)
                            return

                    _state = x.store.get_state(state, x.key)
                    if key not in _state:
                        _state.add(key)
                        observer.on_next(x)
                elif type(x) is rs.OnCreateMux:
                    x.store.add_key(state, x.key)
                    x.store.set_state(state, x.key, set())
                    observer.on_next(x)

                elif type(x) in [rs.OnCompletedMux, rs.OnErrorMux]:
                    x.store.del_key(state, x.key)
                    observer.on_next(x)

                elif type(x) is rs.state.ProbeStateTopology:                                        
                    state = x.topology.create_state(name="distinct", data_type='set')
                    observer.on_next(x)
                else:
                    observer.on_next(x)

            return source.subscribe_(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler)

        return rs.MuxObservable(on_subscribe)

    return _distinct
