from array import array
import rx
from rx.subject import Subject
import rxsci as rs
from .multiplex import demux_mux_observable


def group_by_mux(key_mapper):
    outer_observer = Subject()
    def _group_by(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    key = i.key
                    map_key = key_mapper(i.item)

                    index = i.store.get_map(state, key, map_key)
                    if index is rs.state.markers.STATE_NOTSET:
                        index = i.store.add_map(state, key, map_key)
                        observer.on_next(rs.OnCreateMux((index, i.key), store=i.store))
                    observer.on_next(i._replace(key=(index, i.key)))

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    outer_observer.on_next(i)

                elif type(i) is rs.OnCompletedMux:
                    for k in i.store.iterate_map(state, i.key):
                        index = i.store.get_map(state, i.key, k)
                        observer.on_next(i._replace(key=(index, i.key)))
                        i.store.del_map(state, i.key, k)
                    i.store.del_key(state, i.key)
                    outer_observer.on_next(i)
                    
                elif type(i) is rs.OnErrorMux:
                    for k in i.store.iterate_map(state, i.key):
                        index = i.store.get_map(state, i.key, k)
                        observer.on_next(i._replace(key=(index, i.key)))
                        i.store.del_map(state, i.key, k)
                    i.store.del_key(state, i.key)
                    outer_observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_mapper(name="groupby")
                    observer.on_next(i)

                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _group_by, outer_observer


def group_by(key_mapper, pipeline):
    """Groups items of according to a key mapper

    .. marble::
        :alt: group_by

        --1--2--a--3--b--c-|
        [    group_by()    ]
        -+-----+-----------|
               +a-----b--c-|
         +1--2-----3-------|

    Examples:
        >>> rs.ops.group_by(lambda i: i.category, rs.ops.count)

    Args:
        key_mapper: A function to extract the key from each item
        pipeline: The Rx pipe to execute on each group.

    Source:
        A MuxObservable.

    Returns:
        A MuxObservable with one observable per group.
    """
    _group_by, outer_obs = group_by_mux(key_mapper)

    return rx.pipe(
        _group_by,
        pipeline,
        demux_mux_observable(outer_obs),
    )
