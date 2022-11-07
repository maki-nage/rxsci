from array import array
import rx
from rx.subject import Subject
import rxsci as rs
from .multiplex import demux_mux_observable


def new_index(next_index, free_slots):
    if len(free_slots) > 0:
        index = free_slots.pop()
        return index, next_index, free_slots
    else:
        index = next_index
        return index, next_index+1, free_slots


def del_index(free_slots, index):
    free_slots.append(index)
    return free_slots


def group_by_mux(key_mapper):
    outer_observer = Subject()

    def _group_by(source):
        def on_subscribe(observer, scheduler):
            state = None
            state_nextindex = None
            state_freeslots = None

            def on_next(i):
                nonlocal state, state_nextindex, state_freeslots

                itype = type(i)
                if itype is rs.OnNextMux:
                    key = i.key[0]
                    map_key = key_mapper(i.item)

                    group_map = i.store.get_state(state, key)
                    index = group_map.get(map_key, None)
                    if index is None:
                        next_index = i.store.get_state(state_nextindex, 0)
                        if next_index is rs.state.markers.STATE_NOTSET:
                            next_index = 0
                        free_slots = i.store.get_state(state_freeslots, 0)
                        if free_slots is rs.state.markers.STATE_NOTSET:
                            free_slots = array('Q')
                        index, next_index, free_slots = new_index(next_index, free_slots)
                        i.store.set_state(state_nextindex, 0, next_index)
                        i.store.set_state(state_freeslots, 0, free_slots)
                        group_map[map_key] = index
                        observer.on_next(rs.OnCreateMux((index, i.key), store=i.store))
                    observer.on_next(i._replace(key=(index, i.key)))

                elif itype is rs.OnCreateMux:
                    i.store.add_key(state, i.key[0])
                    i.store.set_state(state, i.key[0], dict())
                    outer_observer.on_next(i)

                elif itype is rs.OnCompletedMux or itype is rs.OnErrorMux:
                    group_map = i.store.get_state(state, i.key[0])
                    print(group_map)
                    for k, v in group_map.items():
                        observer.on_next(i._replace(key=(v, i.key)))
                    i.store.del_key(state, i.key[0])
                    outer_observer.on_next(i)

                elif itype is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name="groupby", data_type='dict')
                    state_nextindex = i.topology.create_state(name="groupby_nextindex", data_type='int', global_scope=True)
                    state_freeslots = i.topology.create_state(name="groupby_freeslots", data_type='obj', global_scope=True)
                    observer.on_next(i)
                    outer_observer.on_next(i)
                else:
                    if state is None:
                        observer.on_error(ValueError("No state configured in group_by operator. A state store operator is probably missing in the graph"))
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

    The source must be a MuxObservable.

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

    Returns:
        A MuxObservable with one observable per group.
    """
    _group_by, outer_obs = group_by_mux(key_mapper)
    pipeline = rx.pipe(*pipeline) if type(pipeline) is list else pipeline
    return rx.pipe(
        _group_by,
        pipeline,
        demux_mux_observable(outer_obs),
    )
