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
            next_index = 0
            free_slots = array('Q')
            state = {}

            def on_next(i):
                nonlocal next_index
                if type(i) is rs.OnNextMux:
                    key1 = i.key
                    key2 = key_mapper(i.item)

                    if key1 not in state:
                        state[key1] = {}
                        index, next_index, _ = new_index(next_index, free_slots)
                        state[key1][key2] = index
                        observer.on_next(rs.OnCreateMux((index, i.key)))
                    elif key2 not in state[key1]:
                        index, next_index, _ = new_index(next_index, free_slots)
                        state[key1][key2] = index
                        observer.on_next(rs.OnCreateMux((index, i.key)))
                    else:
                        index = state[key1][key2]
                    observer.on_next(rs.OnNextMux((index, i.key), i.item))
                elif type(i) is rs.OnCreateMux:
                    outer_observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    for k in state[i.key]:
                        index = state[i.key][k]
                        observer.on_next(rs.OnCompletedMux((index, i.key)))
                        del_index(free_slots, index)
                    del state[i.key]
                    outer_observer.on_next(i)
                elif type(i) is rs.OnErrorMux:
                    for k in state[i.key]:
                        index = state[i.key][k]
                        observer.on_next(rs.OnErrorMux((index, i.key), i.error))
                        del_index(free_slots, index)
                    del state[i.key]
                    outer_observer.on_next(i)
                else:
                    observer.on_next(TypeError("group_by: unknow item type: {}".format(type(i))))

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
