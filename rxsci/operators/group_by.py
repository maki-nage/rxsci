import rx
from rx.subject import Subject
import rxsci as rs
import rxsci.operators as rsops


def group_by_mux(key_mapper):
    outer_observer = Subject()
    def _group_by(source):
        def on_subscribe(observer, scheduler):
            state = {}

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    key1 = i.key
                    key2 = key_mapper(i.item)
                    key = (key2, key1)

                    if key1 not in state:
                        state[key1] = set()
                        state[key1].add(key2)
                        observer.on_next(rs.OnCreateMux(key))
                    elif key2 not in state[key1]:
                        state[key1].add(key2)
                        observer.on_next(rs.OnCreateMux(key))
                    observer.on_next(rs.OnNextMux(key, i.item))
                elif type(i) is rs.OnCreateMux:
                    outer_observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    for k in state[i.key]:
                        observer.on_next(rs.OnCompletedMux((k, i.key)))
                    del state[i.key]
                    outer_observer.on_next(i)
                elif type(i) is rs.OnErrorMux:
                    dellist = []
                    for k in state[i.key]:
                        observer.on_next(rs.OnErrorMux((k, i.key), i.error))
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
    _group_by, outer_obs = group_by_mux(key_mapper)

    return rx.pipe(
        _group_by,
        pipeline,
        rsops.demux_mux_observable(outer_obs),
    )
