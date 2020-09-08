import rxsci as rs
import rx.operators as ops
from rxsci.internal.utils import NotSet


def last_mux():
    def _last(source):
        def on_subscribe(observer, scheduler):
            state = []

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    state[i.key[0]] = i.item
                elif type(i) is rs.OnCreateMux:
                    append_count = (i.key[0]+1) - len(state)
                    if append_count > 0:
                        for _ in range(append_count):
                            state.append(NotSet)
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    value = state[i.key[0]]
                    if value is not NotSet:
                        observer.on_next(rs.OnNextMux(i.key, value))
                    observer.on_next(i)
                    state[i.key[0]] = NotSet
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key, i.error))
                    state[i.key[0]] = NotSet
                else:
                    observer.on_next(TypeError("first: unknow item type: {}".format(type(i))))

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _last


def last():
    def _last(source):
        if isinstance(source, rs.MuxObservable):
            return last_mux()(source)
        else:
            return ops.last()(source)

    return _last
