import rxsci as rs
import rx.operators as ops


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
                            state.append(None)                    
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:                    
                    #if i.key[0] in state:
                    observer.on_next(rs.OnNextMux(i.key, state[i.key[0]]))
                    observer.on_next(i)
                    state[i.key[0]] = None
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key, i.error))
                    state[i.key[0]] = None
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
