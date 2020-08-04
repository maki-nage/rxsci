import rxsci as rs
import rx.operators as ops


def last_mux():
    def _last(source):
        def on_subscribe(observer, scheduler):
            state = {}
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    state[i.key] = i.item
                elif type(i) is rs.OnCreateMux:
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:                    
                    if i.key in state:
                        observer.on_next(rs.OnNextMux(i.key, state[i.key]))
                    observer.on_next(i)
                    del state[i.key]
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key, i.error))
                    del state[i.key]
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
