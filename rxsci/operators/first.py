import rxsci as rs
import rx.operators as ops


def first_mux():
    def _first(source):
        def on_subscribe(observer, scheduler):
            state = {}
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    if state[i.key] is False:
                        observer.on_next(i)
                        state[i.key] = True
                elif type(i) is rs.OnCreateMux:
                    state[i.key] = False
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
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
    return _first


def first():
    def _first(source):
        if isinstance(source, rs.MuxObservable):
            return first_mux()(source)
        else:
            return ops.first()(source)

    return _first
