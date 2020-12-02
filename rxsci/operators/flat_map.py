import rxsci as rs
import rx
import rx.operators as ops


def flat_map_mux():
    def _flat_map(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:                    
                        for ii in i.item:
                            observer.on_next(i._replace(item=ii))
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _flat_map


def flat_map():
    def _flat_map(source):
        if isinstance(source, rs.MuxObservable):
            return flat_map_mux()(source)
        else:
            return ops.flat_map(lambda i: rx.from_(i))(source)

    return _flat_map
