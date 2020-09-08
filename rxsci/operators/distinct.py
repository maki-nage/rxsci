import rx
import rx.operators as ops
import rxsci as rs


def distinct(key_mapper=None):

    def _distinct(source):
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


        Examples:
            >>> data = range(200000, 0, -1)
            >>> rx.from_(data).pipe(
            >>>     ops.distinct(),
            >>> )

        Args:
            key_mapper: [Optional]  A function to compute the comparison
                key for each element.

        Returns:
            An operator function that takes an observable source and
            returns an observable sequence only containing the distinct
            elements, based on a computed key value, from the source
            sequence.
        """
        mux = True if isinstance(source, rs.MuxObservable) else False

        def subscribe(observer, scheduler=None):
            hashset = {} if mux else set()

            def on_next(x):
                if isinstance(x, rs.OnCreateMux):
                    hashset[x.key] = set()
                    observer.on_next(x)
                    return
                elif isinstance(x, rs.OnCompletedMux) \
                or isinstance(x, rs.OnErrorMux):
                    del hashset[x.key]
                    observer.on_next(x)
                    return

                i = x.item if mux else x
                key = i

                if key_mapper:
                    try:
                        key = key_mapper(i)
                    except Exception as ex:
                        observer.on_error(ex)
                        return

                state = hashset[x.key] if mux else hashset
                if key not in state:
                    state.add(key)
                    observer.on_next(x)
            return source.subscribe_(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler)
        return rx.create(subscribe)
    return _distinct
