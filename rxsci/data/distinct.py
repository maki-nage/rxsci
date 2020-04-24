import rx
import rx.operators as ops


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

        def subscribe(observer, scheduler=None):
            hashset = set()

            def on_next(x):
                key = x

                if key_mapper:
                    try:
                        key = key_mapper(x)
                    except Exception as ex:
                        observer.on_error(ex)
                        return

                if key not in hashset:
                    hashset.add(key)
                    observer.on_next(x)
            return source.subscribe_(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler)
        return rx.create(subscribe)
    return _distinct
