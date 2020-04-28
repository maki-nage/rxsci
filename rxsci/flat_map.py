import rx.operators as ops


def flat_map(mapper):
    def _flat_map(source):
        """
        Projects each element of an observable sequence to an observable
        sequence and merges the resulting observable sequences into one
        observable sequence.

        Example:
            >>> flat_map(source)

        Args:
            source: Source observable to flat map.

        Returns:
            An operator function that takes a source observable and returns
            an observable sequence whose elements are the result of invoking
            the one-to-many transform function on each element of the
            input sequence .
        """
        return source.pipe(
            ops.map(mapper),
            ops.merge_all(),
        )

    return _flat_map
