import rx
import rx.operators as ops


try:
    import pandas as pd

    def to_pandas(columns=None):
        """Converts an observable to a pandas dataframe

        If colums is not specified, then items must be namedtuples and the
        columns names are infered from the fields of the namedtuple objects.

        The source must be an Observable.

        Args:
            columns: [Optional]

        Returns:
            An observable the emits a single item. This item is a pandas
            DataFrame.
        """
        return rx.pipe(
            ops.to_list(),
            ops.map(lambda i: pd.DataFrame(i, columns=i[0]._fields if columns is None else columns))
        )

except Exception:
    def to_pandas():
        raise ImportError('Pandas not found. Please install it to use this operator')
