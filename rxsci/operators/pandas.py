import rx
import rx.operators as ops


try:
    import pandas as pd

    def to_pandas(columns=None):
        """Converts an observable to a pandas dataframe

        Source:
            An Observable of namedtuples

        Returns:
            An observable the emits a single item. This item is a pandas dataframe.
        """
        return rx.pipe(
            ops.to_list(),
            ops.map(lambda i: pd.DataFrame(i, columns=i[0]._fields if columns is None else columns))
        )

except Exception:
    def to_pandas():
        raise ImportError('Pandas not found. Please install it to use this operator')
