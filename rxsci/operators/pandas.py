import rx
import rx.operators as ops

from rxsci.internal.utils import build_tdqm_kwargs

try:
    import pandas as pd
    try:
        from tqdm.auto import tqdm
    except Exception:
        pass

    def from_pandas(dataframe, scheduler=None, progress=False, index=False):
        """Creates an observable from a pandas dataframe

        When a dict is provided as the progress argument, it accepts these keys:
        interval: The interval in seconds used to update the progressbar
        prefix: A prefix displayed at before the progressbar.

        Args:
            dataframe: A pandas dataframe
            scheduler: An optional scheduler to schedule the values on.
            progess: Displays a progressbar while iterating the dataframe

        Returns:
            An observable that emits one nametuple per row in the dataframe.
        """
        if progress:
            tqdm_kwargs = build_tdqm_kwargs(progress)
            if 'total' not in tqdm_kwargs:
                tqdm_kwargs['total']=len(dataframe)

            return rx.from_(
                tqdm(dataframe.itertuples(index=index), **tqdm_kwargs),
                scheduler=scheduler,
            )
        return rx.from_(
            dataframe.itertuples(index=index),
            scheduler=scheduler,
        )


    def to_pandas(columns=None):
        """Converts an observable to a pandas dataframe

        If colums is not specified, then items must be namedtuples and the
        columns names are infered from the fields of the namedtuple objects.

        The source must be an Observable.

        Args:
            columns: [Optional] The name of the columns of the dataframe

        Returns:
            An observable the emits a single item. This item is a pandas
            DataFrame.
        """
        return rx.pipe(
            ops.to_list(),
            ops.filter(len),
            ops.map(lambda i: pd.DataFrame(i, columns=i[0]._fields if columns is None else columns))
        )

except Exception:
    def to_pandas():
        raise ImportError('Pandas not found. Please install it to use this operator')

    def from_pandas():
        raise ImportError('Pandas not found. Please install it to use this operator')
