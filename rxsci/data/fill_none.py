import rxsci as rs


def fill_none(value):
    '''Replaces None values with value

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: fill_none

        -0----none-2----1-----|
        [    fill_none(42)    ]
        -0----42---4----1-----|

    Args:
        value: The value used to replace None values.

    Returns:
        An observable where None items are replaced with value.
    '''

    def _fill_none(i):
        if isinstance(i, tuple):  # we mandate namedtuple
            fields = {}
            for field in i._fields:
                val = getattr(i, field)
                if val is None:
                    fields[field] = value

            if len(fields) > 0:
                return i._replace(**fields)
            else:
                return i
        else:
            return value if i is None else i

    return rs.ops.map(_fill_none)
