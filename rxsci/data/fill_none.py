import rxsci as rs


def fill_none(value):
    '''Replaces None values with value

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

    def __fill_none(i):
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

    def _fill_none(source):
        return source.pipe(
            rs.ops.map(__fill_none),
        )

    return _fill_none
