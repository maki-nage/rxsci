import rx.operators as ops


def fill_none(value):
    '''Replaces None values with value
    '''

    def __fill_none(i):
        fields = {}
        for field in i._fields:
            val = getattr(i, field)
            if val is None:
                fields[field] = value

        if len(fields) > 0:
            return i._replace(**fields)
        else:
            return i

    def _fill_none(source):
        return source.pipe(
            ops.map(__fill_none),
        )

    return _fill_none
