import rx


def run(pipe):
    try:
        return pipe.run()
    except rx.internal.SequenceContainsNoElementsError:
        pass
