from rx.core.observer import AutoDetachObserver
from . import OnCreateMux, OnNextMux, OnCompletedMux, OnErrorMux


class MuxObserver(AutoDetachObserver):
    def __init__(self,
                 on_create_mux,
                 on_next_mux, on_error_mux, on_completed_mux,
                 on_error, on_completed,
                 ):
        self._on_create_mux = on_create_mux
        self._on_next_mux = on_next_mux
        self._on_completed_mux = on_completed_mux
        self._on_error_mux = on_error_mux

        super().__init__(
            on_next=self._on_next_item,
            on_error=on_error,
            on_completed=on_completed
        )

    def _on_next_item(i):
        pass
