from __future__ import annotations


class SimflowConfigError(Exception):
    def __init__(self, message: str, block: str | None = None):
        if block is None:
            super().__init__(message)
        else:
            super().__init__(f"in config block '{block}': {message}")
