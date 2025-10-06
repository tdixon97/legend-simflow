from __future__ import annotations


class SimflowConfigError(Exception):
    def __init__(self, block: str, message: str):
        self.block = block
        super().__init__(f"in config block '{block}': {message}")
