from __future__ import annotations

from dbetto import AttrsDict

from .exceptions import SimflowConfigError

# Type alias to make it explicit this is the simflow configuration object
SimflowConfig = AttrsDict

__all__ = ["SimflowConfig", "SimflowConfigError"]
