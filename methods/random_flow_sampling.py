from __future__ import annotations  # noqa: D100

import secrets
from typing import Any

from .method import StatelessMethod


class RandomFlowSampling(StatelessMethod):
    """Implementation of random flow sampling."""

    def __init__(self, *, sampling_p: float, **kwargs) -> None:
        """Initialize the RandomFlowSampling method."""
        super().__init__(**kwargs)
        self.sampling_p: float = sampling_p

    def _process_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        if secrets.randbelow(100) / 100.0 < self.sampling_p:
            # renormalize the "packets" and "bytes" fields:
            message["packets"] = int(int(message["packets"]) / self.sampling_p)
            message["bytes"] = int(int(message["bytes"]) / self.sampling_p)
            return message
        return None
