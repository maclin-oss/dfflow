"""
Pipeline step abstraction.
"""

from __future__ import annotations

import pandas as pd
from typing import Callable, Any

class Step:
    """
    Represents a single processing step inside a FlowPipeline.

    A Step wraps a transformation function and metadata such as
    configuration and caching information.

    Parameters
    ----------
    name : str
        Human readable step name.
    func : Callable[..., pd.DataFrame]
        Function that takes a pandas DataFrame and returns a transformed DataFrame.
    config : dict[str, Any] | None, default=None
        Optional keyword arguments passed to the step function during the execution.
    cacheable : bool, default=False
        Whether step execution can be skipped if input data has not changed.
    """

    def __init__(self,
                 name: str,
                 func: Callable[..., pd.DataFrame],
                 config: dict[str, Any] | None = None,
                 cacheable: bool = False,
                 ) -> None:

        if not name or not isinstance(name, str):
            raise ValueError("Step name must be non-empty string")

        if not callable(func):
            raise TypeError("Step func must be callable")

        self.name = name
        self.func = func
        self.config = dict(config) if config is not None else {}
        self.cacheable = cacheable
        self.last_hash: str | None = None

    def __repr__(self) -> str:
        return f"Step(name={self.name}, cacheable={self.cacheable})"
