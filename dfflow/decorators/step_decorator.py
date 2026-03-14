"""
Decorator utilities for marking pipeline step function.
"""

from __future__ import annotations

import pandas as pd
from typing import Callable

def step(name: str, cacheable:bool = False) -> Callable:
    """
    Decorator to attach pipeline metadata to transform functions.

    Parameters
    ----------
    name : str
        Display name of the pipeline step.
    cacheable : bool, default=False
        Whether step supports input caching.

    Returns
    -------
    Callable
        Decorated function with step metadata attached.
    """

    if not isinstance(name, str) or not name:
        raise ValueError("Step must be non-empty string")

    if not isinstance(cacheable, bool):
        raise TypeError("cacheable must be bool")


    def decorator(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
        if not callable(func):
            raise TypeError("Decorated object must be callable")

        setattr(func,
                "_dfflow_meta",
                {
                    "name": name,
                    "cacheable": cacheable,
                },
            )
        return func
    return decorator