"""
Branch node for conditional pipeline routing.
"""

from __future__ import annotations

import pandas as pd
from dataclasses import dataclass, field
from typing import Callable

from .step import Step

@dataclass()
class Branch:
    """
    Conditional fork inside a FlowPipeline.

    Evaluates `condition(df)` at runtime and executes either
    the `if_true` or `if_false` step list.

    Parameters
    ----------
    name : str
        Branch label used in logs and visualization.
    condition : Callable[[pd.DataFrame], bool]
        Predicate that receives the current DataFrame and returns bool.
    if_true : list[step]
        Steps executed when condition returns true.
    if_false : list[step], default=[]
        Steps executed when condition returns false. Empty = pass-through.
    """

    name: str
    condition: Callable[[pd.DataFrame], bool]
    if_true: list[Step] = field(default_factory=list)
    if_false: list[Step] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.name, str):
            raise ValueError("Branch name must be non-empty string")

        if not callable(self.condition):
            raise TypeError("Branch condition must be callable")

        if not self.if_true and not self.if_false:
            raise ValueError("Branch must define at least one path (if_true or if_false)")

        for step in self.if_true + self.if_false:
            if not isinstance(step, Step):
                raise TypeError("Branch paths must contain Step instances")

    def __repr__(self) -> str:
        return (
            f"Branch(name={self.name!r}, "
            f"if_true={len(self.if_true)} step(s), "
            f"if_false={len(self.if_false)} step(s))"
        )
