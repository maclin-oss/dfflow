"""
Core pipeline execution engine with conditional branching.
"""

from __future__ import annotations

import time
import uuid
from typing import Callable, TYPE_CHECKING
import pandas as pd

from .step import Step
from .branch import Branch
from .exceptions import PipelineStepError
from ..utils.hashing import hash_df
from ..viz.pipeline_viz import text_visualize
from ..diff.observability_formatter import format_observability_report

if TYPE_CHECKING:
    from ..logging.logger import DFLogger

PipelineNode = Step | Branch


class FlowPipeline:
    """
    Execute a sequence of pipeline nodes (Step or Branch).

    Supports:
    - Sequential execution
    - Conditional branching
    - Step-level caching
    - Structured logging
    - Observability reporting

    Parameters
    ----------
    logger : DFLogger | None, default=None
        Optional DFLogger instance for structured logging.
    """

    def __init__(self, logger: DFLogger | None = None) -> None:
        self.nodes: list[PipelineNode] = []
        self.logger = logger
        self.run_id = str(uuid.uuid4())
        self.steps = self.nodes  # backward compatibility

    # Node Registration
    def add_step(self, step_or_func: Step | Callable) -> None:
        """
        Add a Step or @step-decorated function to the pipeline.

        Parameters
        ----------
        step_or_func : Step | Callable
            A Step instance or a function decorated with @step.
        """
        if isinstance(step_or_func, Step):
            self.nodes.append(step_or_func)

        elif callable(step_or_func) and hasattr(step_or_func, "_dfflow_meta"):
            meta = step_or_func._dfflow_meta
            self.nodes.append(
                Step(meta["name"], step_or_func, cacheable=meta["cacheable"])
            )

        else:
            raise TypeError(
                "add_step expects Step or @step-decorated function"
            )

    def add_branch(
        self,
        name: str,
        condition: Callable[[pd.DataFrame], bool],
        if_true: list[Step],
        if_false: list[Step] | None = None,
    ) -> None:
        """
        Add a conditional Branch node to the pipeline.

        Parameters
        ----------
        name : str
            Human-readable label shown in logs and visualize().
        condition : Callable[[pd.DataFrame], bool]
            Predicate that receives the current DataFrame and returns bool.
        if_true : list[Step]
            Steps executed when condition returns True.
        if_false : list[Step] | None, default=None
            Steps executed when condition returns False. None = pass-through.
        """
        self.nodes.append(
            Branch(
                name=name,
                condition=condition,
                if_true=if_true,
                if_false=if_false or [],
            )
        )

    # Visualization
    def visualize(self) -> str:
        """
        Return ASCII text diagram of the full pipeline including branches.
        """
        return text_visualize(self.nodes)

    # Execution
    def run(
        self,
        df: pd.DataFrame,
        show_observability: bool = False,
    ) -> pd.DataFrame:
        """
        Execute all pipeline nodes sequentially.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame to process.
        show_observability : bool, default=False
            If True, logs a full diff report after every step.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame after all nodes have executed.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Pipeline input must be pandas DataFrame")

        for node in self.nodes:
            if isinstance(node, Branch):
                df = self._run_branch(node, df, show_observability)
            else:
                df = self._run_step(node, df, show_observability)

        return df

    # Internal Execution Helpers
    def _run_step(
        self,
        step: Step,
        df: pd.DataFrame,
        show_observability: bool,
    ) -> pd.DataFrame:
        """
        Execute a single Step with caching, logging, and observability.
        """

        start = time.time()
        before_df = df.copy()

        try:
            if step.cacheable:
                current_hash = hash_df(df)
                if current_hash == step.last_hash:
                    self._safe_log("info", f"{step.name} skipped (cache hit)", df)
                    return df
                step.last_hash = current_hash

            result = step.func(df, **step.config)

            if not isinstance(result, pd.DataFrame):
                raise TypeError(
                    f"Step '{step.name}' must return pandas DataFrame"
                )

            duration = time.time() - start

            self._safe_log(
                "info",
                f"{step.name} | {before_df.shape} → {result.shape} | {duration:.2f}s",
                result,
            )

            if show_observability:
                report = format_observability_report(
                    step.name, before_df, result
                )
                self._safe_log("info", report, result)

            return result

        except Exception as e:
            self._safe_log("error", f"{step.name} FAILED: {repr(e)}", df)
            raise PipelineStepError(step.name) from e

    def _run_branch(
        self,
        branch: Branch,
        df: pd.DataFrame,
        show_observability: bool,
    ) -> pd.DataFrame:
        """
        Evaluate branch condition and execute the matching path.
        """

        try:
            taken = branch.condition(df)

            if not isinstance(taken, bool) and not (hasattr(taken, "item") and isinstance(taken.item(), bool)):
                raise TypeError(
                    f"Branch '{branch.name}' condition must return bool"
                )

            taken = bool(taken)

        except Exception as e:
            self._safe_log(
                "error",
                f"Branch '{branch.name}' condition failed: {repr(e)}",
                df,
            )
            raise PipelineStepError(branch.name) from e

        label = "TRUE" if taken else "FALSE"
        path = branch.if_true if taken else branch.if_false

        self._safe_log(
            "info",
            f"Branch '{branch.name}' → {label} path ({len(path)} step(s))",
            df,
        )

        for step in path:
            df = self._run_step(step, df, show_observability)

        return df

    # Safe Logging
    def _safe_log(self, level: str, message: str, df: pd.DataFrame) -> None:
        """
        Log a message safely — silently skips if no logger is configured.
        """
        if self.logger and hasattr(self.logger, level):
            getattr(self.logger, level)(
                f"[Run {self.run_id}] {message}",
                df,
            )