from typing import Callable, List, Tuple, Optional
import pandas as pd
from .logger import DFLogger


class FlowPipeline:
    """
    Step-based pipeline for sequential DataFrame transformations.

    Designed to execute a series of Pandas DataFrame operations
    while optionally logging each step using DFLogger.
    """

    def __init__(self, logger: Optional[DFLogger] = None):
        """
        Initialize the pipeline.

        Parameters
        ----------
        logger : DFLogger, optional
            Logger instance used to record pipeline step execution.
        """
        self.steps: List[Tuple[str, Callable]] = []
        self.logger = logger

    def add_step(self, name: str, func: Callable):
        """
        Add a transformation step to the pipeline.

        Parameters
        ----------
        name : str
            Human-readable name of the pipeline step.
        func : Callable
            Function that takes a DataFrame and returns a DataFrame.
        """
        self.steps.append((name, func))

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute all pipeline steps sequentially.

        Each step receives the output DataFrame of the previous step.
        If a logger is provided, step execution details are logged.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame to process.

        Returns
        -------
        pandas.DataFrame
            Final DataFrame after all transformations.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Pipeline input must be DataFrame")

        for name, func in self.steps:
            before_shape = df.shape
            df = func(df)
            after_shape = df.shape

            if self.logger:
                self.logger.info(
                    f"Step '{name}' completed | {before_shape} → {after_shape}",
                    df,
                )

        return df
