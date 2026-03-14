"""
Custom exception for dfflow pipeline execution.
"""
from __future__ import annotations

class PipelineStepError(Exception):
    """
    Raised when a pipeline step fails during execution.

    Parameters
    ----------
    step_name : str
        Name of the step that failed.
    """

    def __init__(self, step_name: str) -> None:
        super().__init__(f"Pipeline step failed: {step_name}")
        self.step_name = step_name
