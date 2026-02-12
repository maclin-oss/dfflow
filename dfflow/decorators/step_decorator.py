from functools import wraps
import pandas as pd


def log_step(step_name: str):
    """
    Decorator to mark a function as a DataFrame processing step.
    """

    def decorator(func):
        """
        Wrap the target function with step tracking logic.
        """

        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
            """
            Execute the wrapped function and report step completion.

            Parameters
            ----------
            df : pandas.DataFrame
                Input DataFrame passed to the wrapped function.

            Returns
            -------
            pandas.DataFrame
                Output DataFrame returned by the wrapped function.
            """
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Input must be DataFrame")

            result = func(df, *args, **kwargs)
            print(f"[dfflow] {step_name} completed")
            return result

        return wrapper

    return decorator
