import pandas as pd
import numpy as np
import random

def perturb_date(df, columns, unit, min_val, max_val, semaphore):
    """
    Applies a date perturbation technique to specific columns of the DataFrame.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns where the perturbation will be applied.
    - unit: Unit of time to be added/subtracted (e.g., 'days', 'hours', 'minutes').
    - min_val: Minimum number of units to be added/subtracted.
    - max_val: Maximum number of units to be added/subtracted.
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """

    supported_units = [
        'days',
        'hours',
        'minutes',
        'seconds',
        'milliseconds',
        'microseconds',
        'nanoseconds'
    ]

    if unit not in supported_units:
        raise ValueError(f"Unsupported unit: {unit}")

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(lambda x: x + pd.Timedelta(**{unit: random.randint(min_val, max_val)}))
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def perturb_numeric_range(df, columns, perturbation_range, semaphore):
    """
    Applies a numeric perturbation technique to specific columns of the DataFrame.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns where the perturbation will be applied.
    - perturbation_range: Range of perturbation values as a tuple (min_val, max_val).
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        original_values = df[column]
        perturbed_values = original_values.copy()

        # Check the column type (int or float) and perturb the values
        if np.issubdtype(original_values.dtype, np.integer):
            perturbed_values += np.random.randint(*perturbation_range, size=len(original_values))
        elif np.issubdtype(original_values.dtype, np.floating):
            perturbed_values += np.random.uniform(*perturbation_range, size=len(original_values))
        else:
            raise ValueError(f"Column '{column}' is not of type int or float.")

        df[column] = perturbed_values
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def perturb_numeric_gaussian(df, columns, perturbation_std, semaphore):
    """
    Applies a Gaussian perturbation technique to specific columns of the DataFrame.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns where the perturbation will be applied.
    - perturbation_std: Standard deviation of the Gaussian perturbation.
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        original_values = df[column]
        perturbed_values = original_values.copy()

        # Check the column type (int or float) and perturb the values
        if np.issubdtype(original_values.dtype, np.integer):
            perturbed_values += np.random.normal(scale=perturbation_std, size=len(original_values)).astype(int)
        elif np.issubdtype(original_values.dtype, np.floating):
            perturbed_values += np.random.normal(scale=perturbation_std, size=len(original_values))
        else:
            raise ValueError(f"Column '{column}' is not of type int or float.")

        df[column] = perturbed_values
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def perturb_numeric_laplacian(df, columns, perturbation_value, semaphore):
    """
    Applies a Laplacian perturbation technique to specific columns of the DataFrame.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns where the perturbation will be applied.
    - perturbation_value: Perturbation value.
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        original_values = df[column]
        perturbed_values = original_values.copy()

        # Check the column type (int or float) and perturb the values
        if np.issubdtype(original_values.dtype, np.integer):
            perturbed_values += np.random.laplace(scale=perturbation_value/np.sqrt(2), size=len(original_values))
        elif np.issubdtype(original_values.dtype, np.floating):
            perturbed_values += np.random.laplace(scale=perturbation_value/np.sqrt(2), size=len(original_values))
        else:
            raise ValueError(f"Column '{column}' is not of type int or float.")

        df[column] = perturbed_values
    semaphore.release()  # Release the semaphore after modifying the DataFrame