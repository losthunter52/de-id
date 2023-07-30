import numpy as np
import re


def mask_full(df, column_names, semaphore):
    """
    Applies the '*' mask to all specified columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        column_names (list): A list of column names to apply the mask to.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[column_names] = df[column_names].fillna('*')
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def mask_range(df, column_names, start_index, end_index, semaphore):
    """
    Applies the '*' mask to a range of characters in each specified column.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        column_names (list): A list of column names to apply the mask to.
        start_index (int): The starting index of the range (inclusive).
        end_index (int): The ending index of the range (exclusive).
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in column_names:
        df[column] = apply_range_mask_vectorized(df[column], start_index, end_index)
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def apply_range_mask_vectorized(column, start_index, end_index):
    """
    Applies the '*' mask to a range of characters in the column.

    Args:
        column (pandas.Series): The input column.
        start_index (int): The starting index of the range (inclusive).
        end_index (int): The ending index of the range (exclusive).

    Returns:
        pandas.Series: The column with the specified range masked.
    """
    mask = np.arange(len(column)).reshape(-1, 1) >= start_index
    mask &= np.arange(len(column)).reshape(-1, 1) < end_index
    column[mask] = '*'
    return column


def mask_last_n_characters(df, column_names, n, semaphore):
    """
    Applies the '*' mask to the last N characters of each specified column.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        column_names (list): A list of column names to apply the mask to.
        n (int): The number of characters to mask from the end of each value.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in column_names:
        df[column] = apply_last_n_character_mask_vectorized(df[column], n)
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def apply_last_n_character_mask_vectorized(column, n):
    """
    Applies the '*' mask to the last N characters of the column.

    Args:
        column (pandas.Series): The input column.
        n (int): The number of characters to mask from the end of each value.

    Returns:
        pandas.Series: The column with the specified range masked.
    """
    mask = np.array([len(str(val)) > n for val in column])
    column[mask] = ['*' * n for _ in range(len(column[mask]))]
    return column


def mask_first_n_characters(df, column_names, n, semaphore):
    """
    Applies the '*' mask to the first N characters of each specified column.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        column_names (list): A list of column names to apply the mask to.
        n (int): The number of characters to mask from the beginning of each value.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in column_names:
        df[column] = apply_first_n_character_mask_vectorized(df[column], n)
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def apply_first_n_character_mask_vectorized(column, n):
    """
    Applies the '*' mask to the first N characters of the column.

    Args:
        column (pandas.Series): The input column.
        n (int): The number of characters to mask from the beginning of each value.

    Returns:
        pandas.Series: The column with the specified range masked.
    """
    mask = np.array([len(str(val)) > n for val in column])
    column[mask] = ['*' * (len(str(val)) - n) + str(val)[-n:] for val in column[mask]]
    return column


def mask_email(df, column_names, semaphore):
    """
    Extracts the email domain from each specified column and replaces it with 'email.com' if it's not a valid email.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        column_names (list): A list of column names to apply the mask to.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    pattern = re.compile(r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    for column in column_names:
        df[column] = extract_email_domain_vectorized(df[column], pattern)
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def extract_email_domain_vectorized(column, pattern):
    """
    Extracts the email domain from a column using a vectorized approach.

    Args:
        column (pandas.Series): The input column.
        pattern (re.Pattern): The regular expression pattern to match the email domain.

    Returns:
        pandas.Series: The column with the email domain extracted or replaced by 'email.com'.
    """
    mask = column.str.contains(pattern)
    column[mask] = column[mask].str.extract(pattern).str[1:]
    column[~mask] = "email.com"
    return column


def mask_cpf(df, cpf_column, semaphore):
    """
    Applies the mask to CPFs, keeping only the first 3 digits and the last 2 digits visible.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        cpf_column (str): The name of the column containing CPF values.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[cpf_column] = mask_cpf_vectorized(df[cpf_column])
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def mask_cpf_vectorized(column):
    """
    Applies the mask to CPFs in a column using a vectorized approach.

    Args:
        column (pandas.Series): The input column.

    Returns:
        pandas.Series: The column with the CPF values masked.
    """
    cpf_lengths = column.str.len()
    mask = cpf_lengths > 5
    column[mask] = column[mask].str[:3] + "*" * (cpf_lengths[mask] - 5) + column[mask].str[-2:]
    column[~mask] = "*" * cpf_lengths[~mask]
    column = column.str[:3] + "." + column.str[3:6] + "." + column.str[6:9] + "-" + column.str[9:]
    return column
