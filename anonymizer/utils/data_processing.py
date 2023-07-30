import pandas as pd

def value_to_dataframe(values):
    """
    Converts values into a DataFrame.

    Args:
        values (value): The values to be converted.

    Returns:
        pandas.DataFrame: The converted DataFrame.
    """
    df = pd.DataFrame(values)
    return df

def csv_to_dataframe(csv_file):
    """
    Converts a CSV file into a DataFrame.

    Args:
        csv_file (str): The path to the CSV file.

    Returns:
        pandas.DataFrame: The converted DataFrame.
    """
    df = pd.read_csv(csv_file)
    return df

def convert_to_string(df, column_names, semaphore):
    """
    Converts the specified columns to string type.

    Args:
        df (pandas.DataFrame): The DataFrame to be converted.
        column_names (str or list): Name of the column(s) to be converted.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[column_names] = df[column_names].apply(pd.to_string, errors='coerce')
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def convert_to_numeric(df, column_names, semaphore):
    """
    Converts the specified columns to numeric type.

    Args:
        df (pandas.DataFrame): The DataFrame to be converted.
        column_names (str or list): Name of the column(s) to be converted.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[column_names] = df[column_names].apply(pd.to_numeric, errors='coerce')
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def convert_to_datetime(df, column_names, semaphore):
    """
    Converts the specified columns to datetime type.

    Args:
        df (pandas.DataFrame): The DataFrame to be converted.
        column_names (str or list): Name of the column(s) to be converted.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[column_names] = df[column_names].apply(pd.to_datetime, errors='coerce', format='mixed')
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def convert_to_bool(df, column_names, semaphore):
    """
    Converts the specified columns to boolean type.

    Args:
        df (pandas.DataFrame): The DataFrame to be converted.
        column_names (str or list): Name of the column(s) to be converted.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df[column_names] = df[column_names].apply(pd.to_bool, errors='coerce')
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def check_columns(df, semaphore):
    """
    Checks if there are any columns in the DataFrame where all fields are NaN or NaT.

    Args:
        df (pandas.DataFrame): The DataFrame to be checked.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.

    Raises:
        ValueError: If there are any columns where all fields are NaN or NaT.
    """
    semaphore.acquire()  # Acquire the semaphore before accessing the DataFrame
    nan_columns = df.columns[df.isnull().all()]
    semaphore.release()  # Release the semaphore after accessing the DataFrame

    if len(nan_columns) > 0:
        raise ValueError(f"There are columns where all fields are NaN or NaT: {nan_columns.tolist()}")