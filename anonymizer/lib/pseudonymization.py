import hashlib

def pseudonymize_columns(df, columns, semaphore):
    """
    Pseudonymizes the values in the specified columns of a DataFrame.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns to be pseudonymized.
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].map(lambda x: f'{column}_{hashlib.md5(str(x).encode()).hexdigest()}')
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def pseudonymize_rows(df, columns, semaphore):
    """
    Pseudonymizes the rows of the DataFrame based on the specified columns.

    Args:
    - df: pandas DataFrame.
    - columns: List of columns to be used for pseudonymization.
    - semaphore: threading.Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df['Object'] = df[columns].agg(''.join, axis=1)
    df.drop(columns=columns, inplace=True)
    
    pseudonyms = df['Object'].map(lambda x: f'Object_{hashlib.md5(x.encode()).hexdigest()}')
    df['Object'] = pseudonyms
    semaphore.release()  # Release the semaphore after modifying the DataFrame