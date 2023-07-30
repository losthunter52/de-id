import hashlib

def apply_md5(df, columns, semaphore):
    """
    Applies the MD5 hash function to the specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        columns (str or list): Name of the column(s) to apply the MD5 hash.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def apply_sha1(df, columns, semaphore):
    """
    Applies the SHA1 hash function to the specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        columns (str or list): Name of the column(s) to apply the SHA1 hash.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(lambda x: hashlib.sha1(str(x).encode()).hexdigest())
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def apply_sha256(df, columns, semaphore):
    """
    Applies the SHA256 hash function to the specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        columns (str or list): Name of the column(s) to apply the SHA256 hash.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
    semaphore.release()  # Release the semaphore after modifying the DataFrame
