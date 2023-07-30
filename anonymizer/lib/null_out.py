def drop_columns(df, columns, semaphore):
    """
    Drops the specified columns from a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        columns (str or list): Name of the column(s) to be dropped.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.

    Return:
        Pandas DataFrame without the dropped columns.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    df = df.drop(columns, axis=1)
    semaphore.release()  # Release the semaphore after modifying the DataFrame

    return df
