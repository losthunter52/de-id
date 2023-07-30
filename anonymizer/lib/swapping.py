import pandas as pd
import numpy as np

def swap_columns(df, columns, semaphore):
    """
    Swaps the values in the specified columns of the DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to be modified.
        columns (str or list): Name of the column(s) to be swapped.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = np.random.permutation(df[column])
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def swap_rows(df, columns, semaphore):
    """
    Swaps the rows of the DataFrame based on the values in the specified columns.

    Args:
        df (pandas.DataFrame): The DataFrame to be modified.
        columns (str or list): Name of the column(s) to be used for row swapping.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    combined_column = '_combined_'
    
    # Create a combined column with the provided columns
    df[combined_column] = df[columns].apply(tuple, axis=1)
    
    # Shuffle the combined column
    df[combined_column] = np.random.permutation(df[combined_column])
    
    # Split the shuffled combined column back into separate columns
    df[columns] = pd.DataFrame(df[combined_column].tolist(), index=df.index)
    
    # Remove the combined column
    df.drop(columns=[combined_column], inplace=True)
    semaphore.release()  # Release the semaphore after modifying the DataFrame