def percent_generalization(df, column_names, semaphore):
    """
    Applies a generalization technique to one or more columns of a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column_names (str or list): Name of the column(s) to be generalized.
        generalize_func (function): Generalization function to be applied to the column(s).
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """

    # Convert the column name(s) to a list if it's a string
    if isinstance(column_names, str):
        column_names = [column_names]
    
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    # Apply the generalization function to the specified columns using the pandas applymap method
    df[column_names] = df[column_names].applymap(percent_generalize_func)
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def age_generalization(df, column_names, semaphore):
    """
    Applies a generalization technique to one or more columns of a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column_names (str or list): Name of the column(s) to be generalized.
        generalize_func (function): Generalization function to be applied to the column(s).
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """

    # Convert the column name(s) to a list if it's a string
    if isinstance(column_names, str):
        column_names = [column_names]
    
    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    # Apply the generalization function to the specified columns using the pandas applymap method
    df[column_names] = df[column_names].applymap(age_generalize_func)
    semaphore.release()  # Release the semaphore after modifying the DataFrame

def age_generalize_func(value):
    """
    Generalization function for age values.
    
    Args:
        value: The age value to be generalized.
    
    Returns:
        str: The generalized age category.
    """
    if value >= 18:
        return 'Adult'
    else:
        return 'Young'

def percent_generalize_func(value):
    """
    Generalization function for percentage values.
    
    Args:
        value: The percentage value to be generalized.
    
    Returns:
        str: The generalized percentage category.
    """
    if value >= 75:
        return 'High'
    elif value >= 50:
        return 'Medium'
    else:
        return 'Low'
