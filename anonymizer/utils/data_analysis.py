import pandas as pd
from anonymizer.utils.data_processing import check_columns

def calculate_k_anonymity(df, sensitive_columns):
    """
    Calculate the k-anonymity of a dataset.

    Args:
        df (pd.DataFrame): The dataset to analyze.
        sensitive_columns (list): List of columns containing sensitive attributes.

    Returns:
        str: Minimum count among the grouped sensitive attribute combinations.
    """
    check_columns(df, sensitive_columns)

    group_counts = df.groupby(sensitive_columns).size().reset_index(name='count')
    min_count = str(group_counts['count'].min())
    return min_count

def calculate_l_diversity(df, sensitive_columns, diversity_columns):
    """
    Calculate the l-diversity of a dataset.

    Args:
        df (pd.DataFrame): The dataset to analyze.
        sensitive_columns (list): List of columns containing sensitive attributes.
        diversity_columns (list): List of columns containing attributes for diversity measurement.

    Returns:
        str: Average diversity among the unique sensitive attribute combinations.
    """

    check_columns(df, sensitive_columns)
    check_columns(df, diversity_columns)

    unique_combinations = df[sensitive_columns].drop_duplicates()
    min_diversities = []

    for _, group in unique_combinations.iterrows():
        group_df = df[
            (df[sensitive_columns[0]] == group[sensitive_columns[0]]) &
            (df[sensitive_columns[1]] == group[sensitive_columns[1]])
        ]
        group_diversity = group_df[diversity_columns].nunique().min()
        min_diversities.append(group_diversity)

    average_diversity = str(sum(min_diversities) / len(min_diversities))
    return average_diversity

def calculate_t_closeness(df, sensitive_columns, closeness_columns):
    """
    Calculate the t-closeness of a dataset.

    Args:
        df (pd.DataFrame): The dataset to analyze.
        sensitive_columns (list): List of columns containing sensitive attributes.
        closeness_columns (list): List of columns for t-closeness measurement.

    Returns:
        str: Average distance of attribute means or an error message.
    """
    check_columns(df, sensitive_columns)
    check_columns(df, closeness_columns)

    non_numeric_attributes = [attr for attr in closeness_columns if not pd.api.types.is_numeric_dtype(df[attr])]
    if non_numeric_attributes:
        return(f"NaN")

    overall_attribute_means = df[closeness_columns].mean()
    group_distances = df.groupby(sensitive_columns)[closeness_columns].apply(lambda x: (x - overall_attribute_means).abs().mean())
    average_group_distance = str(group_distances.mean())
    return average_group_distance
