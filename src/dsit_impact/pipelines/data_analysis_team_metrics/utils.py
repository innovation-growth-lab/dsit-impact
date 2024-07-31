"""
This script provides util functions to compute topic embeddings, distance matrices, and 
diversity components for a given dataset.

Functions:
    - compute_distance_matrix(embeddings: np.ndarray, ids: list) -> pd.DataFrame:
        Computes the distance matrix between embeddings and returns a normalised matrix.
    - aggregate_embeddings_and_compute_matrix(data: pd.DataFrame, group_by_col: str, 
        embeddings_col: str) -> pd.DataFrame:
        Aggregates embeddings and computes a distance matrix based on the aggregated embeddings.
    - _filter_digits(topics, level):
        Filters digits from the topics based on the specified level.
    - add_topic_columns(aggregated_df: pd.DataFrame) -> pd.DataFrame:
        Adds columns for each unique topic in the 'topics' column of the aggregated DataFrame.
    - aggregate_taxonomy_by_author_and_year(data: pd.DataFrame, level: int) -> pd.DataFrame:
        Aggregates taxonomy level by author and year, and adds publication counts.
    - calculate_disparity(x_row: np.array, d: np.array) -> float:
        Calculates the disparity between elements in the given array.

Dependencies:
    - pandas
    - numpy
    - scipy
"""

import re
import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist, squareform

def compute_distance_matrix(embeddings: np.ndarray, ids: list) -> pd.DataFrame:
    """
    Compute the distance matrix between embeddings and return a normalised matrix.

    Parameters:
        embeddings (numpy.ndarray): An array of shape (n_samples, n_features) containing the
            embeddings.
        ids (list): A list of length n_samples containing the IDs corresponding to each
            embedding.

    Returns:
        pd.DataFrame: A DataFrame of shape (n_samples, n_samples) containing the normalised
            distance matrix.
    """
    distance_matrix = squareform(pdist(embeddings, "euclidean"))
    min_value = np.min(distance_matrix[distance_matrix >= 0])
    max_value = np.max(distance_matrix)
    normalised_matrix = (distance_matrix - min_value) / (max_value - min_value)
    np.fill_diagonal(normalised_matrix, 0)
    return pd.DataFrame(normalised_matrix, index=ids, columns=ids)


def aggregate_embeddings_and_compute_matrix(
    data: pd.DataFrame, group_by_col: str, embeddings_col: str
) -> pd.DataFrame:
    """
    Aggregates embeddings and computes a distance matrix based on the aggregated embeddings.

    Args:
        data (pandas.DataFrame): The input data containing the embeddings.
        group_by_col (str): The column to group the data by.
        embeddings_col (str): The column containing the embeddings.

    Returns:
        pd.DataFrame: The distance matrix based on the aggregated embeddings.
    """
    grouped_data = data.groupby(group_by_col)[embeddings_col].apply(
        lambda x: np.mean(np.vstack(x), axis=0)
    )
    aggregated_embeddings = grouped_data.tolist()
    ids = grouped_data.index.tolist()
    return compute_distance_matrix(np.array(aggregated_embeddings), ids)


def _filter_digits(topics, level):
    return [
        digit
        for sublist in topics
        if sublist is not None
        for item in sublist
        if item is not None
        for digit in re.findall(r"\d+", item[level])
    ]


def _add_topic_columns(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds columns for each unique topic in the 'topics' column of the aggregated DataFrame,
    and populates each column with the count of occurrences of that topic.

    Args:
        aggregated_df (pd.DataFrame): DataFrame with 'author', 'year', and 'topics' columns.

    Returns:
        pd.DataFrame: DataFrame with additional columns for each unique topic.
    """
    exploded_df = aggregated_df.explode("topics")
    topic_dummies = pd.get_dummies(exploded_df["topics"])
    topic_counts = topic_dummies.groupby(exploded_df.index).sum()
    aggregated_df = pd.concat([aggregated_df, topic_counts], axis=1)
    aggregated_df = aggregated_df.drop(columns=["topics"])
    return aggregated_df


def aggregate_taxonomy_by_author_and_year(
    data: pd.DataFrame, level: int
) -> pd.DataFrame:
    """
    Aggregates taxonomy level by author and year, and adds publication counts.

    Args:
        df (pd.DataFrame): Input DataFrame with columns 'id', 'author', 'publication_date',
            and 'topics', where 'topics' is a list of dictionaries with keys 'topic', 'subfield',
            'field', and 'domain'.

    Returns:
        pd.DataFrame: DataFrame with 'author_id', 'year', 'topics', 'yearly_publication_count',
            and 'total_publication_count' aggregated.
    """
    data["year"] = pd.to_datetime(data["publication_date"]).dt.year

    # drop duplicate id, author
    data = data.drop_duplicates(subset=["id", "author"])

    # aggregate topic level by author and year
    aggregated = (
        data.groupby(["author", "year"])["topics"]
        .agg(lambda x: _filter_digits(x, level))
        .reset_index()
    )

    # compute yearly publication counts and total publication counts
    yearly_counts = (
        data.groupby(["author", "year"])
        .size()
        .reset_index(name="yearly_publication_count")
    )
    total_counts = (
        data.groupby("author").size().reset_index(name="total_publication_count")
    )

    # merge the counts with the aggregated DataFrame
    aggregated = pd.merge(aggregated, yearly_counts, on=["author", "year"], how="left")
    aggregated = pd.merge(aggregated, total_counts, on="author", how="left")

    # create columns for each unique topic
    aggregated = _add_topic_columns(aggregated)

    return aggregated


def calculate_disparity(x_row: np.array, d: np.array) -> float:
    """
    Calculates the disparity between elements in the given array.

    Args:
        x_row (np.array): The input array.
        d (np.array): The disparity matrix.

    Returns:
        float: The calculated disparity.

    """
    non_zero_indices = np.nonzero(x_row)[0]
    num_non_zero = len(non_zero_indices)
    if num_non_zero <= 1:
        return 0.0

    disparity_sum = 0.0
    for i in range(num_non_zero):
        for j in range(i + 1, num_non_zero):
            disparity_sum += d[non_zero_indices[i], non_zero_indices[j]]
    return disparity_sum / ((num_non_zero * (num_non_zero - 1)) / 2)
