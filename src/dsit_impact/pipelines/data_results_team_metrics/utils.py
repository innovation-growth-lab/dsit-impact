"""
This script provides utility functions for computing topic embeddings, distance 
matrices, and diversity components for a given dataset.

Functions:
    - compute_distance_matrix(embeddings: np.ndarray, ids: list) -> pd.DataFrame:
        Computes the pairwise distance matrix between embeddings and normalises 
        the matrix.
    - aggregate_embeddings_and_compute_matrix(data: pd.DataFrame, group_by_col: 
        str, embeddings_col: str) -> pd.DataFrame:
        Groups data by a specified column, aggregates embeddings, and computes 
        a distance matrix.
    - _filter_single_list(topic, level):
        Extracts the specified level from a nested list of topics.
    - _compute_frequency_arrays(topic_counts: pd.DataFrame, author_counts: 
        pd.DataFrame, topic_to_col: dict, n_topics: int) -> list:
        Efficiently computes topic frequency arrays for author-year combinations.
    - create_author_and_year_frequency(data: pd.DataFrame, level: int, cwts_data: 
        pd.DataFrame) -> pd.DataFrame:
        Aggregates taxonomy data by author and year and creates topic frequency 
        arrays.
    - calculate_disparity(x_row: np.array, d: np.array) -> float:
        Calculates disparity as the average distance between elements in the 
        given array based on a disparity matrix.

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


def _filter_single_list(topic, level):
    """Util function to parse out the "level"th position of nested lists"""
    matches = re.findall(r"\d+", topic[level])
    return int(matches[0]) if matches else np.nan


def _compute_frequency_arrays(topic_counts, author_counts, topic_to_col, n_topics):
    """
    Efficiently compute (n_topics,) dimensional frequency arrays for all author-year combinations.

    Args:
        topic_counts (pd.DataFrame): DataFrame with columns: author, year, topic_id, frequency.
        author_counts (pd.DataFrame): DataFrame with columns: author, year.
        topic_to_col (dict): Mapping of topic IDs to column indices.
        n_topics (int): Total number of unique topics.

    Returns:
        np.ndarray: Array of shape (n_author_years, n_topics), where each row is a topic frequency array.
    """
    # map author-year combinations to row indices
    author_year_map = {
        tuple(row): i for i, row in author_counts[["author", "year"]].iterrows()
    }
    topic_counts["row_index"] = topic_counts.apply(
        lambda row: author_year_map[(row["author"], row["year"])], axis=1
    )

    # map topic IDs to column indices
    topic_counts["col_index"] = topic_counts["topic_id"].map(topic_to_col)

    # initialise an empty array for all frequencies
    frequency_matrix = np.zeros((len(author_counts), n_topics), dtype=int)

    # populate the matrix
    for _, row in topic_counts.iterrows():
        frequency_matrix[row["row_index"], row["col_index"]] += row["frequency"]

    # convert each row into a list of frequencies
    return list(frequency_matrix)


def create_author_and_year_frequency(
    data: pd.DataFrame, level: int, cwts_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Aggregates taxonomy level by author and year, and adds publication counts.

    Args:
        df (pd.DataFrame): Input DataFrame with columns 'id', 'author', 'publication_date',
            and 'topics', where 'topics' is a list of dictionaries with keys 'topic', 'subfield',
            'field', and 'domain'.
        level (int): The taxonomy level to aggregate by (0 for topic, 2 for subfield, 4 for field,
            and 6 for domain).

    Returns:
        pd.DataFrame: DataFrame with 'author_id', 'year', 'topics', 'yearly_publication_count',
            and 'total_publication_count' aggregated.
    """

    topic_to_col = {topic: i for i, topic in enumerate(sorted(cwts_data))}

    # extract year
    data["year"] = pd.to_datetime(data["publication_date"]).dt.year

    # aggregate counts for authors
    author_counts = (
        data.groupby(["author", "year"]).agg(publications=("id", "count")).reset_index()
    )

    # flatten topics and create frequency arrays
    flattened_topics = (
        data[["author", "year", "topics"]].explode("topics").dropna(subset=["topics"])
    )
    flattened_topics["topic_id"] = flattened_topics["topics"].apply(
        lambda x: _filter_single_list(x, level)
    )
    topic_counts = (
        flattened_topics.groupby(["author", "year", "topic_id"])
        .size()
        .reset_index(name="frequency")
    )

    # create (n_topics,) frequency arrays
    frequency_arrays = _compute_frequency_arrays(
        topic_counts, author_counts, topic_to_col, len(cwts_data)
    )
    author_counts["frequency"] = frequency_arrays

    return author_counts


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
