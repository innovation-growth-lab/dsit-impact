"""
This is a boilerplate pipeline 'data_analysis_team_metrics'
generated using Kedro 0.19.6
"""

import logging
from typing import Tuple
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import pdist, squareform


logger = logging.getLogger(__name__)


def compute_topic_embeddings(
    cwts_data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compute topic embeddings and distance matrices for topics, subfields, fields, and domains.

    Args:
        cwts_data (pd.DataFrame): The input dataframe containing the CWTS data.

    Returns:
        Tuple: A tuple containing the topic distance matrix, subfield distance matrix,
        field distance matrix, and domain distance matrix.
    """
    cwts_data["string_to_encode"] = (
        cwts_data["domain_name"]
        + ", "
        + cwts_data["field_name"]
        + ", "
        + cwts_data["subfield_name"]
        + ", "
        + cwts_data["topic_name"]
        + " - "
        + cwts_data["keywords"]
    )
    encoder = SentenceTransformer("sentence-transformers/allenai-specter")

    logger.info("Computing embeddings for topics")
    cwts_data["topic_embeddings"] = cwts_data["string_to_encode"].apply(encoder.encode)
    embeddings = np.array(cwts_data["topic_embeddings"].tolist())

    logger.info("Computing distance matrices for topics")
    topic_distance_matrix = compute_distance_matrix(
        embeddings, cwts_data["topic_id"].tolist()
    )

    logger.info("Computing distance matrices for subfields, fields, and domains")
    subfield_distance_matrix = aggregate_embeddings_and_compute_matrix(
        cwts_data, "subfield_id", "topic_embeddings"
    )
    field_distance_matrix = aggregate_embeddings_and_compute_matrix(
        cwts_data, "field_id", "topic_embeddings"
    )
    domain_distance_matrix = aggregate_embeddings_and_compute_matrix(
        cwts_data, "domain_id", "topic_embeddings"
    )

    return (
        topic_distance_matrix,
        subfield_distance_matrix,
        field_distance_matrix,
        domain_distance_matrix,
    )


def compute_distance_matrix(embeddings: np.ndarray, ids: list) -> pd.DataFrame:
    """
    Compute the distance matrix between embeddings and return a normalized matrix.

    Parameters:
        embeddings (numpy.ndarray): An array of shape (n_samples, n_features) containing the
            embeddings.
        ids (list): A list of length n_samples containing the IDs corresponding to each
            embedding.

    Returns:
        pd.DataFrame: A DataFrame of shape (n_samples, n_samples) containing the normalized
            distance matrix.
    """
    distance_matrix = squareform(pdist(embeddings, "euclidean"))
    min_value = np.min(distance_matrix[distance_matrix >= 0])
    max_value = np.max(distance_matrix)
    normalized_matrix = (distance_matrix - min_value) / (max_value - min_value)
    np.fill_diagonal(normalized_matrix, 0)
    return pd.DataFrame(np.tril(normalized_matrix), index=ids, columns=ids)


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


def aggregate_topics_by_author_and_year(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates topics by author and year, and adds publication counts.

    Args:
        df (pd.DataFrame): Input DataFrame with columns 'id', 'author_id', 'publication_date',
            and 'topics'.

    Returns:
        pd.DataFrame: DataFrame with 'author_id', 'year', 'topics', 'yearly_publication_count',
            and 'total_publication_count' aggregated.
    """
    data["year"] = pd.to_datetime(data["publication_date"]).dt.year

    aggregated = (
        data.groupby(["author", "year"])["topics"]
        .agg(lambda x: [item[0] for sublist in x for item in sublist])
        .reset_index()
    )
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

    return aggregated
