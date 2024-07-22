"""
This is a boilerplate pipeline 'data_analysis_team_metrics'
generated using Kedro 0.19.6
"""

import logging
from typing import Sequence, Dict, Generator, Union
import pandas as pd
from joblib import Parallel, delayed
import numpy as np
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import pdist, squareform


logger = logging.getLogger(__name__)


def compute_topic_embeddings(cwts_data: pd.DataFrame):

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


def compute_distance_matrix(embeddings, ids):
    """
    Compute the distance matrix between embeddings and return a normalized matrix.

    Parameters:
    embeddings (numpy.ndarray): An array of shape (n_samples, n_features) containing the embeddings.
    ids (list): A list of length n_samples containing the IDs corresponding to each embedding.

    Returns:
    pandas.DataFrame: A DataFrame of shape (n_samples, n_samples) containing the normalized distance matrix.
    """
    distance_matrix = squareform(pdist(embeddings, "euclidean"))
    min_value = np.min(distance_matrix[distance_matrix >= 0])
    max_value = np.max(distance_matrix)
    normalized_matrix = (distance_matrix - min_value) / (max_value - min_value)
    np.fill_diagonal(normalized_matrix, 0)
    return pd.DataFrame(np.tril(normalized_matrix), index=ids, columns=ids)


def aggregate_embeddings_and_compute_matrix(data, group_by_col, embeddings_col):
    """
    Aggregates embeddings and computes a distance matrix based on the aggregated embeddings.

    Args:
        data (pandas.DataFrame): The input data containing the embeddings.
        group_by_col (str): The column to group the data by.
        embeddings_col (str): The column containing the embeddings.

    Returns:
        tuple: A tuple containing the distance matrix and the list of IDs.

    """
    grouped_data = data.groupby(group_by_col)[embeddings_col].apply(
        lambda x: np.mean(np.vstack(x), axis=0)
    )
    aggregated_embeddings = grouped_data.tolist()
    ids = grouped_data.index.tolist()
    return compute_distance_matrix(np.array(aggregated_embeddings), ids)
