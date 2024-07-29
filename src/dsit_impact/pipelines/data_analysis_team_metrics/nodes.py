"""
This is a boilerplate pipeline 'data_analysis_team_metrics'
generated using Kedro 0.19.6
"""

import logging
from typing import Tuple
import re
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import pdist, squareform
from kedro.io import AbstractDataset


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


def create_author_aggregates(authors_data: AbstractDataset, level: int) -> pd.DataFrame:
    """
    Create aggregates of author data based on a specified taxonomy level.

    Args:
        authors_data (AbstractDataset): A dataset containing author data.
        level (int): The taxonomy level to aggregate the data on.

    Returns:
        pd.DataFrame: The aggregated author data.

    """
    agg_author_data = []
    for i, loader in enumerate(authors_data.values()):
        data = loader()
        logger.info("Loaded author data slice %d / %d", i + 1, len(authors_data))
        agg_data = aggregate_taxonomy_by_author_and_year(data=data, level=level)
        logger.info(
            "Created annual values for %d observations", agg_data["author"].nunique()
        )
        agg_author_data.append(agg_data)

    # concatenate slices
    agg_author_data = pd.concat(agg_author_data, ignore_index=True)
    return agg_author_data


def compute_moving_average(aggregated_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the moving average for each column in the given DataFrame.

    Args:
        data (pd.DataFrame): The input DataFrame containing the data.

    Returns:
        pd.DataFrame: The DataFrame with the moving averages computed for each column.
    """
    aggregated_data = aggregated_data.sort_values(["author", "year"])
    for col in aggregated_data.columns.difference(
        ["author", "year", "yearly_publication_count", "total_publication_count"]
    ):
        aggregated_data[col] = aggregated_data.groupby("author")[col].transform(
            lambda x: x.rolling(window=3, min_periods=1).mean()
        )
    return aggregated_data


def calculate_diversity_components(
    data: pd.DataFrame, disparity_matrix: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate diversity components based on the given data and disparity matrix. The diversity
    measure builds from Leydesdorff, Wagner, and Bornmann (2019) and consists of three components:

    - Variety: The number of unique topics an author has published on.
    - Evenness: The distribution of publications across topics.
    - Disparity: The diversity of topics an author has published

    The implementation follows Rousseau's (2023) suggestion to use the Kvålseth-Jost measure for
    evenness, which is a generalisation of the Gini coefficient presented by Jost (2006) and
    included in the meta discussion paper by Chao and Ricotta (2023).

    Args:
        data (pd.DataFrame): The input data containing the necessary columns.
        disparity_matrix (pd.DataFrame): The disparity matrix used for calculating disparity.

    Returns:
        pd.DataFrame: A DataFrame containing the diversity components.

    """
    x_matrix = data.drop(
        columns=[
            "author",
            "year",
            "yearly_publication_count",
            "total_publication_count",
        ]
    ).to_numpy()
    disparity_matrix = disparity_matrix.to_numpy()

    # compute variety
    n = x_matrix.shape[1]
    nx = np.count_nonzero(x_matrix, axis=1)
    variety = nx / n

    # compute eveness using the Kvålseth-Jost measure for each row
    q = 2
    with np.errstate(divide="ignore", invalid="ignore"):
        p_matrix = x_matrix / np.sum(x_matrix, axis=1, keepdims=True)
        evenness = np.sum(p_matrix**q, axis=1) ** (1 / (1 - q)) - 1
        evenness = np.nan_to_num(evenness, nan=0.0) / (n - 1)

    # compute disparity
    disparity = np.array(
        [calculate_disparity(row, disparity_matrix) for row in x_matrix]
    )

    # create a dataframe
    diversity_components = data[
        ["author", "year", "yearly_publication_count", "total_publication_count"]
    ].copy()
    diversity_components["variety"] = variety
    diversity_components["evenness"] = evenness
    diversity_components["disparity"] = disparity

    return diversity_components


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


def calculate_paper_diversity(
    publications: pd.DataFrame, disparity_matrix: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate the diversity metrics for a given set of publications.

    Args:
        publications (pd.DataFrame): A DataFrame containing information about the publications.
            It should have columns 'id', 'topics', and 'publication_date'.
        disparity_matrix (pd.DataFrame): A DataFrame representing the disparity matrix.

    Returns:
        pd.DataFrame: A DataFrame containing the diversity metrics for each author.
            It includes columns 'id', 'variety', 'evenness', and 'disparity'.
    """
    data = publications.copy()
    data = data[["id", "topics", "publication_date"]]
    data["author"] = data["id"]

    data = aggregate_taxonomy_by_author_and_year(data=data, level=4)

    div_metrics = calculate_diversity_components(data, disparity_matrix)

    div_metrics.rename(columns={"author": "id"}, inplace=True)

    return div_metrics[["id", "variety", "evenness", "disparity"]]


def calculate_coauthor_diversity(
    publications: pd.DataFrame, authors: pd.DataFrame, disparity_matrix: pd.DataFrame
):
    paper_data = publications[["id", "authorships", "publication_date"]].copy()
    # get all author ids
    paper_data["authorships"] = paper_data["authorships"].apply(
        lambda x: [author[0] for author in x] if x is not None else None
    )

    # prepare data for merge
    paper_data = paper_data.explode("authorships")
    paper_data["publication_date"] = pd.to_datetime(
        paper_data["publication_date"]
    ).dt.year
    paper_data.rename(
        columns={"authorships": "author", "publication_date": "year"}, inplace=True
    )

    # Merge publications with authors on author ID
    merged_df = paper_data.merge(authors, on=["author", "year"], how="left")

    # fillna with 0
    merged_df.fillna(0, inplace=True)

    # Aggregate topic columns and year
    topic_columns = [col for col in authors.columns if col not in ["author", "year"]]
    aggregated_df = (
        merged_df.groupby("id")
        .agg({**{col: "sum" for col in topic_columns}, "year": "first"})
        .reset_index()
    )

    aggregated_df.rename(columns={"id": "author"}, inplace=True)

    # Calculate diversity components
    diversity_components = calculate_diversity_components(aggregated_df, disparity_matrix)

    # Rename author column to id
    diversity_components.rename(columns={"author": "id"}, inplace=True)

    return diversity_components[['id', 'variety', 'evenness', 'disparity']]
