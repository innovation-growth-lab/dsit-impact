"""
This script provides a set of functions to compute topic embeddings, distance 
matrices, and diversity components for a given dataset.

Functions:
    - compute_topic_embeddings(cwts_data: pd.DataFrame) -> Tuple[pd.DataFrame, 
      pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        Computes topic embeddings and distance matrices for topics, subfields, 
        fields, and domains.
    - create_author_aggregates(authors_data: AbstractDataset, level: int) -> 
      pd.DataFrame:
        Creates aggregates of author data based on a specified taxonomy level.
    - calculate_diversity_components(data: pd.DataFrame, disparity_matrix: 
      pd.DataFrame) -> pd.DataFrame:
        Calculates diversity components based on the given data and disparity 
        matrix.
    - calculate_coauthor_diversity(publications: pd.DataFrame, authors: 
      pd.DataFrame, disparity_matrix: pd.DataFrame) -> pd.DataFrame:
        Calculates the coauthor diversity metrics for a given set of publications 
        and authors.
    - calculate_paper_diversity(publications: pd.DataFrame, disparity_matrix: 
      pd.DataFrame) -> pd.DataFrame:
        Calculates the diversity metrics for a given set of publications.

Dependencies:
    - pandas
    - numpy
    - scipy
    - sentence-transformers
    - logging
"""

import logging
from typing import Tuple
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from kedro.io import AbstractDataset
from .utils import (
    compute_distance_matrix,
    aggregate_embeddings_and_compute_matrix,
    create_author_and_year_frequency,
    calculate_disparity,
)


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


def create_author_aggregates(
    authors_data: AbstractDataset, level: int, cwts_data: list
) -> pd.DataFrame:
    """
    Create aggregates of author data based on a specified taxonomy level.

    Args:
        authors_data (AbstractDataset): A dataset containing author data.
        level (int): The taxonomy level to aggregate the data on.
        cwts_data (list): List of unique topic IDs.

    Returns:
        pd.DataFrame: DataFrame with columns: author, year, publications,
            total_publications, frequency. The "frequency" column contains
            (n_topics,) dimensional arrays of topic frequencies.
    """
    author_records = []

    for i, loader in enumerate(authors_data.values()):
        data = loader()

        # drop duplicates
        data = data.drop_duplicates(subset=["id", "author"])

        # create author and year frequency data
        author_frequencies = create_author_and_year_frequency(data, level, cwts_data)

        logger.info("Processed author data slice %d / %d", i + 1, len(authors_data))
        author_records.append(author_frequencies)

    logger.info("Concatenating author data")
    author_data = pd.concat(author_records, ignore_index=True)
    author_data["total_publications"] = author_data.groupby("author")[
        "publications"
    ].transform("sum")

    return author_data


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
    x_matrix = data[["frequency"]].to_numpy()
    x_matrix = np.vstack(x_matrix[:, 0])
    disparity_matrix = disparity_matrix.to_numpy()
    data.drop(columns=["frequency"], inplace=True)

    # compute variety
    logger.info("Calculating variety")
    n = x_matrix.shape[1]
    nx = np.count_nonzero(x_matrix, axis=1)
    variety = nx / n

    # compute eveness using the Kvålseth-Jost measure for each row
    logger.info("Calculating evenness")
    q = 2
    with np.errstate(divide="ignore", invalid="ignore"):
        p_matrix = x_matrix / np.sum(x_matrix, axis=1, keepdims=True)
        evenness = np.sum(p_matrix**q, axis=1) ** (1 / (1 - q)) - 1
        evenness = np.nan_to_num(evenness / (nx - 1), nan=0.0)

    # compute disparity
    logger.info("Calculating disparity")
    disparity = np.array(
        [calculate_disparity(row, disparity_matrix) for row in x_matrix]
    )

    logger.info("Diversity components calculated")
    diversity_components = data.copy()
    diversity_components["variety"] = variety
    diversity_components["evenness"] = evenness
    diversity_components["disparity"] = disparity

    return diversity_components


def calculate_paper_diversity(
    publications: pd.DataFrame,
    disparity_matrix: pd.DataFrame,
    level: int,
    cwts_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the diversity metrics for a given set of publications.

    Args:
        publications (pd.DataFrame): A DataFrame containing information about the publications.
            It should have columns 'id', 'topics', and 'publication_date'.
        disparity_matrix (pd.DataFrame): A DataFrame representing the disparity matrix.
        level (int): The taxonomy level to aggregate the data on.
        cwts_data (pd.DataFrame): A DataFrame containing the CWTS data.

    Returns:
        pd.DataFrame: A DataFrame containing the diversity metrics for each author.
            It includes columns 'id', 'variety', 'evenness', and 'disparity'.
    """
    data = publications.copy()
    data = data[["id", "topics", "publication_date"]]
    data["author"] = data["id"]

    data = create_author_and_year_frequency(data, level, cwts_data)

    div_metrics = calculate_diversity_components(data, disparity_matrix)

    div_metrics.rename(columns={"author": "id"}, inplace=True)

    return div_metrics[["id", "variety", "evenness", "disparity"]]


def _weight_function(delta_year, alpha=1):
    """
    Compute weight based on the time difference.
    - delta_year: The difference between years.
    - alpha: Smoothing factor (higher = steeper weight dropoff).
    """
    return 1 / (1 + alpha * abs(delta_year))


def cumulative_author_aggregates(author_topics: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the weighted cumulative sum of topic frequencies for each author
    over the years. This function processes a DataFrame containing author topics
    and their frequencies by year. It computes a weighted cumulative sum of
    frequencies for each author, where the weights are determined by the difference
    in years.

    Args:
        author_topics (pd.DataFrame): DataFrame containing author data, including columns
            'author', 'year', and additional topic columns.

    Returns:
        pd.DataFrame: DataFrame containing the author topics with an additional column
            'weighted_cumsum' containing the weighted cumulative sum of frequencies.
    """

    results = []
    len_authors = len(author_topics["author"].unique())

    for i, (_, group) in enumerate(author_topics.groupby("author")):
        if i % 10_000 == 0:
            logger.info("Processing author %d / %d", i + 1, len_authors)
        group = group.sort_values("year")  # Sort by year
        weighted_cumsum_list = []

        # compute weighted cumulative sum for each year
        for _, target_row in group.iterrows():
            target_year = target_row["year"]
            frequencies = np.array(group["frequency"].tolist())
            years = group["year"].to_numpy()

            # compute weights based on year differences
            weights = _weight_function(years - target_year)
            weights = weights[:, np.newaxis]

            # compute weighted cumulative sum for the current year
            weighted_cumsum = np.sum(
                frequencies.astype(np.float16) * weights.astype(np.float16), axis=0
            )
            weighted_cumsum_list.append(weighted_cumsum)

        # Add results back to the DataFrame
        group["frequency"] = weighted_cumsum_list
        results.append(group[["author", "year", "frequency"]])

    results = pd.concat(results, ignore_index=True)

    author_topics = author_topics[
        ["author", "year", "publications", "total_publications"]
    ].merge(results, on=["author", "year"], how="left")

    return author_topics


def calculate_coauthor_diversity(
    publications: pd.DataFrame,
    author_topics: pd.DataFrame,
    disparity_matrix: pd.DataFrame,
):
    """
    Calculate the coauthor diversity metrics for a given set of publications and authors.
    It combines the weighted cumulative sums of topic frequencies for each author
    with the disparity matrix to calculate the diversity components.

    Args:
        publications (pd.DataFrame): DataFrame containing publication data, including
            columns 'id', 'authorships', and 'publication_date'.
        author_topics (pd.DataFrame): DataFrame containing author data, including columns
            'author', 'year', and additional topic columns.
        disparity_matrix (pd.DataFrame): DataFrame containing the disparity matrix
            used for diversity calculation.

    Returns:
        pd.DataFrame: DataFrame containing the coauthor diversity metrics, including
            columns 'id', 'variety', 'evenness', and 'disparity'.
    """
    # prepare data for merge
    publication_frequencies = (
        publications[["id", "authorships", "publication_date"]]
        .copy()
        .assign(
            authorships=publications["authorships"].apply(
                lambda x: [author[0] for author in x] if x is not None else None
            )
        )
        .explode("authorships")
        .assign(year=pd.to_datetime(publications["publication_date"]).dt.year)
        .rename(columns={"authorships": "author"})
        .drop_duplicates(subset=["id", "author"])
        .merge(author_topics, on=["author", "year"], how="left")
        .groupby("id")
        .agg({"frequency": "sum", "year": "first"})
        .reset_index()
        .rename(columns={"id": "author"})
    )

    # drop rows where frequencies are not arrays
    publication_frequencies = publication_frequencies[
        publication_frequencies["frequency"].apply(lambda x: isinstance(x, np.ndarray))
    ]

    # Calculate diversity components
    diversity_components = calculate_diversity_components(
        publication_frequencies, disparity_matrix
    )

    # Rename author column to id
    diversity_components.rename(columns={"author": "id"}, inplace=True)

    return diversity_components[["id", "variety", "evenness", "disparity"]]
