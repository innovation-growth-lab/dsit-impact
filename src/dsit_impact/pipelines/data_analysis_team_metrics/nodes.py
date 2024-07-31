"""
This script provides a set of functions to compute topic embeddings, distance matrices, and 
diversity components for a given dataset.

Functions:
    - compute_topic_embeddings(cwts_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, 
      pd.DataFrame, pd.DataFrame]:
        Computes topic embeddings and distance matrices for topics, subfields, fields, and domains.
    - create_author_aggregates(authors_data: AbstractDataset, level: int) -> pd.DataFrame:
        Creates aggregates of author data based on a specified taxonomy level.
    - calculate_diversity_components(data: pd.DataFrame, disparity_matrix: pd.DataFrame
        ) -> pd.DataFrame:
        Calculates diversity components based on the given data and disparity matrix.
    - calculate_coauthor_diversity(publications: pd.DataFrame, authors: pd.DataFrame,
        disparity_matrix: pd.DataFrame) -> pd.DataFrame:
        Calculate the coauthor diversity metrics for a given set of publications and authors.
    - calculate_paper_diversity(publications: pd.DataFrame, disparity_matrix: pd.DataFrame
        ) -> pd.DataFrame:
        Calculate the diversity metrics for a given set of publications.

Dependencies:
    - pandas
    - numpy
    - scipy
    - sentence-transformers
    - logging

Usage:
    Import the necessary functions and call them with appropriate arguments to compute embeddings, 
    distance matrices, and diversity components for your dataset.
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
    aggregate_taxonomy_by_author_and_year,
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
        evenness = np.nan_to_num(evenness / (nx - 1), nan=0.0)

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
    """
    Calculate the coauthor diversity metrics for a given set of publications and authors.

    Args:
        publications (pd.DataFrame): DataFrame containing publication data, including
            columns 'id', 'authorships', and 'publication_date'.
        authors (pd.DataFrame): DataFrame containing author data, including columns
            'author', 'year', and additional topic columns.
        disparity_matrix (pd.DataFrame): DataFrame containing the disparity matrix
            used for diversity calculation.

    Returns:
        pd.DataFrame: DataFrame containing the coauthor diversity metrics, including
            columns 'id', 'variety', 'evenness', and 'disparity'.
    """

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
    diversity_components = calculate_diversity_components(
        aggregated_df, disparity_matrix
    )

    # Rename author column to id
    diversity_components.rename(columns={"author": "id"}, inplace=True)

    return diversity_components[["id", "variety", "evenness", "disparity"]]
