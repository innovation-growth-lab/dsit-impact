"""
This script provides functionality for retrieving and processing citation
intent data from the GtR-OpenAlex dataset using the Semantic Scholar API.
It includes functions to fetch citation and paper data, and to concatenate
partitioned datasets.

Functions:
    get_citation_data(oa_dataset, base_url, fields, api_key, perpage=500):
        Retrieves citation intent data from the GtR-OpenAlex dataset.
    get_paper_data(oa_dataset, base_url, fields, api_key):
        Retrieves paper data from the Open Access dataset.
    concatenate_partitions(partitioned_dataset):
        Concatenates the partitions from the given inputs.

Dependencies:
    - logging
    - pandas
    - requests
    - kedro
"""
import logging
from typing import Sequence, Generator, Dict
import pandas as pd
from kedro.io import AbstractDataset
from .utils import get_intent, get_paper_details

logger = logging.getLogger(__name__)


def get_citation_data(
    oa_dataset: pd.DataFrame,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
    perpage: int = 500,
) -> Generator:
    """
    Retrieves citation intent data from the GtR-OpenAlex dataset.

    Args:
        oa_dataset (pd.DataFrame): The OpenAlex dataset containing IDs.
        base_url (str): The base URL for the Semantic Scholar API.
        fields (List[str]): The fields to fetch from the API.
        api_key (str): The API key to use.
        perpage (int, optional): The number of citations to fetch per page.

    Yields:
        Dict: A dictionary containing the processed citation dataframe.

    """
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")

    # split the dataset into chunks of 10_000
    dataset_chunks = [
        oa_dataset.iloc[i : i + 10_000] for i in range(0, len(oa_dataset), 10_000)
    ]

    for i, chunk in enumerate(dataset_chunks):
        logger.info("Processing chunk %d / %d", i, len(dataset_chunks))
        # get paper influential and PDF details
        processed_df = get_intent(
            oa_dataset=chunk,
            base_url=base_url,
            fields=fields,
            api_key=api_key,
            perpage=perpage,
        )
        logger.info("Processed chunk %d / %d", i, len(dataset_chunks))
        yield {f"s{i}": processed_df}


def get_paper_data(
    oa_dataset: pd.DataFrame,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
) -> Generator:
    """
    Retrieves paper data from the Open Access dataset.

    Args:
        oa_dataset (pd.DataFrame): The Open Access dataset.
        base_url (str): The base URL for the API.
        fields (Sequence[str]): The fields to retrieve from the API.
        api_key (str): The API key for authentication.

    Yields:
        Dict: A dictionary containing the processed paper dataframe.
    """
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")

    # split the dataset into chunks of 10_000
    dataset_chunks = [
        oa_dataset.iloc[i : i + 10_000] for i in range(0, len(oa_dataset), 10_000)
    ]

    for i, chunk in enumerate(dataset_chunks):
        logger.info("Processing chunk %d / %d", i, len(dataset_chunks))
        # get paper influential and PDF details
        processed_df = get_paper_details(
            oa_dataset=chunk, base_url=base_url, fields=fields, api_key=api_key
        )
        logger.info("Processed chunk %d / %d", i, len(dataset_chunks))
        yield {f"s{i}": processed_df}


def concatenate_partitions(
    partitioned_dataset: Dict[str, AbstractDataset]
) -> pd.DataFrame:
    """
    Concatenate the partitions from the given inputs.

    Args:
        partitioned_dataset (Dict[str, AbstractDataset]): The partitioned dataset.

    Returns:
        pd.DataFrame: The concatenated dataset.
    """
    datasets = []
    for i, dataset in enumerate(partitioned_dataset.values()):
        logger.info("Concatenating partition %d / %d", i + 1, len(partitioned_dataset))
        datasets.append(dataset())
    return pd.concat(datasets, ignore_index=True)
