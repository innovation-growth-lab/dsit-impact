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

import re
import logging
from datetime import datetime
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
    filter_date: str = None,
) -> Generator:
    """
    Retrieves citation intent data from the GtR-OpenAlex dataset.

    Args:
        oa_dataset (pd.DataFrame): The OpenAlex dataset containing IDs.
        base_url (str): The base URL for the Semantic Scholar API.
        fields (List[str]): The fields to fetch from the API.
        api_key (str): The API key to use.
        perpage (int, optional): The number of citations to fetch per page.
            Defaults to 500.
        filter_date (str, optional): The date to filter the dataset by.
            Defaults to None.

    Yields:
        Dict: A dictionary containing the processed citation dataframe.

    """
    oa_dataset = oa_dataset.copy()
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")
    day_timestamp = str(datetime.now().strftime("%y%m%d"))

    if filter_date not in ("None", None):
        # for left-censored updates, filter out older OA publications
        assert re.match(r"\d{4}-\d{2}-\d{2}", filter_date)
        oa_dataset = oa_dataset[oa_dataset["publication_date"] >= filter_date]

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
        yield {f"{day_timestamp}/s{i}": processed_df}


def get_paper_data(
    oa_dataset: pd.DataFrame,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
    filter_date: str = None,
) -> Generator:
    """
    Retrieves paper data from the Open Access dataset.

    Args:
        oa_dataset (pd.DataFrame): The Open Access dataset.
        base_url (str): The base URL for the API.
        fields (Sequence[str]): The fields to retrieve from the API.
        api_key (str): The API key for authentication.
        filter_date (str, optional): The date to filter the dataset by.
            Defaults to None.

    Yields:
        Dict: A dictionary containing the processed paper dataframe.
    """
    oa_dataset = oa_dataset.copy()
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")
    day_timestamp = str(datetime.now().strftime("%y%m%d"))

    if filter_date not in ("None", None):
        # for left-censored updates, filter out older OA publications
        assert re.match(r"\d{4}-\d{2}-\d{2}", filter_date)
        oa_dataset = oa_dataset[oa_dataset["publication_date"] >= filter_date]

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
        yield {f"{day_timestamp}/s{i}": processed_df}


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

    concat_data = pd.concat(datasets, ignore_index=True)

    # drop duplicates based on id, doi, context
    cols_to_use_for_dup = [
        col for col in concat_data.columns if col in ["id", "doi", "context"]
    ]

    # sort values by influential to keep most up-to-date paper detail data
    concat_data = concat_data.sort_values(by="influential", ascending=False)
    concat_data = concat_data.drop_duplicates(subset=cols_to_use_for_dup, keep="first")

    return concat_data


def get_unmatched_papers(
    incoming_data: pd.DataFrame, s2_data: AbstractDataset, only_unparsed: bool = True
) -> pd.DataFrame:
    """
    Get the unparsed papers from the incoming data.

    Args:
        incoming_data (pd.DataFrame): The incoming data.
        s2_data (AbstractDataset): The Semantic Scholar dataset.
        only_unparsed (bool, optional): Whether to return only the unparsed papers.
            Defaults to True.

    Returns:
        pd.DataFrame: The unparsed papers.
    """
    parsed_papers = []
    for i, loader in enumerate(s2_data.values()):
        logger.info("Processing loader %d / %d", i, len(s2_data))
        data = loader()
        data = data.drop_duplicates(subset=["id"])
        parsed_papers.append(data)

    parsed_papers = pd.concat(parsed_papers, ignore_index=True)

    # get the unparsed PDFs
    if only_unparsed:
        unparsed_papers = incoming_data[
            ~incoming_data["id"].isin(parsed_papers["id"])
        ]

        return unparsed_papers

    return incoming_data
