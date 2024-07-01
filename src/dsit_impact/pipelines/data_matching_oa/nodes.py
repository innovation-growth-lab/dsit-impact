"""
This is a boilerplate pipeline 'data_matching_oa'
generated using Kedro 0.19.6
"""

import logging
from typing import List, Dict, Union, Callable, Generator
from html import unescape
import pandas as pd
from joblib import Parallel, delayed
import requests
from kedro.io import AbstractDataset
from .utils import (
    fetch_papers_for_id, preprocess_ids, json_loader,  # OA
    process_item, select_best_match, clean_html_entities, setup_session  # CR
)


logger = logging.getLogger(__name__)


def preprocess_publication_doi(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the Gateway to Research publication data to include
    doi values that are compatible with OA filter module.

    Args:
        df (pd.DataFrame): The Gateway to Research publication data.

    Returns:
        pd.DataFrame: The preprocessed publication data.
    """
    if "doi" in df.columns:
        df["doi"] = df["doi"].str.extract(r"(10\..+)")
    return df


def create_list_doi_inputs(df: pd.DataFrame, **kwargs) -> list:
    """Create a list of doi values from the Gateway to Research publication data.

    Args:
        df (pd.DataFrame): The Gateway to Research publication data.

    Returns:
        list: A list of doi values.
    """
    doi_singleton_list = df[df["doi"].notnull(
    )]["doi"].drop_duplicates().tolist()

    # concatenate doi values to create group querise
    doi_list = preprocess_ids(doi_singleton_list, kwargs.get("grouped", True))

    return doi_list


def fetch_papers(
    ids: Union[List[str], List[List[str]]],
    mailto: str,
    perpage: int,
    filter_criteria: Union[str, List[str]],
    parallel_jobs: int = 8,
) -> Dict[str, List[Callable]]:
    """
    Fetches papers based on the provided processed_ids, mailto, perpage,
    filter_criteria, and parallel_jobs.

    Args:
        ids (Union[List[str], List[List[str]]]): The processed IDs of the papers to fetch.
        mailto (str): The email address to use for fetching papers.
        perpage (int): The number of papers to fetch per page.
        filter_criteria (Union[str, List[str]]): The filter criteria to apply when fetching papers.
        parallel_jobs (int, optional): The number of parallel jobs to use for fetching papers.
            Defaults to 4.

    Returns:
        Dict[str, List[Callable]]: A dictionary containing the fetched papers, grouped by chunks.

    """
    # slice oa_ids
    oa_id_chunks = [ids[i: i + 80] for i in range(0, len(ids), 80)]
    logger.info("Slicing data. Number of oa_id_chunks: %s", len(oa_id_chunks))
    return {
        f"s{str(i)}": lambda chunk=chunk: Parallel(n_jobs=parallel_jobs, verbose=10)(
            delayed(fetch_papers_for_id)(
                oa_id, mailto, perpage, filter_criteria)
            for oa_id in chunk
        )
        for i, chunk in enumerate(oa_id_chunks)
    }


def concatenate_openalex(
    data: Dict[str, AbstractDataset],
) -> pd.DataFrame:
    """
    Load the partitioned JSON dataset, iterate transforms, return dataframe.

    Args:
        data (Dict[str, AbstractDataset]): The partitioned JSON dataset.

    Returns:
        pd.DataFrame: The concatenated OpenAlex dataset.
    """
    outputs = []
    for i, (key, batch_loader) in enumerate(data.items()):
        data_batch = batch_loader()
        df_batch = json_loader(data_batch)
        outputs.append(df_batch)
        logger.info("Loaded %s. Progress: %s/%s", key, i + 1, len(data))
    return pd.concat(outputs)


def get_doi(
    outcome_id: str,
    title: str,
    author: str,
    journal: str,
    publication_date: str,
    mailto: str,
    session: requests.Session
) -> Dict[str, str]:
    """
    Retrieves the DOI (Digital Object Identifier) for a given publication by querying
    the Crossref API.

    Args:
        outcome_id (str): The ID of the outcome.
        title (str): The title of the publication.
        author (str): The author(s) of the publication.
        journal (str): The journal of the publication.
        publication_date (str): The publication date of the publication.
        mailto (str): The email address to be used for API requests.
        session (requests.Session): The session object to be used for making HTTP requests.

    Returns:
        Dict[str, str]: A dictionary containing the DOI and other relevant information
        for the publication.
    """

    title = "".join([c for c in title if c.isalnum() or c.isspace()])
    query = f"{title}, {author}, {journal}, {publication_date}"
    url = f'https://api.crossref.org/works?query.bibliographic="{
        query}"&mailto={mailto}&rows=5'
    max_retries = 5
    attempts = 0

    while attempts < max_retries:
        attempts += 1
        logging.info("Attempt %s for: %s", attempts, query)
        try:
            response = session.get(url, timeout=20)
            data = response.json()

            processed_items = [
                process_item(item, title, author, journal, publication_date)
                for item in data["message"]["items"]
            ]

            res = [item for item in processed_items if item]
            return select_best_match(outcome_id, res)
        except KeyError as e:
            logging.warning("Missing key: %s", e)
        except Exception as e:  # pylint: disable=broad-except
            logging.warning("Error fetching data: %s", e)
        if attempts == max_retries:
            logging.error("Max retries reached for: %s", query)
            break
    return None


def crossref_doi_match(
    oa_data: pd.DataFrame, gtr_data: pd.DataFrame, mailto: str
) -> Generator[Dict[str, pd.DataFrame], None, None]:
    """
    Matches DOI values between two DataFrames using Crossref API.

    Args:
        oa_data (pd.DataFrame): DataFrame containing Open Access data.
        gtr_data (pd.DataFrame): DataFrame containing GTR data.
        mailto (str): Email address to be used for Crossref API requests.

    Yields:
        Generator[Dict[str, pd.DataFrame], None, None]: A generator that yields a dictionary
        with a single key-value pair. The key represents the batch number, and the value is
        a DataFrame containing the matched results for that batch.

    """
    gtr_data["doi"] = gtr_data["doi"].str.lower().str.extract(r"(10\..+)")
    oa_data["doi"] = oa_data["doi"].str.lower().str.extract(r"(10\..+)")
    unmatched_data = gtr_data[~gtr_data["doi"].isin(oa_data["doi"])]

    # create 5-item tuples for each row in unmatched_data
    inputs = unmatched_data[
        ["outcome_id", "title", "author", "journal_title", "publication_date"]
    ].to_dict(orient="records")
    cleaned_inputs = [clean_html_entities(record) for record in inputs]

    # create a number of batches from inputs
    input_batches = [
        cleaned_inputs[i: i + 250] for i in range(0, len(cleaned_inputs), 250)
    ]

    for i, batch in enumerate(input_batches):
        session = setup_session()
        logger.info("Processing batch %s / %s", i + 1, len(input_batches))
        results = Parallel(n_jobs=4, verbose=10)(
            delayed(get_doi)(
                x["outcome_id"],
                x["title"],
                x["author"],
                x["journal_title"],
                x["publication_date"],
                mailto,
                session,
            )
            for x in batch
        )

        # skip None, transform to dataframe
        results = [r for r in results if r]
        df = pd.DataFrame(results)

        if df.empty:
            continue

        # merge to dataframe of batch
        df = df.merge(pd.DataFrame(batch), on="outcome_id",
                      how="right", suffixes=("_cr", "_gtr"))
        yield {f"s{i}": df}


def concatenate_crossref(data: Dict[str, AbstractDataset]) -> pd.DataFrame:
    """
    Load the partitioned JSON dataset, iterate transforms, return dataframe.

    Args:
        data(Dict[str, AbstractDataset]): The partitioned JSON dataset.

    Returns:
        pd.DataFrame: The concatenated Crossref dataset.
    """
    outputs = []
    for i, (key, batch_loader) in enumerate(data.items()):
        data_batch = batch_loader()
        outputs.append(data_batch)
        logger.info("Loaded %s. Progress: %s/%s", key, i + 1, len(data))
    return pd.concat(outputs, ignore_index=True)
