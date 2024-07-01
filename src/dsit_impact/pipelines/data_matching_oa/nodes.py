"""
This is a boilerplate pipeline 'data_matching_oa'
generated using Kedro 0.19.6
"""

import logging
from typing import List, Dict, Union, Callable
from html import unescape
import pandas as pd
from joblib import Parallel, delayed
import requests
from requests.adapters import HTTPAdapter, Retry
from thefuzz import fuzz
from kedro.io import AbstractDataset
from .utils import fetch_papers_for_id, preprocess_ids, json_loader


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
    doi_singleton_list = df[df["doi"].notnull()]["doi"].drop_duplicates().tolist()

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
    oa_id_chunks = [ids[i : i + 80] for i in range(0, len(ids), 80)]
    logger.info("Slicing data. Number of oa_id_chunks: %s", len(oa_id_chunks))
    return {
        f"s{str(i)}": lambda chunk=chunk: Parallel(n_jobs=parallel_jobs, verbose=10)(
            delayed(fetch_papers_for_id)(oa_id, mailto, perpage, filter_criteria)
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


def get_doi(outcome_id, title, author, journal, publication_date, mailto, session):
    title = "".join([c for c in title if c.isalnum() or c.isspace()])
    query = f"{title}, {author}, {journal}, {publication_date}"
    url = f'https://api.crossref.org/works?query.bibliographic="{query}"&mailto={mailto}&rows=5'
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


def process_item(item, title, author, journal, publication_date):
    try:
        year = item["issued"]["date-parts"][0][0]
        cr = {
            "title": item["title"][0],
            "author": f"{item['author'][0]['family']}, {item['author'][0]['given']}",
            "journal": item["container-title"][0],
            "year": year,
            "doi": item["DOI"].lower(),
            "score": item["score"],
            "year_diff": abs(year - int(publication_date[:4])),
        }
        # calculate fuzzy scores and average them
        fuzzy_scores = [
            fuzz.token_set_ratio(title.lower(), cr["title"].lower()),
            fuzz.token_set_ratio(author.lower(), cr["author"].lower()),
        ]
        if journal:
            fuzzy_scores.append(fuzz.token_set_ratio(journal, cr["journal"]))
        cr["fuzzy_score"] = sum(fuzzy_scores) / len(fuzzy_scores)

        return cr if cr["year_diff"] <= 1 else None
    except KeyError as e:
        logger.warning("Missing key: %s", e)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("Error processing item: %s", e)
    return None


def clean_html_entities(input_record):
    # Iterate over each key-value pair in the record and unescape HTML entities in string values
    return {
        key: unescape(value.replace("&", "and")) if isinstance(value, str) else value
        for key, value in input_record.items()
    }


def select_best_match(outcome_id, matches):
    best_match = None
    highest_score = 0
    for match in matches:
        cr_score = match["score"]
        cr_fuzzy_score = match["fuzzy_score"]

        composite_score = cr_score + cr_fuzzy_score

        if all([composite_score > highest_score, cr_score > 60, cr_fuzzy_score > 60]):
            highest_score = composite_score
            match["outcome_id"] = outcome_id
            best_match = match

    return best_match


def setup_session():
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def crossref_doi_match(oa_data: pd.DataFrame, gtr_data: pd.DataFrame, mailto: str):
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
        cleaned_inputs[i : i + 250] for i in range(0, len(cleaned_inputs), 250)
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
        df = df.merge(pd.DataFrame(batch), on="outcome_id", how="right", suffixes=("_cr", "_gtr"))
        yield {f"s{i}": df}

def concatenate_crossref(data: Dict[str, AbstractDataset]) -> pd.DataFrame:
    """
    Load the partitioned JSON dataset, iterate transforms, return dataframe.

    Args:
        data (Dict[str, AbstractDataset]): The partitioned JSON dataset.

    Returns:
        pd.DataFrame: The concatenated Crossref dataset.
    """
    outputs = []
    for i, (key, batch_loader) in enumerate(data.items()):
        data_batch = batch_loader()
        outputs.append(data_batch)
        logger.info("Loaded %s. Progress: %s/%s", key, i + 1, len(data))
    return pd.concat(outputs, ignore_index=True)