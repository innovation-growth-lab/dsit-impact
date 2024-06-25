"""
This is a boilerplate pipeline 'data_matching_oa'
generated using Kedro 0.19.6
"""

import logging
from typing import List, Dict, Union, Callable
import pandas as pd
from joblib import Parallel, delayed
from .utils import fetch_papers_for_id, preprocess_ids

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
        df["doi"] = (
            df["doi"]
            .str.replace("/dx.", "/", regex=True)
            .str.replace("http:", "https:", regex=False)
            .str.split()
            .str[0]
            .apply(lambda x: x if isinstance(x, str) and x.count(":") <= 1 else None)
        )
    return df


def create_list_doi_inputs(df: pd.DataFrame) -> list:
    """Create a list of doi values from the Gateway to Research publication data.

    Args:
        df (pd.DataFrame): The Gateway to Research publication data.

    Returns:
        list: A list of doi values.
    """
    doi_singleton_list = df[df["doi"].notnull()]["doi"].drop_duplicates().tolist()

    # concatenate doi values to create group querise
    doi_list = preprocess_ids(doi_singleton_list, grouped=True)

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
