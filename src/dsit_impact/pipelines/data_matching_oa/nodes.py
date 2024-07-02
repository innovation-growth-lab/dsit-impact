"""
This is a boilerplate pipeline 'data_matching_oa'
generated using Kedro 0.19.6
"""

import logging
from typing import List, Dict, Union, Callable, Generator
import pandas as pd
from joblib import Parallel, delayed
from thefuzz import fuzz  # pylint: disable=import-error
from kedro.io import AbstractDataset
from .utils.oa import fetch_papers_for_id, preprocess_ids, json_loader
from .utils.cr import clean_html_entities, setup_session, get_doi
from .utils.oa_match import clean_html_entities_for_oa, get_oa_match, get_best_match


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
        df = df.merge(
            pd.DataFrame(batch), on="outcome_id", how="right", suffixes=("_cr", "_gtr")
        )
        yield {f"s{i}": df}


def oa_search_match(
    oa_data: pd.DataFrame,
    gtr_data: pd.DataFrame,
    config: Dict[str, Union[str, Dict[str, str]]],
) -> Generator[Dict[str, pd.DataFrame], None, None]:
    """
    Perform OA search and matching for GTR data.

    Args:
        oa_data (pd.DataFrame): DataFrame containing OA data.
        gtr_data (pd.DataFrame): DataFrame containing GTR data.
        config (Dict[str, Union[str, Dict[str, str]]]): Configuration parameters.

    Yields:
        Generator[Dict[str, pd.DataFrame], None, None]: A generator that yields a dictionary
        containing the results of each batch.

    """
    gtr_data["doi"] = gtr_data["doi"].str.lower().str.extract(r"(10\..+)")
    oa_data["doi"] = oa_data["doi"].str.lower().str.extract(r"(10\..+)")
    unmatched_data = gtr_data[~gtr_data["doi"].isin(oa_data["doi"])]

    inputs = unmatched_data[
        ["outcome_id", "title", "chapterTitle", "author", "publication_date"]
    ].to_dict(orient="records")

    cleaned_inputs = [clean_html_entities_for_oa(record) for record in inputs]

    input_batches = [
        cleaned_inputs[i : i + 250] for i in range(0, len(cleaned_inputs), 250)
    ]

    for i, batch in enumerate(input_batches):
        session = setup_session()
        logger.info("Processing batch %s / %s", i + 1, len(input_batches))
        results = Parallel(n_jobs=8, verbose=10)(
            delayed(get_oa_match)(
                x["outcome_id"],
                x["title"],
                x["chapterTitle"],
                x["author"],
                x["publication_date"],
                config,
                session,
            )
            for x in batch
        )

        # skip None, transform to dataframe
        results = [r for r in results if r]
        df = pd.DataFrame([item for sublist in results for item in sublist])
        df = df.drop_duplicates(subset=["outcome_id", "id", "doi"])

        if df.empty:
            continue

        # merge to dataframe of batch
        df = df.merge(
            pd.DataFrame(batch), on="outcome_id", how="right", suffixes=("_oa", "_gtr")
        )
        yield {f"s{i}": df}


def concatenate_matches(data: Dict[str, AbstractDataset]) -> pd.DataFrame:
    """
    Load the partitioned JSON dataset, iterate transforms, return dataframe.

    Args:
        data(Dict[str, AbstractDataset]): The partitioned parquet dataset.

    Returns:
        pd.DataFrame: The concatenated Crossref dataset.
    """
    outputs = []
    for i, (key, batch_loader) in enumerate(data.items()):
        data_batch = batch_loader()
        outputs.append(data_batch)
        logger.info("Loaded %s. Progress: %s/%s", key, i + 1, len(data))
    return pd.concat(outputs, ignore_index=True)


def oa_filter(data: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the given DataFrame based on the 'outcome_id' column and returns the
    filtered DataFrame with the best OpenAlex match, based on fuzzy string matching
    and completeness of the associated metadata.

    Parameters:
    data (pd.DataFrame): The input DataFrame containing the data to be filtered.

    Returns:
    pd.DataFrame: The filtered DataFrame.

    """
    data["doi"] = data["doi"].str.extract(r"(10\..+)")
    data["title_gtr"] = data["title_gtr"].apply(lambda x: x[-1])
    data.drop_duplicates(
        subset=["outcome_id", "id", "title_oa", "doi", "title_gtr", "author"],
        inplace=True,
    )
    best_matches = []
    total_length = len(data["outcome_id"].unique())
    for i, (_, group) in enumerate(data.groupby("outcome_id")):
        logger.info("Processing group %s / %s", i + 1, total_length)
        best_matches.append(get_best_match(group))
    return pd.concat(best_matches, ignore_index=True)


def select_better_match(crossref: pd.DataFrame, openalex: pd.DataFrame) -> pd.DataFrame:
    """
    Selects the better match between crossref and openalex dataframes based
    on certain criteria.

    Args:
        crossref (pd.DataFrame): The dataframe containing crossref data.
        openalex (pd.DataFrame): The dataframe containing openalex data.

    Returns:
        pd.DataFrame: The dataframe with the better match selected for each outcome.

    """
    # drop outcome duplicates
    crossref.drop_duplicates(subset=["outcome_id"], inplace=True)
    openalex.drop_duplicates(subset=["outcome_id"], inplace=True)

    # dropna titles
    crossref.dropna(subset=["title_cr"], inplace=True)
    openalex.dropna(subset=["title_oa"], inplace=True)

    # rename columns pre-concat
    crossref.rename(
        columns={
            "title_cr": "title_match",
            "author_cr": "author_match",
            "doi": "doi_match",
        },
        inplace=True,
    )
    openalex.rename(
        columns={"title_oa": "title_match", "id": "id_match", "doi": "doi_match"},
        inplace=True,
    )

    # drop columns
    crossref.drop(columns=["score"], inplace=True)

    # create source columns
    crossref["source"] = "cr"
    openalex["source"] = "oa"

    # concatenate
    match_data = pd.concat([crossref, openalex], ignore_index=True)
    total_unique_outcomes = len(match_data["outcome_id"].unique())

    results = []
    for i, outcome_id in enumerate(match_data["outcome_id"].unique(), start=1):
        logger.info("Processing outcome %s / %s", i + 1, total_unique_outcomes)
        results.append(break_ties(match_data[match_data["outcome_id"] == outcome_id]))

    return pd.concat(results, ignore_index=True)


def break_ties(group: pd.DataFrame) -> pd.DataFrame:
    """
    Breaks ties between matching records based on similarity scores, DOI presence, 
    and source preference.

    Args:
        group (pandas.DataFrame): A group of matching records.

    Returns:
        pandas.DataFrame: The best matching record based on tie-breaking rules.
    """

    # Compute similarity scores
    group["similarity"] = group.apply(
        lambda x: fuzz.token_set_ratio(x["title_match"], x["title_gtr"]), axis=1
    )

    # Sort by similarity, then by presence of DOI, then prefer 'oa' source
    group_sorted = group.sort_values(
        by=["similarity", "doi_match", "source"], ascending=[False, False, True]
    )

    # Apply tie-breaking rules
    if (
        len(group_sorted) > 1
        and abs(group_sorted.iloc[0]["similarity"] - group_sorted.iloc[1]["similarity"])
        <= 5
    ):
        # If the top two are within 5 points, check DOI and source
        if pd.notnull(group_sorted.iloc[0]["doi_match"]) and pd.notnull(
            group_sorted.iloc[1]["doi_match"]
        ):
            # If both have DOI, prefer 'oa' source
            best_match = group_sorted[group_sorted["source"] == "oa"].head(1)
        else:
            # Else, select the one with a DOI
            best_match = group_sorted[pd.notnull(group_sorted["doi_match"])].head(1)
    else:
        # Else, select the top one
        best_match = group_sorted.head(1)

    return best_match
