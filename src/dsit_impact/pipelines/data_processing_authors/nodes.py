"""
This is a boilerplate pipeline 'data_processing_authors'
generated using Kedro 0.19.6
"""

import logging
from typing import Sequence, Dict, Generator, Union
import pandas as pd
from joblib import Parallel, delayed

from ..data_matching_oa.utils.oa import fetch_papers_for_id


logger = logging.getLogger(__name__)


def create_author_list(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    This function creates a list of authors from the author column
    """
    authors = (
        input_df["authorships"]
        .apply(lambda x: [y[0] for y in x] if x is not None else [])
        .explode("authorships")
        .astype(str)
        .unique()
        .tolist()
    )
    # batch authors for OA queries
    authors = [authors[i : i + 1_250] for i in range(0, len(authors), 1_250)]
    return authors


def fetch_author_papers(
    authors: Sequence[str],
    mailto: str,
    perpage: int,
    filter_criteria: Union[str, Sequence[str]],
) -> Generator[Dict[str, pd.DataFrame], None, None]:
    """
    Fetches papers for a given list of authors.

    Args:
        authors (Sequence[str]): A sequence of authors to fetch papers for.
        mailto (str): The email address to be used for API requests.
        perpage (int): The number of papers to fetch per page.
        filter_criteria (Union[str, Sequence[str]]): The filter criteria to be used for
            fetching papers.

    Yields:
        Generator[Dict[str, pd.DataFrame], None, None]: A generator that yields a dictionary
            containing author papers.

    """
    for i, author_batch in enumerate(authors):
        author_inputs = [
            ["2005-12-31", "|".join(author_batch[i : i + 50])]
            for i in range(0, len(author_batch), 50)
        ]

        logger.info("Fetching papers for authors batch %s / %s", i + 1, len(authors))
        author_papers = Parallel(n_jobs=8, verbose=10)(
            delayed(fetch_papers_for_id)(
                oa_id=[publication_date, author_list],
                mailto=mailto,
                perpage=perpage,
                filter_criteria=filter_criteria,
                sample_size=100 * len(author_list.split("|")),
                keys_to_include=["id", "publication_date", "topics", "authorships"],
            )
            for publication_date, author_list in author_inputs
        )

        logger.info("Concatenating author papers")
        logger.info("Creating author column")
        author_df = []
        for paper_group in author_papers:
            author_df.append(pd.DataFrame(paper_group))
        author_df = pd.concat(author_df, ignore_index=True)

        author_df = postprocess_results(author_df, author_batch)

        yield {f"s{i}": author_df}


def postprocess_results(
    dataframe: pd.DataFrame, input_authors: Sequence[str]
) -> pd.DataFrame:
    """
    Postprocesses the results by adding author and topic information to the dataframe.

    Args:
        dataframe (pd.DataFrame): The input dataframe containing the results.
        input_authors (Sequence[str]): A sequence of input authors.

    Returns:
        pd.DataFrame: The dataframe with author and topic information added.
    """

    logger.info("Creating author column")
    dataframe["authorships"] = dataframe["authorships"].apply(
        lambda x: (
            [
                (
                    (
                        author["author"]["id"].replace("https://openalex.org/", ""),
                        inst["id"].replace("https://openalex.org/", ""),
                        inst["country_code"],
                        author["author_position"],
                    )
                    if author["institutions"]
                    else [
                        author["author"]["id"].replace("https://openalex.org/", ""),
                        "",
                        "",
                        author["author_position"],
                    ]
                )
                for author in x
                for inst in author["institutions"] or [{}]
            ]
            if x
            else None
        )
    )

    dataframe["author"] = dataframe["authorships"].apply(
        lambda x: [y[0] for y in x if y[0] in input_authors] if x is not None else []
    )

    logger.info("Extracting relevant topic info")
    dataframe["topics"] = dataframe["topics"].apply(
        lambda x: (
            [
                (
                    (
                        topic.get("id", "").replace("https://openalex.org/", "")
                        if topic.get("id")
                        else ""
                    ),
                    topic.get("display_name", "") if topic.get("display_name") else "",
                    (
                        topic.get("subfield", {})
                        .get("id", "")
                        .replace("https://openalex.org/", "")
                        if topic.get("subfield")
                        else ""
                    ),
                    (
                        topic.get("subfield", {}).get("display_name", "")
                        if topic.get("subfield")
                        else ""
                    ),
                    (
                        topic.get("field", {})
                        .get("id", "")
                        .replace("https://openalex.org/", "")
                        if topic.get("field")
                        else ""
                    ),
                    (
                        topic.get("field", {}).get("display_name", "")
                        if topic.get("field")
                        else ""
                    ),
                    (
                        topic.get("domain", {})
                        .get("id", "")
                        .replace("https://openalex.org/", "")
                        if topic.get("domain")
                        else ""
                    ),
                    (
                        topic.get("domain", {}).get("display_name", "")
                        if topic.get("domain")
                        else ""
                    ),
                )
                for topic in x
            ]
            if x
            else None
        )
    )

    logger.info("Exploding matched authors")
    dataframe = dataframe.explode("author")
    return dataframe[["id", "author", "publication_date", "topics"]]
