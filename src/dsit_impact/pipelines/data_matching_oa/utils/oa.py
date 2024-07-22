import logging
from typing import Iterator, List, Dict, Sequence, Union, Generator, Optional
import time
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd

logger = logging.getLogger(__name__)


def _revert_abstract_index(abstract_inverted_index: Dict[str, Sequence[int]]) -> str:
    """Reverts the abstract inverted index to the original text.

    Args:
        abstract_inverted_index (Dict[str, Sequence[int]]): The abstract inverted index.

    Returns:
        str: The original text.
    """
    try:
        length_of_text = (
            max(
                [
                    index
                    for sublist in abstract_inverted_index.values()
                    for index in sublist
                ]
            )
            + 1
        )
        recreated_text = [""] * length_of_text

        for word, indices in abstract_inverted_index.items():
            for index in indices:
                recreated_text[index] = word

        return " ".join(recreated_text)
    except (AttributeError, ValueError):
        return ""


def _parse_results(
    response: List[Dict], keys_to_include: Optional[List[str]] = None
) -> List[Dict]:
    """Parses OpenAlex API response to retain specified keys or all if keys_to_include
     is None.

    Args:
        response (List[Dict]): The response from the OpenAlex API.
        keys_to_include (Optional[List[str]]): List of keys to include in the 
            result. Returns full dictionary if None.

    Returns:
        List[Dict]: A list of dictionaries containing the parsed information.
    """
    parsed_response = []
    for paper in response:
        parsed_paper = {
            "id": paper.get("id", "").replace("https://openalex.org/", ""),
            "doi": paper.get("doi", ""),
            "title": paper.get("title", ""),
            "publication_date": paper.get("publication_date", ""),
            "abstract": _revert_abstract_index(paper.get("abstract_inverted_index", {})),
            "authorships": paper.get("authorships", []),
            "cited_by_count": paper.get("cited_by_count", ""),
            "concepts": paper.get("concepts", []),
            "mesh_terms": paper.get("mesh", []),
            "topics": paper.get("topics", []),
            "grants": paper.get("grants", []),
            "referenced_works": paper.get("referenced_works", []),
            "ids": paper.get("ids", []),
            "counts_by_year": paper.get("counts_by_year", []),
        }
        if keys_to_include is not None:
            # Filter the dictionary to only include specified keys
            parsed_paper = {
                key: parsed_paper[key] for key in keys_to_include if key in parsed_paper
            }
        parsed_response.append(parsed_paper)
    return parsed_response


def preprocess_ids(
    ids: Union[str, List[str], Dict[str, str]], grouped: bool = True
) -> List[str]:
    """Preprocesses ids to ensure they are in the correct format."""
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(ids, dict):
        ids = list(ids.values())
    if grouped:
        ids = list(_chunk_oa_ids(ids))
    return ids


def _chunk_oa_ids(ids: List[str], chunk_size: int = 50) -> Generator[str, None, None]:
    """Yield successive chunk_size-sized chunks from ids."""
    for i in range(0, len(ids), chunk_size):
        yield "|".join(ids[i: i + chunk_size])


def _works_generator(
    mailto: str,
    perpage: str,
    oa_id: Union[str, List[str]],
    filter_criteria: Union[str, List[str]],
    session: requests.Session,
    sample_size: int = -1,
) -> Iterator[list]:
    """Creates a generator that yields a list of works from the OpenAlex API based on a
    given work ID.

    Args:
        mailto (str): The email address to use for the API.
        perpage (str): The number of results to return per page.
        oa_id (Union[str, List[str]): The work ID to use for the API.
        filter_criteria (Union[str, List[str]]): The filter criteria to use for the API.
        session (requests.Session): The requests session to use.

    Yields:
        Iterator[list]: A generator that yields a list of works from the OpenAlex API
        based on a given work ID.
    """
    cursor = "*"
    assert isinstance(
        filter_criteria, type(oa_id)
    ), "filter_criteria and oa_id must be of the same type."

    # multiple filter criteria
    if isinstance(filter_criteria, list) and isinstance(oa_id, list):
        filter_string = ",".join(
            [f"{criteria}:{id_}" for criteria,
                id_ in zip(filter_criteria, oa_id)]
        )
    else:
        filter_string = f"{filter_criteria}:{oa_id}"

    if sample_size == -1:
        cursor_url = (
            f"https://api.openalex.org/works?filter={filter_string}"
            f"&mailto={mailto}&per-page={perpage}&cursor={{}}"
        )

        try:
            # make a call to estimate total number of results
            response = session.get(cursor_url.format(cursor), timeout=20)
            data = response.json()

            while response.status_code == 429:  # needs testing (try with 200)
                logger.info("Waiting for 1 hour...")
                time.sleep(3600)
                response = session.get(cursor_url.format(cursor), timeout=20)
                data = response.json()

            logger.info("Fetching data for %s", oa_id[:50])
            total_results = data["meta"]["count"]
            num_calls = total_results // int(perpage) + 1
            logger.info("Total results: %s, in %s calls",
                        total_results, num_calls)
            while cursor:
                response = session.get(cursor_url.format(cursor), timeout=20)
                data = response.json()
                results = data.get("results")
                cursor = data["meta"].get("next_cursor", False)
                yield results

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error fetching data for %s: %s", oa_id, e)
            yield []
    else:  # OA does not accept cursor pagination with samples.
        cursor_url = (
            f"https://api.openalex.org/works?filter={filter_string}&seed=123"
            f"&mailto={
                mailto}&per-page={perpage}&sample={sample_size}&page={{}}"
        )

        try:
            # make a call to estimate total number of results
            response = session.get(cursor_url.format(1), timeout=20)
            data = response.json()

            while response.status_code == 429:  # needs testing (try with 200)
                logger.info("Waiting for 1 hour...")
                time.sleep(3600)
                response = session.get(cursor_url.format(1), timeout=20)
                data = response.json()

            logger.info("Fetching data for %s", oa_id[:50])
            total_results = data["meta"]["count"]
            num_calls = total_results // int(perpage) + 1
            logger.info("Total results: %s, in %s calls",
                        total_results, num_calls)
            for page in range(1, num_calls + 1):
                response = session.get(cursor_url.format(page), timeout=20)
                data = response.json()
                results = data.get("results")
                yield results

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error fetching data for %s: %s", oa_id, e)
            yield []


def fetch_papers_for_id(
    oa_id: Union[str, List[str]],
    mailto: str,
    perpage: str,
    filter_criteria: Union[str, List[str]],
    **kwargs,
) -> List[dict]:
    """Fetches all papers cited by a specific work ID."""
    assert isinstance(
        filter_criteria, type(oa_id)
    ), "filter_criteria and oa_id must be of the same type."
    papers_for_id = []
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.3)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    for page, papers in enumerate(
        _works_generator(
            mailto, perpage, oa_id,
            filter_criteria, session, sample_size=kwargs.get("sample_size", -1)
        )
    ):
        papers_for_id.extend(_parse_results(papers, kwargs.get("keys_to_include", None)))
        logger.info(
            "Fetching page %s. Total papers collected: %s",
            page,
            len(papers_for_id),
        )

    return papers_for_id


def json_loader(data: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
    """
    Load JSON data, transform it into a DataFrame, and wrangle data.

    Args:
        data (Dict[str, Union[str, List[str]]): The JSON data.

    Returns:
        pandas.DataFrame: The transformed DataFrame.

    """
    output = []

    for batch in data:
        json_data = [
            {
                k: v
                for k, v in item.items()
                if k
                in [
                    "id",
                    "ids",
                    "doi",
                    "title",
                    "publication_date",
                    "cited_by_count",
                    "counts_by_year",
                    "authorships",
                    "topics",
                    "concepts",
                    "grants",
                ]
            }
            for item in batch
        ]

        df = pd.DataFrame(json_data)
        if df.empty:
            continue

        df["pmid"] = df["ids"].apply(
            lambda x: (
                x.get("pmid").replace("https://pubmed.ncbi.nlm.nih.gov/", "")
                if x and x.get("pmid")
                else None
            )
        )

        df["mag_id"] = df["ids"].apply(
            lambda x: (x.get("mag") if x and x.get("mag") else None)
        )

        # break atuhorship nested dictionary jsons, create triplets of authorship
        df["authorships"] = df["authorships"].apply(
            lambda x: (
                [
                    (
                        (
                            author["author"]["id"].replace(
                                "https://openalex.org/", ""),
                            inst["id"].replace("https://openalex.org/", ""),
                            inst["country_code"],
                            author["author_position"],
                        )
                        if author["institutions"]
                        else [
                            author["author"]["id"].replace(
                                "https://openalex.org/", ""),
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

        # create tuples from counts by year, if available
        df["counts_by_year"] = df["counts_by_year"].apply(
            lambda x: (
                [
                    (year["year"], year["cited_by_count"])
                    for year in x
                ]
                if x
                else None
            )
        )

        # create a list of topics
        df["topics"] = df["topics"].apply(
            lambda x: (
                [
                    (
                        topic["id"].replace("https://openalex.org/", ""),
                        topic["display_name"],
                        topic["subfield"]["id"].replace(
                            "https://openalex.org/", ""),
                        topic["subfield"]["display_name"],
                        topic["field"]["id"].replace(
                            "https://openalex.org/", ""),
                        topic["field"]["display_name"],
                        topic["domain"]["id"].replace(
                            "https://openalex.org/", ""),
                        topic["domain"]["display_name"],
                    )
                    for topic in x
                ]
                if x
                else None
            )
        )

        # extract concepts
        df["concepts"] = df["concepts"].apply(
            lambda x: (
                [
                    (
                        concept["id"].replace("https://openalex.org/", ""),
                        concept["display_name"],
                    )
                    for concept in x
                ]
                if x
                else None
            )
        )

        # process grants, getting triplets out of "funder", "funder_display_name", and "award_id"
        df["grants"] = df["grants"].apply(
            lambda x: (
                [
                    (
                        grant.get("funder", {})
                        # .get("id", "")
                        .replace("https://openalex.org/", ""),
                        grant.get("funder_display_name"),
                        grant.get("award_id"),
                    )
                    for grant in x
                ]
                if x
                else None
            )
        )

        df = df[[
            "id",
            "doi",
            "pmid",
            "mag_id",
            "title",
            "publication_date",
            "cited_by_count",
            "counts_by_year",
            "authorships",
            "topics",
            "concepts",
            "grants",
        ]]

        # append to output
        output.append(df)

    df = pd.concat(output)

    return df
