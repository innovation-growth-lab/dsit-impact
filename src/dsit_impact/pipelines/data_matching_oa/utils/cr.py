import logging
from typing import List, Dict, Union
import requests
from requests.adapters import HTTPAdapter, Retry
from html import unescape
from thefuzz import fuzz

logger = logging.getLogger(__name__)


def _process_item(
    item: Dict[str, Union[str, Dict[str, str]]],
    title: str,
    author: str,
    journal: str,
    publication_date: str
) -> Union[Dict[str, Union[str, int, float]], None]:
    """
    Process an item and return a dictionary containing relevant information if the
    item meets certain criteria.

    Args:
        item (Dict[str, Union[str, Dict[str, str]]]): The item to be processed.
        title (str): The title to compare with the item's title.
        author (str): The author to compare with the item's author.
        journal (str): The journal to compare with the item's journal.
        publication_date (str): The publication date to compare with the item's year.

    Returns:
        Union[Dict[str, Union[str, int, float]], None]: A dictionary containing relevant
        information if the item meets the criteria, or None if the item does not meet the
        criteria.
    """
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


def clean_html_entities(
    input_record: Dict[str, Union[str, int, float]]
) -> Dict[str, Union[str, int, float]]:
    """
    Iterate over each key-value pair in the record and unescape HTML entities
    in string values
    """
    return {
        key: unescape(value.replace("&", "and")) if isinstance(
            value, str) else value
        for key, value in input_record.items()
    }


def _select_best_match(
    outcome_id: str, matches: List[Dict[str, Union[str, int, float]]]
) -> Union[Dict[str, Union[str, int, float]], None]:
    """
    Selects the best match from a list of matches based on a composite score.

    Args:
        outcome_id (str): The ID of the outcome.
        matches (List[Dict[str, Union[str, int, float]]]): A list of dictionaries
        representing the matches.

    Returns:
        Union[Dict[str, Union[str, int, float]], None]: The best match dictionary
        or None if no match is found.
    """
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
    """
    Set up and configure a session for making HTTP requests.

    Returns:
        requests.Session: The configured session object.
    """
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
                _process_item(item, title, author, journal, publication_date)
                for item in data["message"]["items"]
            ]

            res = [item for item in processed_items if item]
            return _select_best_match(outcome_id, res)
        except KeyError as e:
            logging.warning("Missing key: %s", e)
        except Exception as e:  # pylint: disable=broad-except
            logging.warning("Error fetching data: %s", e)
        if attempts == max_retries:
            logging.error("Max retries reached for: %s", query)
            break
    return None
