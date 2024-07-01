
"""
Utilities for data collection from OpenAlex.
"""

import logging
from typing import List, Dict, Union
import re
import requests
from html import unescape
from thefuzz import fuzz

logger = logging.getLogger(__name__)


def _process_string(s: str) -> Union[str, list]:
    # find all positions of the specified characters
    split_positions = [m.start() for m in re.finditer("[:\\(\\[]", s)]
    if not split_positions:
        s = "".join([c for c in s if c.isalnum() or c.isspace()])
        return [unescape(s.replace("&", "and"))]

    parts = []
    last_pos = 0
    for pos in split_positions:
        # include the part up to the current split position
        parts.append(s[last_pos:pos])
        last_pos = pos + 1
    parts.append(s[last_pos:])

    # create cumulative substrings
    cumulative_parts = []
    cumulative = ""
    for part in parts:
        part = "".join([c for c in part if c.isalnum() or c.isspace()])
        cumulative += (part if not cumulative else " " + part)
        cumulative_parts.append(unescape(cumulative.replace("&", "and")))

    return cumulative_parts


def clean_html_entities_for_oa(
    input_record: Dict[str, Union[str, int, float]]
) -> Dict[str, Union[str, int, float, Dict[str, str]]]:
    """
    Iterate over each key-value pair in the record and unescape HTML entities
    in string values. If ":", "(", or "[" is in the string, split and create a
    list of cumulative sub-strings.
    """
    return {
        key: value if key in ["outcome_id", "author", "publication_date"] 
        else _process_string(value) if isinstance(value, str) else value
        for key, value in input_record.items()
    }


def get_oa_match(
    outcome_id: str,
    title: Union[str, List[str]],
    chapterTitle: str,
    author: str,
    publication_date: str,
    config: Dict[str, str],
    session: requests.Session,
) -> List[Dict[str, str]]:
    """
    Retrieves Open Access (OA) matches based on the provided parameters.

    Args:
        outcome_id (str): The ID of the outcome.
        title (Union[str, List[str]]): The title(s) of the publication(s) 
        to match.
        chapterTitle (str): The chapter title of the publication.
        author (str): The author of the publication.
        publication_date (str): The publication date of the publication.
        config (Dict[str, str]): Configuration parameters.
        session (requests.Session): The session object for making HTTP 
        requests.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the matched 
        OA publications.
            Each dictionary contains the following keys:
            - "outcome_id": The ID of the outcome.
            - "id": The ID of the matched publication.
            - "title": The display name of the matched publication.
            - "doi": The DOI of the matched publication.
    """
    
    display_titles = title if not chapterTitle else chapterTitle
    mailto = config["mailto"]
    candidate_outputs = []
    for candidate_title in display_titles:
        logger.info("Processing title: %s", candidate_title)
        query = f"{candidate_title}"
        url = (
            f"https://api.openalex.org/works?filter=title.search:{query}"
            f"&mailto={mailto}&per-page=25"
        )
        max_retries = 5
        attempts = 0
        success = False  # Flag to indicate successful data fetch

        while attempts < max_retries and not success:
            attempts += 1
            logging.info("Attempt %s for: %s", attempts, query)
            if attempts == max_retries:
                logging.error("Max retries reached for: %s", query)
                break

            try:
                response = session.get(url, timeout=20)
                data = response.json()
                results = data.get("results")
                candidate_outputs.append(results)
                break
            except KeyError as e:
                logging.warning("Missing key: %s", e)
            except Exception as e:  # pylint: disable=broad-except
                logging.warning("Error fetching data: %s", e)

    # flatten list of candidates
    candidate_flat = [
        item for sublist in candidate_outputs for item in sublist]

    # keep candidates with one approximate author name
    matching_author = []
    for candidate_output in candidate_flat:
        authorships = candidate_output["authorships"]
        for authorship in authorships:
            candidate_author = authorship["author"]
            matched_author = author_fuzzy_match(author, candidate_author)
            if matched_author:
                matching_author.append(candidate_output)

    # keep candidates with up to 2 years difference in publication date
    matching_date = []
    if matching_author and publication_date:
        for candidate_output in matching_author:
            publication_year = candidate_output["publication_year"]
            year_diff = abs(int(publication_date[:4]) - int(publication_year))
            if year_diff < 2:
                matching_date.append(candidate_output)

    return_dicts = []
    ids = []
    if matching_date:
        for candidate_output in matching_date:
            cand_id = candidate_output["id"].replace("https://openalex.org/", "")
            if cand_id in ids:
                continue
            return_dict = {
                "outcome_id": outcome_id,
                "id": cand_id,
                "title": candidate_output["display_name"],
                "doi": candidate_output["doi"]
            }
            logger.info("Matched: %s", display_titles)
            return_dicts.append(return_dict)
            ids.append(cand_id)
    return return_dicts


def author_fuzzy_match(
    author: str,
    candidate_author: List[Dict[str, Union[str, Dict[str, str]]]]
):
    """
    Performs a fuzzy matching between the given author name and a 
    candidate author name.
    
    Args:
        author (str): The author name to match.
        candidate_author (List[Dict[str, Union[str, Dict[str, str]]]]): The 
        candidate author information.
        
    Returns:
        Dict[str, Union[str, Dict[str, str]]] or None: The candidate author 
        if the fuzzy score is above or equal to 75, otherwise None.
    """
    
    candidate_author_name = candidate_author["display_name"]
    author = " ".join([word for word in author.split() if len(word) > 1])
    fuzzy_score = fuzz.token_set_ratio(
        author.lower(), candidate_author_name.lower())
    if fuzzy_score >= 75:
        return candidate_author
    return None