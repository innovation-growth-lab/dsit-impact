import logging
from typing import Sequence, Dict, Union, List, Any
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)


def get_intent(oa_dataset: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Retrieves the intent data from the given OA dataset

    Args:
        oa_dataset (pd.DataFrame): The input OA dataset.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: A DataFrame containing the processed intent citations
            with the following columns:
            - parent_doi: The DOI of the parent publication.
            - pmid: The PMID of the citation.
            - doi: The DOI of the citation.
            - influential: Indicates whether the citation is influential.
            - intent: The intent of the citation.
            - context: The context of the citation.
    """
    inputs = oa_dataset.apply(
        lambda x: (x["id"], x["doi"], x["mag_id"], x["pmid"]),
        axis=1,
    ).tolist()

    s2_outputs = Parallel(n_jobs=8, verbose=10)(
        delayed(iterate_citation_detail_points)(*input, direction="citations", **kwargs)
        for input in inputs
    )

    s2_dict = dict(list(zip(oa_dataset["id"], s2_outputs)))

    processed_level_citations = process_citations(s2_dict)

    return pd.DataFrame(
        processed_level_citations,
        columns=[
            "id",
            "pmid",
            "doi",
            "mag_id",
            "is_open_access",
            "pdf_url",
            "influential",
            "intent",
            "context",
        ],
    )


def iterate_citation_detail_points(
    oa: str,
    doi: str,
    mag_id: str,
    pmid: str,
    direction: str,
    **kwargs,
) -> Dict[str, Union[str, Sequence[Dict[str, Any]]]]:
    """
    Iterates over citation detail points and fetches citation details based on the
        provided parameters.

    Args:
        oa (str): The OA (Open Access) identifier.
        parent_doi (str): The DOI (Digital Object Identifier) of the parent paper.
        doi (str): The DOI of the current paper.
        parent_pmid (str): The PubMed ID of the parent paper.
        pmid (str): The PubMed ID of the current paper.
        direction (str): The direction of the citation details to fetch. Can be
            "references" or "forward".
        **kwargs: Additional keyword arguments to pass to the fetch_citation_details
            function.

    Returns:
        Dict[str, Union[str, Sequence[Dict[str, Any]]]]: A dictionary containing the fetched
            citation details.

    """
    logger.info("Fetching citation details for %s", oa)
    for prefix, id_ in [("DOI:", doi), ("PMID:", pmid), ("MAG:", mag_id)]:
        if not id_:
            logger.warning("%s: No relevant %s id", oa, prefix[:-1])
            continue
        work_id = f"{prefix}{id_}"
        try:
            data = fetch_citation_details(
                work_id=work_id, direction=direction, **kwargs
            )
            logger.info("%s: Found citation details using %s", oa, work_id)
            return data
        except requests.exceptions.HTTPError as errh:
            logger.error("%s: HTTP Error: %s", oa, errh)
            return {}
        except requests.exceptions.ConnectionError as errc:
            logger.error("%s: Error Connecting: %s", oa, errc)
            return {}
        except requests.exceptions.Timeout as errt:
            logger.error("%s: Timeout Error: %s", oa, errt)
            return {}
        except requests.exceptions.RequestException as err:
            logger.error("%s: Something went wrong with the request: %s", oa, err)
            return {}
        except Exception:  # pylint: disable=broad-except
            logger.warning("Failed to fetch citation details for %s", oa)
            return {}
    return {}


def fetch_citation_details(
    work_id: str,
    base_url: str,
    direction: str,
    fields: Sequence[str],
    api_key: str,
    perpage: int = 500,
) -> Sequence[Dict[str, str]]:
    """
    Fetches citation details for a given work ID.

    Args:
        work_id (str): The work ID to fetch citation details for.
        base_url (str): The base URL for the API.
        direction (str): The direction of the citations.
        fields (Sequence[str]): The fields to fetch.
        api_key (str): The API key to use.
        perpage (int, optional): The number of citations to fetch per page.
            Defaults to 500.

    Returns:
        Sequence[Dict[str, str]]: A list of citation details.
    """
    offset = 0
    url = (
        f"{base_url}/{work_id}/{direction}?"
        f"fields={','.join(fields)}&offset={offset}&limit={perpage}"
    )

    headers = {"X-API-KEY": api_key}

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    data_list = []
    while True:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", [])
        data_list.extend(data)

        if len(data) < perpage:
            break

        offset += perpage
        url = (
            f"{base_url}/{work_id}/{direction}?"
            f"fields={','.join(fields)}&offset={offset}&limit={perpage}"
        )

    return data_list


def process_citations(citation_outputs: Dict[str, Sequence[Dict[str, Any]]]) -> List:
    """
    Process citation outputs and extract relevant information.

    Args:
        citation_outputs (Dict[str, Sequence[Dict[str, Any]]]): A dictionary
            containing citation outputs.

    Returns:
        List: A list of rows containing extracted information from the
            citation outputs.
    """
    rows = []
    for oa_id, outputs in citation_outputs.items():
        for output in outputs:
            context_intents = output.get("contextsWithIntent", [])
            influential = output.get("isInfluential", "")
            is_open_access = output.get("isOpenAccess", "")
            pdf_url = output.get("openAccessPdf", [])
            external_ids = output.get("citingPaper", {}).get("externalIds", {})
            if not external_ids:
                continue
            doi = external_ids.get("DOI", "")
            pmid = external_ids.get("PubMed", "")
            mag_id = external_ids.get("MAG", "")
            for item in context_intents:
                context = item.get("context", "")
                intents = item.get("intents") or [""]
                for intent in intents:
                    rows.append(
                        [
                            oa_id,
                            pmid,
                            doi,
                            mag_id,
                            is_open_access,
                            pdf_url,
                            influential,
                            intent,
                            context,
                        ]
                    )

    return rows


def get_paper_details(oa_dataset: pd.DataFrame, **kwargs):
    """
    Retrieves paper details for a given dataset.

    Args:
        oa_dataset (pd.DataFrame): The dataset containing paper information.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: A DataFrame containing processed paper details.

    """
    inputs = oa_dataset.apply(
        lambda x: (x["id"], x["doi"], x["mag_id"], x["pmid"]),
        axis=1,
    ).tolist()

    s2_outputs = Parallel(n_jobs=8, verbose=10)(
        delayed(iterate_paper_detail_points)(*input, **kwargs) for input in inputs
    )

    s2_dict = dict(list(zip(oa_dataset["id"], s2_outputs)))

    processed_details = process_paper_details(s2_dict)

    return pd.DataFrame(
        processed_details,
        columns=[
            "id",
            "influential",
            "is_open_access",
            "pdf_url",
        ],
    )


def iterate_paper_detail_points(
    oa: str,
    doi: str,
    mag_id: str,
    pmid: str,
    **kwargs,
) -> Dict[str, Union[str, Sequence[Dict[str, Any]]]]:
    """
    Iterates over paper detail points and fetches paper details based on the
        provided parameters.

    Args:
        oa (str): The OA (Open Access) identifier.
        doi (str): The DOI of the current paper.
        mag_id (str): The MAG ID of the current paper.
        pmid (str): The PubMed ID of the current paper.
        **kwargs: Additional keyword arguments to pass to the fetch_paper_details
            function.

    Returns:
        Dict[str, Union[str, Sequence[Dict[str, Any]]]]: A dictionary containing the
            fetched paper details.

    """
    logger.info("Fetching paper details for %s", oa)
    for prefix, id_ in [("DOI:", doi), ("PMID:", pmid), ("MAG:", mag_id)]:
        if not id_:
            logger.warning("%s: No relevant %s id", oa, prefix[:-1])
            continue
        work_id = f"{prefix}{id_}"
        try:
            data = fetch_paper_details(work_id=work_id, **kwargs)
            logger.info("%s: Found paper details using %s", oa, work_id)
            return data
        except requests.exceptions.HTTPError as errh:
            logger.error("%s: HTTP Error: %s", oa, errh)
            continue
        except requests.exceptions.ConnectionError as errc:
            logger.error("%s: Error Connecting: %s", oa, errc)
            continue
        except requests.exceptions.Timeout as errt:
            logger.error("%s: Timeout Error: %s", oa, errt)
            continue
        except requests.exceptions.RequestException as err:
            logger.error("%s: Something went wrong with the request: %s", oa, err)
            continue
        except Exception:  # pylint: disable=broad-except
            logger.warning("Failed to fetch paper details for %s", oa)
            continue
    return {}


def fetch_paper_details(
    work_id: str,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
) -> Sequence[Dict[str, str]]:
    """
    Fetches citation details for a given work ID.

    Args:
        work_id (str): The work ID to fetch citation details for.
        base_url (str): The base URL for the API.
        direction (str): The direction of the citations.
        fields (Sequence[str]): The fields to fetch.
        api_key (str): The API key to use.
        perpage (int, optional): The number of citations to fetch per page.
            Defaults to 500.

    Returns:
        Sequence[Dict[str, str]]: A list of citation details.
    """
    url = f"{base_url}/{work_id}?fields={','.join(fields)}"

    headers = {"X-API-KEY": api_key}

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    response = session.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def process_paper_details(paper_details: Dict[str, Sequence[Dict[str, Any]]]) -> List:
    """
    Process paper details and extract relevant information.

    Args:
        paper_details (Dict[str, Sequence[Dict[str, Any]]]): A dictionary
            containing paper details.
    Returns:
        List: A list of rows containing extracted information from the
            paper outputs.
    """
    rows = []
    for oa_id, output in paper_details.items():
        influential_count = output.get("influentialCitationCount", "")
        is_open_access = output.get("isOpenAccess", "")
        pdf_url_dict = output.get("openAccessPdf", {})
        if pdf_url_dict:
            pdf_url = pdf_url_dict.get("url", None)
        rows.append(
            [
                oa_id,
                influential_count,
                is_open_access,
                pdf_url,
            ]
        )
    return rows
