import logging
from typing import Sequence
import pandas as pd
from .utils import get_intent, get_paper_details

logger = logging.getLogger(__name__)


def get_paper_data(
    oa_dataset: pd.DataFrame,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
):
    """
    Retrieves paper data from the Open Access dataset.

    Args:
        oa_dataset (pd.DataFrame): The Open Access dataset.
        base_url (str): The base URL for the API.
        fields (Sequence[str]): The fields to retrieve from the API.
        api_key (str): The API key for authentication.

    Returns:
        pd.DataFrame: The processed paper data.
    """
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")

    # get paper influential and PDF details
    processed_df = get_paper_details(
        oa_dataset=oa_dataset,
        base_url=base_url,
        fields=fields,
        api_key=api_key,
    )

    return processed_df


def get_citation_data(
    oa_dataset: pd.DataFrame,
    base_url: str,
    fields: Sequence[str],
    api_key: str,
    perpage: int = 500,
) -> pd.DataFrame:
    """
    Retrieves citation intent data from the GtR-OpenAlex dataset.

    Args:
        oa_dataset (pd.DataFrame): The OpenAlex dataset containing IDs.
        base_url (str): The base URL for the Semantic Scholar API.
        fields (List[str]): The fields to fetch from the API.
        api_key (str): The API key to use.
        perpage (int, optional): The number of citations to fetch per page.

    Returns:
        pd.DataFrame: The processed dataset with citation intent information.

    """
    oa_dataset = oa_dataset.drop_duplicates(subset="id")
    oa_dataset["doi"] = oa_dataset["doi"].str.extract(r"(10\..+)")

    # get the citation intent for each level
    processed_df = get_intent(
        oa_dataset=oa_dataset,
        base_url=base_url,
        fields=fields,
        api_key=api_key,
        perpage=perpage,
    )

    # check how often doi is empty and pmid is not
    logger.info(
        "Number of rows with empty doi and non-empty pmid: %d",
        processed_df[(processed_df["doi"] == "") & (processed_df["pmid"] != "")].shape[
            0
        ],
    )

    return processed_df
