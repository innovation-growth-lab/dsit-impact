"""
This script provides functionality for preprocessing GtR (Gateway to Research)
data. It includes a class `GtRDataPreprocessor` with methods to preprocess
different types of GtR data such as organisations, funds, publications, and
projects. Additionally, it includes utility functions for API configuration
and data extraction.

Classes:
    GtRDataPreprocessor: A class for preprocessing various types of GtR data.

Functions:
    fetch_gtr_data(parameters, endpoint, **kwargs): Fetches data from the GtR
        API and preprocesses it.
    concatenate_endpoint(abstract_dict): Concatenates DataFrames from a single
        endpoint into a single DataFrame.

Dependencies:
    - logging
    - random
    - time
    - datetime
    - numpy
    - pandas
    - requests
    - kedro
"""

import logging
import random
import time
import datetime
from typing import Dict, Union, Generator
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from kedro.io import AbstractDataset
from .utils import (
    api_config,
    extract_main_address,
    extract_value_from_nested_dict,
    transform_nested_dict,
)

logger = logging.getLogger(__name__)


class GtRDataPreprocessor:
    """
    Class for preprocessing GtR data.

    This class provides methods to preprocess different types of GtR data, such as organisations,
    funds, publications, and projects.
    """

    def __init__(self) -> None:
        self.methods = {
            "organisations": self._preprocess_organisations,
            "funds": self._preprocess_funds,
            "publications": self._preprocess_publications,
            "projects": self._preprocess_projects,
        }

    def _preprocess_organisations(self, org_df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the organisations data.

        It extracts the main address and drops the "links" column.

        Args:
            org_df (pd.DataFrame): The organisations data.

        Returns:
            pd.DataFrame: The preprocessed data.
        """
        address_columns = org_df["addresses"].apply(extract_main_address)
        address_columns = address_columns.drop("created", axis=1).add_prefix("address_")
        org_df = org_df.drop("addresses", axis=1).join(address_columns)
        org_df = org_df.drop(columns=["links"])
        return org_df

    def _preprocess_funds(self, funds_df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the funds data.

        Extracts the value in pound (ie. {'currencyCode': 'GBP', 'amount': 283590})
        for each row and drops the "links" column.

        Args:
            funds_df (pd.DataFrame): The funds data.

        Returns:
            pd.DataFrame: The preprocessed data.
        """
        funds_df["value"] = funds_df["valuePounds"].apply(lambda x: x["amount"])
        funds_df = funds_df.drop("valuePounds", axis=1)
        funds_df = funds_df.drop(columns=["links"])
        return funds_df

    def _preprocess_publications(self, publications_df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocesses the publications DataFrame by extracting project_id, renaming
        columns, and selecting specific columns.

        Args:
            publications_df (pd.DataFrame): The input DataFrame containing publications data.

        Returns:
            pd.DataFrame: The preprocessed DataFrame with selected columns.

        """
        # extract project_id
        publications_df["project_id"] = publications_df["links"].apply(
            lambda x: extract_value_from_nested_dict(
                data=x,
                outer_key="link",
                inner_key="rel",
                inner_value="PROJECT",
                extract_key="href",
            )
        )

        # create publication_date from datePublished (miliseconds)
        publications_df["publication_date"] = publications_df["datePublished"].apply(
            lambda x: (
                datetime.datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d")
                if np.isfinite(x)
                else np.nan
            )
        )

        # rename cols
        publications_df = publications_df.rename(
            columns={
                "id": "outcome_id",
                "journalTitle": "journal_title",
                "publicationUrl": "publication_url",
            }
        )

        return publications_df[
            [
                "project_id",
                "outcome_id",
                "pubMedId",
                "isbn",
                "issn",
                "doi",
                "title",
                "chapterTitle",
                "type",
                "publication_date",
                "journal_title",
                "publication_url",
                "author",
            ]
        ]

    def _preprocess_projects(self, projects_df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the projects data."""
        columns_to_transform = {
            "identifiers": ["value", "type"],
            "researchSubjects": ["id", "text", "percentage"],
            "researchTopics": ["id", "text", "percentage"],
        }

        for col, keys in columns_to_transform.items():
            projects_df = transform_nested_dict(projects_df, col, keys)

        # rename cols
        projects_df = projects_df.rename(
            columns={
                "grantCategory": "grant_category",
                "abstractText": "abstract_text",
                "leadFunder": "lead_funder",
                "leadOrganisationDepartment": "lead_org_department",
                "researchTopics": "research_topics",
                "researchSubjects": "research_subjects",
                "id": "project_id",
            }
        )

        return projects_df[
            [
                "project_id",
                "identifiers",
                "title",
                "abstract_text",
                "status",
                "grant_category",
                "lead_funder",
                "lead_org_department",
                "research_topics",
                "research_subjects",
            ]
        ]


def fetch_gtr_data(
    parameters: Dict[str, Union[str, int]], endpoint: str, **kwargs
) -> Generator[pd.DataFrame, None, None]:
    """Fetch data from the GtR API.

    Args:
        parameters (Dict[str, Union[str, int]]): Parameters for the API request.
        endpoint (str): The endpoint to fetch data from.

    Returns:
        List[Dict[str, Any]]: The fetched data.
    """
    config = api_config(parameters, endpoint)

    page = 1
    total_pages = 1
    preprocessor = GtRDataPreprocessor()

    while page <= total_pages:
        page_data = []
        url = f"{config['base_url']}{endpoint}?p={page}&s={config['page_size']}"
        session = requests.Session()
        retries = Retry(
            total=config["max_retries"], backoff_factor=config["backoff_factor"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(url, headers=config["headers"], timeout=30)
        try:
            data = response.json()
            if "totalPages" in data and page == 1:
                logger.info("Total pages: %s", data["totalPages"])
                total_pages = (
                    2 if kwargs.get("test_mode") is True else data["totalPages"]
                )
            if config["key"] in data:
                items = data[config["key"]]
                if not items:
                    logger.info("No more data to fetch. Exiting loop.")
                    break
                for item in items:
                    item["page_fetched_from"] = page  # add page info
                    page_data.append(item)
            else:
                logger.error("No '%s' key found in the response", config["key"])
                continue
        except ValueError:  # [HACK] includes simplejson.decoder.JSONDecodeError
            logger.error("Failed to decode JSON response")
            continue

        logger.info("Fetched page %s / %s", page, total_pages)
        page += 1
        time.sleep(random.uniform(0.1, 0.3))  # [HACK] Respect web etiquette

        # preprocess before save
        page_df = pd.DataFrame(page_data)
        preprocessor = GtRDataPreprocessor()
        page_df = preprocessor.methods[endpoint.split("/")[-1]](page_df)
        yield {f"p{page-1}": page_df}


def concatenate_endpoint(
    abstract_dict: Union[AbstractDataset, Dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    """
    Concatenate DataFrames from a single endpoint into a single DataFrame.

    Args:
        abstract_dict (AbstractDataset): A dictionary where the keys are
            the endpoint names and the values are functions that load the
            DataFrames for each endpoint.

    Returns:
        pd.DataFrame: The concatenated DataFrame.
    """
    dataframes = []
    for i, (key, load_function) in enumerate(abstract_dict.items()):
        logger.info(
            "Adding DataFrame for %s. Number: %s / %s", key, i, len(abstract_dict)
        )
        try:
            try:
                df = load_function()
            except TypeError:
                df = load_function
            dataframes.append(df)
        except Exception as e:  # pylint: disable=broad-except
            print(f"Failed to load DataFrame for {key}: {e}")

    # concatenate all DataFrames into a single DataFrame
    concatenated_df = pd.concat(dataframes, ignore_index=True)

    return concatenated_df
