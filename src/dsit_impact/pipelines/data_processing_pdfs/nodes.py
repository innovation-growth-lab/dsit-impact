"""
This script defines functions for processing PDF data and computing section
shares using Kedro.

Functions:
    - preprocess_for_section_collection: Preprocesses datasets for section
      collection.
    - get_citation_sections: Retrieves citation sections from PDFs based on
      Semantic Scholar and OpenAlex data.
    - get_browser_pdfs: Retrieves the content of PDF files based on the
      provided dataset.
    - compute_section_shares: Computes the section shares for each parent_id
      in the given loaders.

Dependencies:
    - pandas: For data manipulation and analysis.
    - joblib: For parallel processing.
    - logging: For logging information.
    - typing: For type hinting.
    - kedro: For creating and managing data pipelines.
    - utils: Contains utility functions `get_pdf_content` and
      `get_browser_pdf_object`.

Usage:
    Import the functions and use them to preprocess data, retrieve PDF content,
    and compute section shares.

Command Line Example:
    python nodes.py
"""

import logging
from typing import Sequence, Dict, Generator
import pandas as pd
from joblib import Parallel, delayed
from kedro.io import AbstractDataset
from .utils import get_pdf_content, get_browser_pdf_object

logger = logging.getLogger(__name__)


def preprocess_for_section_collection(
    oa_dataset: pd.DataFrame, s2_dataset: pd.DataFrame
) -> pd.DataFrame:
    """
    Preprocesses the datasets for section collection.

    Args:
        oa_dataset (pd.DataFrame): The dataset containing the open access articles.
        s2_dataset (pd.DataFrame): The dataset containing the semantic scholar articles.

    Returns:
        pd.DataFrame: The preprocessed merged dataset with grouped contexts.
    """

    s2_dataset.dropna(subset=["pdf_url"], inplace=True)
    oa_dataset = oa_dataset.drop_duplicates(subset=["id", "title"])
    merged_data = pd.merge(oa_dataset, s2_dataset, on="id", how="right")

    # first groupby creates lists of context
    merged_data = (
        merged_data.groupby(["id", "doi", "mag_id", "pmid", "title", "pdf_url"])[
            "context"
        ]
        .apply(list)
        .reset_index()
    )

    # second groupby guarantees unique pdf fetches
    final_data = (
        merged_data.groupby(["doi", "mag_id", "pmid", "pdf_url"])
        .agg(
            {
                "id": list,
                "title": list,
                "context": lambda x: list(x),  # pylint: disable=unnecessary-lambda
            }
        )
        .reset_index()
    )

    return final_data


def get_citation_sections(
    dataset: pd.DataFrame, main_sections: Sequence[str]
) -> Generator[Dict, None, None]:
    """
    Retrieves citation sections from the PDFs based on the Semantic Scholar + OA data.

    Args:
        oa_dataset (pd.DataFrame): The OpenAlex dataset containing IDs.
        s2_dataset (pd.DataFrame): The Semantic Scholar dataset containing titles.

    Returns:
        Dict: A dictionary containing the processed citation sections.

    """

    # split the dataset into chunks of 1_000
    dataset_chunks = [
        dataset.iloc[i : i + 1_000] for i in range(0, len(dataset), 1_000)
    ]

    for i, chunk in enumerate(dataset_chunks):
        logger.info("Processing chunk %d / %d", i, len(dataset_chunks))
        # get the PDF content
        processed_data = get_pdf_content(
            dataset=chunk,
            main_sections=main_sections,
        )
        logger.info("Processed chunk %d / %d", i, len(dataset_chunks))

        processed_df = pd.DataFrame(
            [item for sublist in processed_data for item in sublist],
            columns=[
                "parent_id",
                "doi",
                "mag_id",
                "pmid",
                "section_index",
                "section_heading",
                "main_section_heading",
            ],
        )

        yield {f"s{i}": processed_df}


def get_browser_pdfs(dataset: pd.DataFrame):
    """
    Retrieves the content of PDF files based on the provided dataset.

    Args:
        dataset (pd.DataFrame): The dataset containing 'id', 'pdf_url',
            'title', and 'context' columns.

    Returns:
        list: A list of paper sections extracted from the PDF files.
    """
    dataset = dataset[["doi", "mag_id", "pmid", "pdf_url"]].drop_duplicates()
    dataset["combined_id"] = (
        dataset["doi"].astype(str)
        + "_"
        + dataset["mag_id"].astype(str)
        + "_"
        + dataset["pmid"].astype(str)
    )
    inputs = dataset.apply(lambda x: [x["combined_id"], x["pdf_url"]], axis=1).tolist()
    input_inner_batches = [inputs[i : i + 50] for i in range(0, len(inputs), 50)]
    input_batches = [
        input_inner_batches[i : i + 15] for i in range(0, len(input_inner_batches), 15)
    ]
    for i, batch in enumerate(input_batches):
        logger.info("Processing batch %d / %d", i, len(input_batches))
        pdfs = Parallel(n_jobs=10, verbose=10)(
            delayed(get_browser_pdf_object)(input) for input in batch
        )
        # flatten
        pdfs = [pdf for pdf_batch in pdfs for pdf in pdf_batch]
        pdfs = [(filename, pdf) for filename, pdf in pdfs if isinstance(pdf, bytes)]
        yield {f"s{i}": pdfs}


def compute_section_shares(section_details: AbstractDataset) -> pd.DataFrame:
    """
    Compute the section shares for each parent_id in the given loaders.

    Parameters:
    loaders (AbstractDataset): A dictionary-like object containing loaders.

    Returns:
    section_data (DataFrame): A DataFrame containing the computed section shares for
        each parent_id.
    """
    section_data = []
    for i, loader in enumerate(section_details.values()):
        logger.info("Processing loader %d / %d", i, len(section_details))
        data = loader()
        data = data.drop_duplicates(subset=["parent_id", "doi", "main_section_heading"])

        pivot_table = data.pivot_table(
            index="parent_id",
            columns="main_section_heading",
            values="doi",
            aggfunc="count",
            fill_value=0,
        ).reset_index()

        pivot_table["total_sections"] = pivot_table[
            [col for col in pivot_table.columns if col != "parent_id"]
        ].sum(axis=1)

        section_data.append(pivot_table)

    section_data = pd.concat(section_data, ignore_index=True)
    section_data = section_data.groupby("parent_id").sum().reset_index()

    return section_data
