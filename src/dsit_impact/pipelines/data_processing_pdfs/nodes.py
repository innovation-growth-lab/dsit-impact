"""
This is a boilerplate pipeline 'data_processing_pdfs'
generated using Kedro 0.19.6
"""

import os
import logging
from typing import Sequence, Tuple, Dict, Generator, Union, Set
import time
import tempfile
import scipdf
import pandas as pd
import numpy as np
from thefuzz import fuzz
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait


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


def get_pdf_content(
    dataset: pd.DataFrame, main_sections: Sequence[str]
) -> Sequence[Tuple[int, str]]:
    """
    Retrieves the content of PDF files based on the provided dataset.

    Args:
        dataset (pd.DataFrame): The dataset containing 'id', 'pdf_url',
            'title', and 'context' columns.

    Returns:
        list: A list of paper sections extracted from the PDF files.
    """
    inputs = dataset.apply(
        lambda x: (
            x["doi"],
            x["mag_id"],
            x["pmid"],
            x["pdf_url"],
            list(x["id"]),
            list(x["title"]),
            list(x["context"]),
        ),
        axis=1,
    ).tolist()

    # get paper sections
    sections = Parallel(n_jobs=12, verbose=10)(
        delayed(parse_pdf)(*input, main_sections=main_sections) for input in inputs
    )

    return sections


def parse_pdf(
    doi: str,
    mag_id: str,
    pmid: int,
    pdf: str,
    oa_id: Sequence[str],
    parent_title: Sequence[str],
    contexts: Sequence[Sequence[str]],
    main_sections: Sequence[str],
) -> Sequence[Tuple[int, str]]:
    """
    Parse a PDF file and extract citation sections.

    Args:
        oa_id (Sequence[str]): A sequence of citation IDs.
        doi (str): The DOI (Digital Object Identifier) of the article.
        mag_id (str): The MAG (Microsoft Academic Graph) ID of the article.
        pmid (int): The PubMed ID of the article.
        pdf (str): The path to the PDF file.
        parent_title (Sequence[str]): A sequence of parent titles for each citation.
        contexts (Sequence[Sequence[str]]): A sequence of sequences containing citation contexts.
        main_sections (Sequence[str]): A sequence of main sections to extract from the PDF.

    Returns:
        Sequence[Tuple[int, str]]: A sequence of tuples containing the citation ID and the 
            extracted section.

    """
    try:
        article_dict = scipdf.parse_pdf_to_dict(pdf)
        if article_dict is None:
            logger.error(
                "Received None for article_dict while parsing PDF for %s."
                "This may indicate an issue with the PDF file or the parser.",
                pdf,
            )
            return []
        if not isinstance(article_dict, dict):
            logger.error(
                "Expected article_dict to be a dictionary but got %s for %s."
                "Check the parser's output.",
                type(article_dict).__name__,
                pdf,
            )
            return []
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error parsing PDF for %s: %s", doi, e)
        return []

    citation_sections = []
    for citation_id, citation_title, citation_contexts in zip(
        oa_id, parent_title, contexts
    ):
        sections = parent_section_extraction(
            article_dict,
            citation_title,
            citation_contexts,
            main_sections,
            doi,
            mag_id,
            pmid,
        )
        for section in sections:
            section.insert(0, citation_id)
        citation_sections.extend(sections)

    return citation_sections


def parent_section_extraction(
    article_dict: Dict[str, Union[str, Dict[str, str]]],
    parent_title: str,
    contexts: str,
    main_sections: str,
    doi: str,
    mag_id: str,
    pmid: str,
) -> Sequence[Tuple[int, str]]:
    """
    Extracts parent sections from an article based on the provided parameters.

    Args:
        article_dict (dict): The dictionary containing the article information.
        parent_title (str): The title of the parent section.
        contexts (list): A list of contexts to match against the article sections.
        main_sections (list): A list of typical section headings.
        oa_id (str): The Open Access ID of the article.
        doi (str): The DOI of the article.
        mag_id (str): The MAG ID of the article.
        pmid (str): The PMID of the article.

    Returns:
        list: A list of sections matching the provided parameters.

    """
    try:
        best_match_score = 65
        ref_id = None
        for reference in article_dict.get("references", []):
            title = reference.get("title", "")
            score = fuzz.token_sort_ratio(parent_title, title)
            if score > best_match_score:
                best_match_score = score
                ref_id = reference.get("ref_id")

        sections = []
        general_sections = []
        for i, section in enumerate(article_dict.get("sections", [])):
            section_heading = str(section.get("heading", ""))
            for typical_section in main_sections:
                score = fuzz.token_sort_ratio(typical_section.lower(), section_heading.lower())
                if score > 75:
                    general_sections.append((typical_section, i))
                    break
            if ref_id in section.get("publication_ref", []):
                sections.append([doi, mag_id, pmid, i, section_heading])
            else:
                if len(contexts) > 0:
                    for context in contexts:
                        if fuzz.token_set_ratio(context, section.get("text", "")) > 85:
                            sections.append([doi, mag_id, pmid, i, section_heading])
                else:
                    logger.info("No contexts provided for %s", parent_title)

        general_section_indices = np.array([gs[1] for gs in general_sections])
        general_section_indices = np.append(general_section_indices, 1000)
        # find closest negative (or exact) match for each section
        for section in sections:
            section_differences = general_section_indices - section[3]
            upstream_differences = section_differences[section_differences <= 0]
            if np.max(upstream_differences) < int(
                -len(article_dict.get("sections", [])) * 0.75
            ):
                section.append("Not Found")
            else:
                section_idx = np.argmax(
                    section_differences[section_differences <= 0]
                )
                section.append(general_sections[section_idx][0])

        logger.info("Found %d sections for %s", len(sections), parent_title)

        # if sections is empty, return a single row saying not found
        if len(sections) == 0:
            sections.append([doi, mag_id, pmid, -1, "Not Found", "Not Found"])
        return sections

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error processing sections for %s: %s", parent_title, e)
        return [[doi, mag_id, pmid, -2, "Error", "Error"]]


def get_browser_pdf_object(articles: Sequence[Tuple[str, str]]):
    """
    Downloads a PDF file from a given URL using a headless Chrome browser.

    Args:
        combined_id (str): The combined ID of the PDF file.
        url (pd.DataFrame): The URL of the PDF file to download.

    Returns:
        list: A list containing the combined ID and the content of the downloaded PDF file.
              If an error occurs during the download, an empty string is returned instead
              of the PDF content.
    """
    article_outputs = []
    with tempfile.TemporaryDirectory() as tmp_download_path:
        chrome_options = Options()
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": tmp_download_path,
                "download.prompt_for_download": False,  # Disable download prompt
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True,  # Disable PDF viewer
            },
        )

        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(10)

        for combined_id, url in articles:
            try:
                initial_files = set(os.listdir(tmp_download_path))
                driver.get(url)  # url navigate triggers download

                WebDriverWait(driver, 5).until(
                    lambda driver: len(os.listdir(tmp_download_path))
                    > len(initial_files)  # pylint: disable=cell-var-from-loop
                )

                start_time = time.time()
                while True:
                    time.sleep(0.25)  # polling interval
                    current_files = set(os.listdir(tmp_download_path))
                    new_files = current_files - initial_files
                    new_files = {
                        file
                        for file in new_files
                        if not file.endswith(".crdownload")
                        and "IDSCOC" not in file
                        and "google.chrome" not in file
                    }
                    if new_files or time.time() - start_time > 5:
                        break
                if not new_files:
                    logger.error("Download timed out or failed for %s.", combined_id)
                    continue

                new_file = next(iter(new_files))
                downloaded_file_path = os.path.join(tmp_download_path, new_file)
                logger.info("Downloaded file for %s: %s", combined_id, new_file)
                with open(downloaded_file_path, "rb") as file:
                    pdf_content = file.read()
                article_outputs.append((combined_id, pdf_content))
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error downloading PDF for %s: %s", combined_id, e)
                continue
        driver.quit()
    return article_outputs


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
