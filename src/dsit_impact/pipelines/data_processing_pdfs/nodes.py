"""
This is a boilerplate pipeline 'data_processing_pdfs'
generated using Kedro 0.19.6
"""

import logging
from typing import Sequence, Tuple
import scipdf
import pandas as pd
import numpy as np
from thefuzz import fuzz
from joblib import Parallel, delayed


logger = logging.getLogger(__name__)

TYPICAL_SECTIONS = [
    "Abstract",
    "Introduction",
    "Background",
    "Literature Review",
    "Theoretical Framework",
    "Methods",
    "Methodology",
    "Empirical Design",
    "Experimental Design",
    "Data",
    "Data Collection",
    "Data Analysis",
    "Results",
    "Discussion",
    "Conclusion",
    "Recommendations",
    "Future Work",
    "Limitations",
    "Acknowledgements",
]


def parse_pdf(
    oa_id: str,
    doi: str,
    mag_id: str,
    pmid: int,
    pdf: str,
    parent_title: str,
    contexts: Sequence[str],
    main_sections: Sequence[str],
) -> Sequence[Tuple[int, str]]:
    """
    Parses a PDF document and extracts relevant sections based on the
        parent title and contexts.

    Args:
        id (str): The ID of the parent document.
        pdf (str): The path or content of the PDF document.
        parent_title (str): The title of the parent document.
        contexts (Sequence[str]): A sequence of context strings to match
            against the section text.

    Returns:
        Sequence[Tuple[int, str]]: A sequence of tuples containing the index
            and heading of the matched sections.

    """
    try:
        article_dict = scipdf.parse_pdf_to_dict(pdf)
        if article_dict is None:
            logger.error(
                "Received None for article_dict while parsing PDF for %s."
                "This may indicate an issue with the PDF file or the parser.",
                parent_title,
            )
            return []
        if not isinstance(article_dict, dict):
            logger.error(
                "Expected article_dict to be a dictionary but got %s for %s."
                "Check the parser's output.",
                type(article_dict).__name__,
                parent_title,
            )
            return []
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error parsing PDF for %s: %s", doi, e)
        return []

    try:
        best_match_score = 50
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
            section_heading = section.get("heading", "")
            for typical_section in main_sections:
                score = fuzz.token_sort_ratio(typical_section, section_heading)
                if score > 75:
                    general_sections.append((typical_section, i))
                    break
            if ref_id in section.get("publication_ref", []):
                sections.append([oa_id, doi, mag_id, pmid, i, section_heading])
            else:
                if len(contexts) > 0:
                    for context in contexts:
                        if fuzz.token_set_ratio(context, section.get("text", "")) > 75:
                            sections.append(
                                [oa_id, doi, mag_id, pmid, i, section_heading]
                            )
                else:
                    logger.info("No contexts provided for %s", parent_title)

        general_section_indices = np.array([gs[1] for gs in general_sections])
        # find closest negative (or exact) match for each section
        for section in sections:
            section_differences = general_section_indices - section[4]
            section_idx = np.argmax(section_differences[section_differences <= 0])
            section.append(general_sections[section_idx][0])

        logger.info("Found %d sections for %s", len(sections), parent_title)
        return sections

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error processing sections for %s: %s", parent_title, e)
        return []


def get_pdf_content(dataset: pd.DataFrame, main_sections: Sequence[str]):
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
            x["id"],
            x["doi"],
            x["mag_id"],
            x["pmid"],
            x["pdf_url"],
            x["title"],
            x["context"],
        ),
        axis=1,
    ).tolist()

    # get paper sections
    sections = Parallel(n_jobs=10, verbose=10)(
        delayed(parse_pdf)(*input, main_sections=main_sections) for input in inputs
    )

    return sections


def preprocess_for_section_collection(
    oa_dataset: pd.DataFrame, s2_dataset: pd.DataFrame
):
    """
    Preprocesses the datasets for section collection.

    Args:
        oa_dataset (pd.DataFrame): The dataset containing the open access articles.
        s2_dataset (pd.DataFrame): The dataset containing the semantic scholar articles.

    Returns:
        pd.DataFrame: The preprocessed merged dataset with grouped contexts.
    """

    # drop those with None in pdf_url
    s2_dataset.dropna(subset=["pdf_url"], inplace=True)

    # keep unique id, titles
    oa_dataset = oa_dataset.drop_duplicates(subset=["id", "title"])

    # merge the datasets
    merged_data = pd.merge(oa_dataset, s2_dataset, on="id", how="right")

    # groupby 'id' and 'pdf_url', create a list of contexts
    merged_data = (
        merged_data.groupby(["id", "doi", "mag_id", "pmid", "title", "pdf_url"])[
            "context"
        ]
        .apply(list)
        .reset_index()
    )

    return merged_data


def get_citation_sections(dataset: pd.DataFrame, main_sections: Sequence[str]):
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
        if i < 341:
            continue
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


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import time
import os
import tempfile


def get_browser_pdf_object(combined_id: str, url: pd.DataFrame):
    try:
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

            # Navigate to the URL, which should automatically trigger the PDF download
            driver.get(url)

            WebDriverWait(driver, 5).until(
                lambda driver: len(os.listdir(tmp_download_path)) > 0
            )

            start_time = time.time()  # Record the start time
            is_download_finished = False
            while not is_download_finished:
                if time.time() - start_time > 10:
                    logger.error("Download timed out.")
                    break  # Exit the loop if the timeout is exceeded

                logger.debug("Waiting for download to complete...")
                time.sleep(0.5)  # Polling interval
                is_download_finished = not any(
                    fname.endswith(".crdownload") for fname in os.listdir(tmp_download_path)
                )

            # Assuming the PDF filename is not known beforehand and is the only file in the directory
            files = os.listdir(tmp_download_path)
            if files:
                logger.info("Downloaded file: %s", combined_id)
                downloaded_file_path = os.path.join(tmp_download_path, files[0])
                # read file
                with open(downloaded_file_path, "rb") as file:
                    pdf_content = file.read()
                return [combined_id, pdf_content]
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error downloading PDF for %s: %s", combined_id, e)
        return [combined_id, ""]

    finally:
        # Close the WebDriver
        try:
            driver.quit()
        except:
            pass


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
    input_batches = [inputs[i : i + 50] for i in range(0, len(inputs), 50)]
    for i, batch in enumerate(input_batches):
        logger.info("Processing batch %d / %d", i, len(input_batches))
        pdfs = Parallel(n_jobs=8, verbose=10)(
            delayed(get_browser_pdf_object)(*input) for input in batch
        )
        pdfs = [(filename, pdf) for filename, pdf in pdfs if isinstance(pdf, bytes)]
        yield {f"s{i}": pdfs}
