"""
This module provides utility functions for handling PDF content extraction and 
downloading PDF files using a headless Chrome browser.

Functions:
    - get_pdf_content(dataset: pd.DataFrame, main_sections: Sequence[str]) -> 
      Sequence[Tuple[int, str]]:
        Retrieves the content of PDF files based on the provided dataset.
    - _parse_pdf(doi: str, mag_id: str, pmid: int, pdf: str, oa_id: Sequence[str], 
      parent_title: Sequence[str], contexts: Sequence[Sequence[str]], 
      main_sections: Sequence[str]) -> Sequence[Tuple[int, str]]:
        Parses a PDF file and extracts citation sections.
    - _parent_section_extraction(article_dict: Dict[str, Union[str, Dict[str, str]]], 
      parent_title: str, contexts: str, main_sections: str, doi: str, mag_id: str, 
      pmid: str) -> Sequence[Tuple[int, str]]:
        Extracts parent sections from an article based on the provided parameters.
    - get_browser_pdf_object(articles: Sequence[Tuple[str, str]]):
        Downloads a PDF file from a given URL using a headless Chrome browser.

Dependencies:
    - pandas
    - scipdf
    - numpy
    - logging
    - joblib
"""

import logging
from typing import Sequence, Tuple, Dict, Union
import scipdf
import pandas as pd
import numpy as np
from thefuzz import fuzz
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)


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
    sections = Parallel(n_jobs=8, verbose=10)(
        delayed(_parse_pdf)(*input, main_sections=main_sections) for input in inputs
    )

    return sections


def _parse_pdf(
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
        sections = _parent_section_extraction(
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


def _parent_section_extraction(
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
                score = fuzz.token_sort_ratio(
                    typical_section.lower(), section_heading.lower()
                )
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
                section_idx = np.argmax(section_differences[section_differences <= 0])
                section.append(general_sections[section_idx][0])

        logger.info("Found %d sections for %s", len(sections), parent_title)

        # if sections is empty, return a single row saying not found
        if len(sections) == 0:
            sections.append([doi, mag_id, pmid, -1, "Not Found", "Not Found"])
        return sections

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error processing sections for %s: %s", parent_title, e)
        return [[doi, mag_id, pmid, -2, "Error", "Error"]]
