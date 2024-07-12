"""
This is a boilerplate pipeline 'data_processing_pdfs'
generated using Kedro 0.19.6
"""

import logging
from typing import Sequence, Tuple, Dict, Union, Callable, Generator
import scipdf
import pandas as pd
from thefuzz import fuzz
from joblib import Parallel, delayed


logger = logging.getLogger(__name__)


def parse_pdf(
    pdf: str, parent_title: str, contexts: Sequence[str]
) -> Sequence[Tuple[int, str]]:
    """
    Parses a PDF document and extracts relevant sections based on the 
        parent title and contexts.

    Args:
        pdf (str): The path or content of the PDF document.
        parent_title (str): The title of the parent document.
        contexts (Sequence[str]): A sequence of context strings to match 
            against the section text.

    Returns:
        Sequence[Tuple[int, str]]: A sequence of tuples containing the index 
            and heading of the matched sections.

    """
    article_dict = scipdf.parse_pdf_to_dict(pdf)

    best_match_score = 50
    ref_id = None
    for reference in article_dict.get("references", []):
        title = reference.get("title", "")
        score = fuzz.token_sort_ratio(parent_title, title)
        if score > best_match_score:
            best_match_score = score
            ref_id = reference.get("ref_id")

    # check for each section in "sections" if ref_id is present
    sections = []
    for i, section in enumerate(article_dict.get("sections", [])):
        section_heading = section.get("heading", "")
        if ref_id in section.get("publication_ref", []):
            sections.append(tuple([i, section_heading]))
        else:
            for context in contexts:
                # fuzzy ratio a substring of context in section text
                if fuzz.token_set_ratio(context, section.get("text", "")) > 75:
                    sections.append(tuple([i, section_heading]))

    logger.info("Found %d sections for %s", len(sections), parent_title)
    return sections
