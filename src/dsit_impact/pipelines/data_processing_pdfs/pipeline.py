"""
This script defines the Kedro pipeline for processing PDF data, retrieving
citation sections, and computing section shares.

Pipelines:
    - direct_collection_pipeline: Preprocesses datasets and retrieves citation
      sections.
    - indirect_collection_pipeline: Retrieves the content of PDF files using a
      headless browser.
    - compute_section_shares_pipeline: Computes the section shares for each
      parent_id in the given loaders.

Dependencies:
    - kedro: For creating and managing data pipelines.
    - pandas: For data manipulation and analysis.
    - logging: For logging information.
    - typing: For type hinting.
    - utils: Contains utility functions `get_pdf_content` and
      `get_browser_pdf_object`.

Usage:
    Import the `create_pipeline` function and call it to create the pipeline.
    The pipeline can then be run using Kedro's execution commands.

Command Line Example:
    ```
    kedro run --pipeline data_processing_pdfs
    kedro run --nodes preprocess_for_section_collection -e base
    ```
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    get_citation_sections,
    preprocess_for_section_collection,
    get_browser_pdfs,
    compute_section_shares,
)


def create_pipeline(**kwargs) -> Pipeline:  # pylint: disable=C0116, W0613
    direct_collection_pipeline = pipeline(
        [
            node(
                func=preprocess_for_section_collection,
                inputs={
                    "oa_dataset": "oa.publications.gtr.primary",
                    "s2_dataset": "s2.citation_details.intermediate",
                },
                outputs="pdfs.section_details.preprocessed",
                name="preprocess_for_section_collection",
            ),
            node(
                func=get_citation_sections,
                inputs={
                    "dataset": "pdfs.section_details.preprocessed",
                    "main_sections": "params:pdfs.data_collection.main_sections",
                },
                outputs="pdfs.section_details.raw",
                name="get_citation_sections",
            ),
        ]
    )

    indirect_collection_pipeline = pipeline( # pylint: disable=unused-variable
        [
            node(
                func=get_browser_pdfs,
                inputs={"dataset": "pdfs.section_details.preprocessed"},
                outputs="pdfs.objects.raw",
                name="get_browser_pdfs",
            )
        ]
    )

    compute_section_shares_pipeline = pipeline(
        [
            node(
                func=compute_section_shares,
                inputs={
                    "section_details": "pdfs.section_details.raw",
                },
                outputs="pdfs.section_shares.intermediate",
                name="compute_section_shares",
            )
        ]
    )

    return (
        direct_collection_pipeline
        # + indirect_collection_pipeline
        + compute_section_shares_pipeline
    )
