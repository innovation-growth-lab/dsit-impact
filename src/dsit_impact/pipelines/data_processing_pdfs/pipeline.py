"""
This is a boilerplate pipeline 'data_processing_pdfs'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import get_citation_sections, preprocess_for_section_collection


def create_pipeline(**kwargs) -> Pipeline: # pylint: disable=C0116, W0613
    return pipeline([
        node(
            func=preprocess_for_section_collection,
            inputs={
                "oa_dataset": "pdfs.oa_dataset.input",
                "s2_dataset": "pdfs.s2_dataset.input",
            },
            outputs="pdfs.section_details.input",
            name="preprocess_for_section_collection"
        ),
        node(
            func=get_citation_sections,
            inputs={
                "dataset": "pdfs.section_details.input",
            },
            outputs="pdfs.section_details.raw",
            name="get_citation_sections"
        )
    ])
