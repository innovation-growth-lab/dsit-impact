"""
This is a boilerplate pipeline 'data_processing_pdfs'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import get_citation_sections, preprocess_for_section_collection, get_browser_pdfs


def create_pipeline(**kwargs) -> Pipeline: # pylint: disable=C0116, W0613
    direct_collection_pipeline = pipeline([
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
                "main_sections": "params:pdfs.data_collection.main_sections"
            },
            outputs="pdfs.section_details.raw",
            name="get_citation_sections"
        )
    ])

    indirect_collection_pipeline = pipeline([
        node(
            func=get_browser_pdfs,
            inputs={
                "dataset": "pdfs.section_details.input"
            },
            outputs="pdfs.objects.raw",
            name="get_browser_pdfs"
        )
    ])

    return direct_collection_pipeline + indirect_collection_pipeline
