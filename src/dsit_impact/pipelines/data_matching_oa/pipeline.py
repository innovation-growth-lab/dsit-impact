"""
This is a boilerplate pipeline 'data_matching_oa'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    preprocess_publication_doi,
    create_list_doi_inputs,
    fetch_papers,
)


def create_pipeline(**kwargs) -> Pipeline: # pylint: disable=unused-argument
    """
    Creates a pipeline for collecting data from the GTR API.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        Pipeline: The created pipeline.
    """

    gtr_collection_pipeline = pipeline(
        [
            node(
                func=preprocess_publication_doi,
                inputs="input",
                outputs="preproc",
                name="preprocess_publication_doi"
            ),
            node(
                func=create_list_doi_inputs,
                inputs="preproc",
                outputs="doi_list",
                name="create_nested_doi_list"
            ),
            node(
                func=fetch_papers,
                inputs={
                    "mailto": "params:api.mailto",
                    "perpage": "params:api.perpage",
                    "ids": "doi_list",
                    "filter_criteria": "params:filter_doi",
                    "parallel_jobs": "params:n_jobs",
                },
                outputs="raw",
                name="fetch_papers",
            ),
        ],
        namespace="oa.data_collection.gtr",
    )

    return gtr_collection_pipeline
