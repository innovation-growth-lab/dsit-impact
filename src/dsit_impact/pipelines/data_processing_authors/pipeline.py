"""
This is a boilerplate pipeline 'data_processing_authors'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import create_author_list, fetch_author_papers


def create_pipeline(**kwargs) -> Pipeline: # pylint: disable=unused-argument, missing-function-docstring
    author_collection_pipeline = pipeline(
        [
            node(
                func=create_author_list,
                inputs={"input_df": "authors.oa_dataset.input"},
                outputs="author_list",
                name="create_author_list",
            ),
            node(
                func=fetch_author_papers,
                inputs={
                    "authors": "author_list",
                    "mailto": "params:authors.api.mailto",
                    "perpage": "params:authors.api.perpage",
                    "filter_criteria": "params:authors.filter_criteria",
                },
                outputs="authors.oa_dataset.raw",
                name="fetch_author_papers",
            ),
        ]
    )
    return author_collection_pipeline
