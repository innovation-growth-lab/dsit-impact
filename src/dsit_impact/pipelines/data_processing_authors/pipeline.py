"""
This script defines a Kedro pipeline for collecting author data and fetching
author papers from the OpenAlex API.

Pipelines:
    - author_collection_pipeline: Fetches papers for a list of authors.

Dependencies:
    - kedro: For creating and managing data pipelines.
    - pandas: For data manipulation and analysis.
    - logging: For logging information.
    - typing: For type hinting.
    - fetch_papers_for_id: Utility function for fetching papers from OpenAlex.

Usage:
    Import the `create_pipeline` function and call it to create the pipeline.
    The pipeline can then be run using Kedro's execution commands.

Command Line Example:
    ```
    kedro run --pipeline author_collection_pipeline
    ```
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
