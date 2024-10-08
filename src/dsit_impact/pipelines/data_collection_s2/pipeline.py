"""
This module defines the S2 pipeline for processing Open Access (OA) dataset
using Kedro. The pipeline consists of nodes that fetch citation and paper
data, and concatenate partitions of these datasets.

Functions:
    create_pipeline(**kwargs) -> Pipeline:
        Creates and returns a Kedro pipeline with the following nodes:
        - get_citation_data: Fetches citation details from the OA dataset.
        - get_paper_data: Fetches paper details from the OA dataset.
        - concatenate_citation_partitions: Concatenates citation partitions.
        - concatenate_paper_partitions: Concatenates paper partitions.

Dependencies:
    - Kedro
    - pandas
    - requests

Usage:
    Import the necessary functions and call them with appropriate arguments to
    fetch and process citation and paper data from the OA dataset.

Command Line Example:
    ```
    kedro run --pipeline s2_pipeline
    ```
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import get_citation_data, get_paper_data, concatenate_partitions


def create_pipeline(  # pylint: disable=unused-argument, missing-function-docstring
    **kwargs,
) -> Pipeline:
    return pipeline(
        [
            node(
                func=get_citation_data,
                inputs={
                    "oa_dataset": "oa.publications.gtr.primary",
                    "base_url": "params:s2.data_collection.strength.api.base_url",
                    "fields": "params:s2.data_collection.strength.api.fields",
                    "api_key": "params:s2.data_collection.strength.api.key",
                    "perpage": "params:s2.data_collection.strength.api.perpage",
                },
                outputs="s2.citation_details.raw",
                name="get_citation_data",
            ),
            node(
                func=get_paper_data,
                inputs={
                    "oa_dataset": "oa.publications.gtr.primary",
                    "base_url": "params:s2.data_collection.paper_details.api.base_url",
                    "fields": "params:s2.data_collection.paper_details.api.fields",
                    "api_key": "params:s2.data_collection.paper_details.api.key",
                },
                outputs="s2.paper_details.raw",
                name="get_paper_data",
            ),
            node(
                func=concatenate_partitions,
                inputs={"partitioned_dataset": "s2.citation_details.raw"},
                outputs="s2.citation_details.intermediate",
                name="concatenate_citation_partitions",
            ),
            node(
                func=concatenate_partitions,
                inputs={"partitioned_dataset": "s2.paper_details.raw"},
                outputs="s2.paper_details.intermediate",
                name="concatenate_paper_partitions",
            ),
        ],
        tags="enrich_s2_intent"
    )
