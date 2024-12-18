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
from .nodes import (
    get_citation_data,
    get_paper_data,
    concatenate_partitions,
    get_unmatched_papers,
)


def create_pipeline(  # pylint: disable=unused-argument, missing-function-docstring
    **kwargs,
) -> Pipeline:
    citation_data_pipeline = pipeline(
        [
            node(
                func=get_unmatched_papers,
                inputs={
                    "incoming_data": "oa.publications.gtr.primary",
                    "s2_data": "s2.citation_details.oracle",
                    "only_unparsed": "params:s2.data_collection.only_unparsed",
                },
                outputs="s2.citation_details.unmatched",
                name="get_unmatched_citations",
                tags=["test"]

            ),
            node(
                func=get_citation_data,
                inputs={
                    "oa_dataset": "s2.citation_details.unmatched",
                    "base_url": "params:s2.data_collection.strength.api.base_url",
                    "fields": "params:s2.data_collection.strength.api.fields",
                    "api_key": "params:s2.data_collection.strength.api.key",
                    "perpage": "params:s2.data_collection.strength.api.perpage",
                    "filter_date": "params:s2.data_collection.filter_date",
                },
                outputs="s2.citation_details.raw",
                name="get_s2_citation_data",
                tags=["test"]
            ),
            node(
                func=concatenate_partitions,
                inputs={"partitioned_dataset": "s2.citation_details.raw"},
                outputs="s2.citation_details.intermediate",
                name="concatenate_citation_partitions",
                tags=["test"]
            ),
        ],
    )

    paper_data_pipeline = pipeline(
        [
            node(
                func=get_unmatched_papers,
                inputs={
                    "incoming_data": "oa.publications.gtr.primary",
                    "s2_data": "s2.paper_details.oracle",
                    "only_unparsed": "params:s2.data_collection.only_unparsed",
                },
                outputs="s2.paper_details.unmatched",
                name="get_unmatched_papers",
            ),
            node(
                func=get_paper_data,
                inputs={
                    "oa_dataset": "s2.paper_details.unmatched",
                    "base_url": "params:s2.data_collection.paper_details.api.base_url",
                    "fields": "params:s2.data_collection.paper_details.api.fields",
                    "api_key": "params:s2.data_collection.paper_details.api.key",
                    "filter_date": "params:s2.data_collection.filter_date",
                },
                outputs="s2.paper_details.raw",
                name="get_s2_paper_data",
            ),
            node(
                func=concatenate_partitions,
                inputs={"partitioned_dataset": "s2.paper_details.raw"},
                outputs="s2.paper_details.intermediate",
                name="concatenate_paper_partitions",
            ),
        ],
    )

    return citation_data_pipeline + paper_data_pipeline
