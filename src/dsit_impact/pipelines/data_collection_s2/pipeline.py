"""
This is a boilerplate pipeline 'data_collection_s2'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    get_citation_data,
    get_paper_data,
    concatenate_partitions
)


def create_pipeline(**kwargs) -> Pipeline: # pylint: disable=unused-argument
    return pipeline([
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
            name="get_citation_data"
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
            name="get_paper_data"
        ),
        node(
            func=concatenate_partitions,
            inputs={"partitioned_dataset": "s2.citation_details.raw"},
            outputs="s2.citation_details.intermediate",
            name="concatenate_citation_partitions"
        ),
        node(
            func=concatenate_partitions,
            inputs={"partitioned_dataset": "s2.paper_details.raw"},
            outputs="s2.paper_details.intermediate",
            name="concatenate_paper_partitions"
        )

    ])
