"""Pipeline for data collection.

This pipeline fetches data from the GtR API and preprocesses it into a
format that can be used by the rest of the project. To run this pipeline,
use the following command:

    $ kedro run --pipeline data_collection_gtr

Alternatively, you can run this pipeline for a single endpoint:

    $ kedro run --pipeline data_collection_gtr --tags projects

In regards to the use of namespaces, note that these are appended as
prefixes to the outputs of the nodes in the pipeline. 

"""
from kedro.pipeline import Pipeline, node, pipeline

from dsit_impact import settings
from .nodes import fetch_gtr_data  # pylint: disable=E0401


def create_pipeline(**kwargs) -> Pipeline:  # pylint: disable=W0613
    """Pipeline for data collection.

    Returns:
        Pipeline: The data collection pipeline.
    """
    template_pipeline = pipeline(
        [
            node(
                func=fetch_gtr_data,
                inputs=["params:param_requests", "params:label"],
                outputs="raw",
                name="fetch_gtr_data",
            ),
        ]
    )

    pipelines = [
        pipeline(
            template_pipeline,
            namespace=f"gtr.data_collection.{label}",
            tags=[label, "gtr"],
        )
        for label in settings.GTR_ENDPOINTS
    ]
    return sum(pipelines)