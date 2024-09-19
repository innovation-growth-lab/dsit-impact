"""
This pipeline fetches data from the GtR API and preprocesses it into a format
that can be used by the rest of the project.

Pipelines:
    - data_collection_gtr:
        Fetches and preprocesses data from the GtR API.

Dependencies:
    - Kedro
    - pandas
    - requests
    - logging

Usage:
    Run the pipeline to fetch and preprocess data from the GtR API.

Command Line Example:
    ```
    kedro run --pipeline data_collection_gtr
    ```
    Alternatively, you can run this pipeline for a single endpoint:
    ```
    kedro run --pipeline data_collection_gtr --tags projects
    ```

Note:
    In regards to the use of namespaces, note that these are appended as
    prefixes to the outputs of the nodes in the pipeline.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_gtr_data, concatenate_endpoint


def create_pipeline(**kwargs) -> Pipeline:  # pylint: disable=W0613
    """Pipeline for data collection.

    Returns:
        Pipeline: The data collection pipeline.
    """
    template_pipeline = pipeline(
        [
            node(
                func=fetch_gtr_data,
                inputs={
                    "parameters": "params:gtr.data_collection.publications.param_requests",
                    "endpoint": "params:gtr.data_collection.publications.label",
                    "test_mode": "params:gtr.data_collection.publications.test_mode",
                },
                outputs="gtr.data_collection.publications.raw",
                name="gtr.data_collection.publications.fetch_gtr_data",
            ),
            node(
                func=concatenate_endpoint,
                inputs="gtr.data_collection.publications.raw",
                outputs="gtr.data_collection.publications.intermediate",
                name="gtr.data_collection.publications.concatenate_endpoint",
            ),
        ]
    )

    pipelines = [
        pipeline(
            template_pipeline,
            namespace="gtr.data_collection.publications",
            tags=["publications", "gtr"],
        )
    ]
    # pipelines = [
    #     pipeline(
    #         template_pipeline,
    #         namespace=f"gtr.data_collection.{label}",
    #         tags=[label, "gtr"],
    #     )
    #     for label in settings.GTR_ENDPOINTS
    # ]
    return sum(pipelines)
