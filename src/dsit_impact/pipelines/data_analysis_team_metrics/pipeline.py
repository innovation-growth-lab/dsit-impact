"""
This is a boilerplate pipeline 'data_analysis_team_metrics'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import compute_topic_embeddings, create_author_aggregates


def create_pipeline(  # pylint: disable=unused-argument, missing-function-docstring
    **kwargs,
) -> Pipeline:
    embedding_generation_pipeline = pipeline(
        [
            node(
                func=compute_topic_embeddings,
                inputs={"cwts_data": "cwts.topics.input"},
                outputs=[
                    "cwts.topics.topic.distance_matrix",
                    "cwts.topics.subfield.distance_matrix",
                    "cwts.topics.field.distance_matrix",
                    "cwts.topics.domain.distance_matrix",
                ],
                name="compute_topic_embeddings",
            ),
        ]
    )

    author_aggregates_pipeline = pipeline(
        [
            node(
                func=create_author_aggregates,
                inputs={
                    "authors_data": "authors.oa_dataset.raw",
                    "level": f"params:tm.levels.{level}",
                },
                outputs=f"authors.{level}.aggregates.intermediate",
                name=f"compute_moving_average_{level}",
            )
            for level in ["topic", "subfield", "field", "domain"]
        ],
        tags="author_aggregates",
    )

    return embedding_generation_pipeline + author_aggregates_pipeline
