"""
This is a boilerplate pipeline 'data_analysis_team_metrics'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    compute_topic_embeddings,
    create_author_aggregates,
    calculate_diversity_components,
    calculate_paper_diversity,
    calculate_coauthor_diversity,
)


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
                name=f"create_author_aggregates_{level}",
            )
            for level in ["topic", "subfield", "field", "domain"]
        ],
    )

    calculate_components_pipeline = pipeline(
        [
            node(
                func=calculate_diversity_components,
                inputs={
                    "data": f"authors.{level}.aggregates.intermediate",
                    "disparity_matrix": f"cwts.topics.{level}.distance_matrix",
                },
                outputs=f"authors.{level}.diversity_components.intermediate",
                name=f"calculate_diversity_components_{level}",
            )
            for level in ["topic", "subfield", "field", "domain"]
        ],
    )

    calculate_diversity_scores_pipeline = pipeline(
        [
            node(
                func=calculate_paper_diversity,
                inputs={
                    "publications": "oa.publications.gtr.primary",
                    "disparity_matrix": f"cwts.topics.{level}.distance_matrix",
                },
                outputs=f"publications.{level}.paper_diversity_scores.intermediate",
                name=f"calculate_paper_diversity_{level}",
            )
            for level in ["field", "domain"]
        ]
        + [
            node(
                func=calculate_coauthor_diversity,
                inputs={
                    "publications": "oa.publications.gtr.primary",
                    "authors": f"authors.{level}.aggregates.intermediate",
                    "disparity_matrix": f"cwts.topics.{level}.distance_matrix",
                },
                outputs=f"publications.{level}.coauthor_diversity_scores.intermediate",
                name=f"calculate_coauthor_diversity_{level}",
            )
            for level in ["topic", "subfield", "field", "domain"]
        ],
        tags = ["data_analysis_team_metrics"]
    )

    return (
        embedding_generation_pipeline
        + author_aggregates_pipeline
        + calculate_components_pipeline
        + calculate_diversity_scores_pipeline
    )
