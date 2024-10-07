"""
This pipeline is designed to compute topic embeddings, author aggregates, diversity 
components, and diversity scores for a given dataset.

Pipelines:
    - embedding_generation_pipeline:
        Computes topic embeddings and distance matrices for topics, subfields, fields, 
        and domains.
    - author_aggregates_pipeline:
        Creates aggregates of author data based on specified taxonomy levels.
    - calculate_components_pipeline:
        Calculates diversity components based on the given data and disparity matrix.
    - calculate_diversity_scores_pipeline:
        Calculates paper and coauthor diversity scores for publications.

Usage:
    Import the necessary functions and call them with appropriate arguments to compute 
    embeddings, distance matrices, and diversity components for your dataset.

Command Line Example:
    ```
    kedro run --pipeline data_results_team_metrics -e base
    ```
    or
    ```
    kedro run --nodes compute_topic_embeddings,create_author_aggregates -e base
    ```
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
        ],
        tags="calculate_discipline_diversity_metrics",
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
        tags="calculate_discipline_diversity_metrics",
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
        tags="calculate_discipline_diversity_metrics",
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
            for level in ["subfield", "field", "domain"]
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
            for level in ["subfield", "field", "domain"]
        ],
        tags="calculate_discipline_diversity_metrics",
    )

    return (
        embedding_generation_pipeline
        + author_aggregates_pipeline
        + calculate_components_pipeline
        + calculate_diversity_scores_pipeline
    )
