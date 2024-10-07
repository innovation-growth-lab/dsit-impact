"""
This module defines the data processing pipeline for generating the final enriched dataset
for Open Access (OA) publications. The pipeline integrates data from various sources, including
Gateway to Research (GtR), Semantic Scholar (S2), PDF sectional data, and diversity scores.

Functions:
    create_pipeline(**kwargs) -> Pipeline:
        Creates and returns a Kedro pipeline for generating the final enriched dataset.

Pipeline Nodes:
    - create_final_dataset: Merges various dataframes to create a comprehensive base dataset.

Inputs:
    - gtr_to_oa_map: "oa.publications.gtr.map.primary"
    - oa: "oa.publications.gtr.primary"
    - s2_papers: "s2.paper_details.intermediate"
    - s2_citations: "s2.citation_details.intermediate"
    - section_details: "pdfs.section_shares.intermediate"
    - coauthor_diversity: "publications.field.coauthor_diversity_scores.intermediate"
    - paper_diversity: "publications.field.paper_diversity_scores.intermediate"

Outputs:
    - oa.publications.gtr.base.data: The final enriched dataset.
"""
from kedro.pipeline import Pipeline, pipeline, node
from .nodes import create_final_dataset


def create_pipeline(  # pylint: disable=unused-argument, missing-function-docstring
    **kwargs,
) -> Pipeline:
    return pipeline(
        [
            node(
                create_final_dataset,
                inputs={
                    "gtr_to_oa_map": "oa.publications.gtr.map.primary",
                    "oa": "oa.publications.gtr.primary",
                    "s2_papers": "s2.paper_details.intermediate",
                    "s2_citations": "s2.citation_details.intermediate",
                    "section_details": "pdfs.section_shares.intermediate",
                    "coauthor_diversity": "publications.field.coauthor_diversity_scores.intermediate",
                    "paper_diversity": "publications.field.paper_diversity_scores.intermediate",
                },
                outputs="oa.publications.gtr.base.data",
                name="generate_final_enriched_data",
            )
        ]
    )

# pending → map back to GtR publications.