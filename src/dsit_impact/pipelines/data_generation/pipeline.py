"""
This is a boilerplate pipeline 'data_generation'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import create_base_data


def create_pipeline(  # pylint: disable=unused-argument, missing-function-docstring
    **kwargs,
) -> Pipeline:
    return pipeline(
        [
            node(
                create_base_data,
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
                name="create_base_data_node",
            )
        ]
    )

# pending → map back to GtR publications.