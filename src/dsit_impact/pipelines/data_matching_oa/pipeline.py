"""
This script defines a data collection pipeline for the GTR API using Kedro.

Pipelines:
    - oa_first_collection_pipeline: Preprocesses publication DOIs and fetches
      papers from OpenAlex.
    - cross_ref_matcher_pipeline: Matches DOIs with CrossRef data.
    - oa_search_matcher_pipeline: Matches DOIs with OpenAlex data.
    - merge_pipeline: Selects the better match between CrossRef and OpenAlex.
    - oa_doi_collection_pipeline: Fetches papers using DOIs from combined data.
    - oa_id_collection_pipeline: Fetches papers using OpenAlex IDs.
    - primary_pipeline: Merges all datasets and maps outcome IDs.

Dependencies:
    - Kedro: A Python framework for creating reproducible, maintainable, and
      modular data science code.
    - GTR API: The API from which data is collected.
    - OpenAlex: A service for fetching publication data.
    - CrossRef: A service for matching DOIs.

Usage:
    Import the `create_pipeline` function and call it to create the pipeline.
    The pipeline can then be run using Kedro's execution commands.

Command Line Example:
    ```
    kedro run --pipeline data_matching_oa
    ```

"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import (
    preprocess_publication_doi,
    fetch_papers,
    concatenate_openalex,
    crossref_doi_match,
    oa_search_match,
    concatenate_matches,
    oa_filter,
    select_better_match,
    create_list_oa_inputs,
    concatenate_oa_datasets,
    map_outcome_id,
)


def create_pipeline(**kwargs) -> Pipeline:  # pylint: disable=unused-argument
    """
    Creates a pipeline for collecting data from the GTR API.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        Pipeline: The created pipeline.
    """

    oa_first_collection_pipeline = pipeline(
        [
            node(
                func=preprocess_publication_doi,
                inputs={
                    "input_df": "input",
                    "processed_df": "processed_df",
                    "process_all": "params:process_all"
                },
                outputs="doi_list",
                name="preprocess_publication_doi",
            ),
            node(
                func=fetch_papers,
                inputs={
                    "mailto": "params:api.mailto",
                    "perpage": "params:api.perpage",
                    "ids": "doi_list",
                    "filter_criteria": "params:filter_doi",
                    "parallel_jobs": "params:n_jobs",
                },
                outputs="doi.raw",
                name="fetch_papers",
            ),
            node(
                func=concatenate_openalex,
                inputs={"data": "doi.raw"},
                outputs="doi.intermediate",
                name="concatenate_openalex",
            ),
        ],
        namespace="oa.data_matching.gtr",
    )

    cross_ref_matcher_pipeline = pipeline(
        [
            node(
                func=crossref_doi_match,
                inputs={
                    "oa_data": "oa.data_matching.gtr.doi.intermediate",
                    "gtr_data": "oa.data_matching.gtr.input",
                    "mailto": "params:crossref.doi_matching.gtr.api.mailto",
                },
                outputs="cr.data_matching.gtr.doi.raw",
                name="crossref_doi_match",
            ),
            node(
                func=concatenate_matches,
                inputs={"data": "cr.data_matching.gtr.doi.raw"},
                outputs="cr.data_matching.gtr.doi.intermediate",
                name="concatenate_crossref",
            ),
        ],
    )

    oa_search_matcher_pipeline = pipeline(
        [
            node(
                func=oa_search_match,
                inputs={
                    "oa_data": "oa.data_matching.gtr.doi.intermediate",
                    "gtr_data": "oa.data_matching.gtr.input",
                    "config": "params:oa.data_matching.gtr.api",
                },
                outputs="oa_search.data_matching.gtr.doi.raw",
                name="oa_search_match",
            ),
            node(
                func=concatenate_matches,
                inputs={"data": "oa_search.data_matching.gtr.doi.raw"},
                outputs="oa_search.data_matching.gtr.doi.intermediate",
                name="concatenate_oa_search",
            ),
            node(
                func=oa_filter,
                inputs={"data": "oa_search.data_matching.gtr.doi.intermediate"},
                outputs="oa_search.data_matching.gtr.doi.best_match.intermediate",
                name="get_best_oa_match",
            ),
        ],
    )

    merge_pipeline = pipeline(
        [
            node(
                func=select_better_match,
                inputs={
                    "openalex": "oa_search.data_matching.gtr.doi.best_match.intermediate",
                    "crossref": "cr.data_matching.gtr.doi.intermediate",
                },
                outputs="oa_search.data_matching.gtr.doi.combined.intermediate",
                name="select_better_match",
            )
        ],
    )

    oa_doi_collection_pipeline = pipeline(
        [
            node(
                func=preprocess_publication_doi,
                inputs={
                    "input_df": "oa_search.data_matching.gtr.doi.combined.intermediate",
                    "processed_df": "oa_search.data_matching.gtr.doi.combined.processed_df",
                    "process_all": "params:process_all"
                },
                outputs="doi_list",
                name="create_second_doi_list",
            ),
            node(
                func=fetch_papers,
                inputs={
                    "mailto": "params:oa.data_matching.gtr.api.mailto",
                    "perpage": "params:oa.data_matching.gtr.api.perpage",
                    "ids": "doi_list",
                    "filter_criteria": "params:oa.data_matching.gtr.filter_doi",
                    "parallel_jobs": "params:oa.data_matching.gtr.n_jobs",
                },
                outputs="oa.data_matching.gtr.combined.doi.raw",
                name="fetch_papers_doi",
            ),
            node(
                func=concatenate_openalex,
                inputs={"data": "oa.data_matching.gtr.combined.doi.raw"},
                outputs="oa.data_matching.gtr.combined.doi.intermediate",
                name="concatenate_openalex_doi",
            ),
        ],
        tags="second_search",
    )

    oa_id_collection_pipeline = pipeline(
        [
            node(
                func=create_list_oa_inputs,
                inputs="oa_search.data_matching.gtr.doi.combined.intermediate",
                outputs="oa_list",
                name="create_second_oa_list",
            ),
            node(
                func=fetch_papers,
                inputs={
                    "mailto": "params:oa.data_matching.gtr.api.mailto",
                    "perpage": "params:oa.data_matching.gtr.api.perpage",
                    "ids": "oa_list",
                    "filter_criteria": "params:oa.data_matching.gtr.filter_oa",
                    "parallel_jobs": "params:oa.data_matching.gtr.n_jobs",
                },
                outputs="oa.data_matching.gtr.combined.id.raw",
                name="fetch_papers_id",
            ),
            node(
                func=concatenate_openalex,
                inputs={"data": "oa.data_matching.gtr.combined.id.raw"},
                outputs="oa.data_matching.gtr.combined.id.intermediate",
                name="concatenate_openalex_id",
            ),
        ],
        tags="second_search",
    )

    primary_pipeline = pipeline(
        [
            node(
                func=concatenate_oa_datasets,
                inputs={
                    "base": "oa.data_matching.gtr.doi.intermediate",
                    "doi": "oa.data_matching.gtr.combined.doi.intermediate",
                    "oa": "oa.data_matching.gtr.combined.id.intermediate",
                },
                outputs="oa.publications.gtr.primary",
                name="concatenate_datasets",
            ),
            node(
                func=map_outcome_id,
                inputs={
                    "gtr_data": "oa.data_matching.gtr.input",
                    "oa_data": "oa.publications.gtr.primary",
                    "rlu_outputs": "oa_search.data_matching.gtr.doi.combined.intermediate",
                },
                outputs="oa.publications.gtr.map.primary",
                name="map_outcome_id",
            ),
        ],
        tags=["second_search", "primary"],
    )

    return (
        oa_first_collection_pipeline  # oa search using doi
        + cross_ref_matcher_pipeline
        + oa_search_matcher_pipeline
        + merge_pipeline  # lookups with CR & OA
        + oa_doi_collection_pipeline
        + oa_id_collection_pipeline  # fetch data for lookup results
        + primary_pipeline  # merge all three datasets
    )
