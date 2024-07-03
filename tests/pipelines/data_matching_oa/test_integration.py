
# pylint: skip-file
import logging
import pandas as pd
import pytest

from dsit_impact.pipelines.data_matching_oa.pipeline import (
    create_pipeline as create_gtr_matching_oa_pipeline,
)

@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["oa"]["data_matching"]["gtr"]

@pytest.fixture(scope="function")
def gtr_data(project_context):
    gtr_input_data = project_context.catalog.load("oa.data_matching.gtr.input")
    return gtr_input_data.sample(100, random_state=42)

@pytest.fixture(scope="function")
def gtr_oa_data(project_context):
    gtr_oa_data = project_context.catalog.load("oa.data_matching.gtr.doi.intermediate")
    return gtr_oa_data.sample(100, random_state=42)

@pytest.fixture(scope="function")
def oa_rlu_data(project_context):
    oa_rlu_data = project_context.catalog.load("oa_search.data_matching.gtr.doi.best_match.intermediate")
    return oa_rlu_data.sample(100, random_state=42)
     

@pytest.fixture(scope="function")
def cr_rlu_data(project_context):
    cr_rlu_data = project_context.catalog.load("cr.data_matching.gtr.doi.intermediate")
    return cr_rlu_data.sample(100, random_state=42)

@pytest.fixture(scope="function")
def catalog_data(catalog, gtr_data, gtr_oa_data, oa_rlu_data, cr_rlu_data, params):
    catalog.add_feed_dict(
        {
            'oa.data_matching.gtr.input': gtr_data,
            'oa.data_matching.gtr.doi.intermediate': gtr_oa_data,
            'oa_search.data_matching.gtr.doi.best_match.intermediate': oa_rlu_data,
            'cr.data_matching.gtr.doi.intermediate': cr_rlu_data,
            'params:oa.data_matching.gtr.api.mailto': params["api"]["mailto"],
            'params:oa.data_matching.gtr.api.perpage': params["api"]["perpage"],
            'params:oa.data_matching.gtr.n_jobs': 2,
            'params:oa.data_matching.gtr.filter_doi': params["filter_doi"],
            'params:oa.data_matching.gtr.filter_oa': params["filter_oa"],
            'params:crossref.doi_matching.gtr.api.mailto': params["api"]["mailto"],
            'params:oa.data_matching.gtr.api': params["api"],
        }
    )
    return catalog
    

@pytest.mark.integration
def test_gtr_collection_pipeline(caplog, seq_runner, catalog_data):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes(
            "oa.data_matching.gtr.preprocess_publication_doi"
        ).to_nodes(
            "oa.data_matching.gtr.concatenate_openalex"
        )
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text


@pytest.mark.integration
def test_gtr_cr_rlu_collection_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes(
            "crossref_doi_match"
        ).to_nodes(
            "crossref_doi_match"
        )
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text

@pytest.mark.integration
def test_gtr_oa_rlu_collection_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes(
            "oa_search_match"
        ).to_nodes(
            "oa_search_match"
        )
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text

@pytest.mark.integration
def test_final_selection_and_merge_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes(
            "select_better_match"
        )
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text