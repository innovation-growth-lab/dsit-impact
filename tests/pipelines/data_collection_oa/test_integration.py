# pylint: skip-file
import logging
import pandas as pd
import pytest
from kedro.io import MemoryDataset
from dsit_impact.pipelines.data_collection_oa.pipeline import (
    create_pipeline as create_gtr_matching_oa_pipeline,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["oa"]["data_matching"]["gtr"]


@pytest.fixture(scope="function")
def gtr_data(project_context):
    gtr_input_data = project_context.catalog.load(
        "gtr.data_collection.publications.intermediate"
    )
    return gtr_input_data.sample(10, random_state=42)


@pytest.fixture(scope="function")
def oa_first_data(project_context):
    oa_input_data = project_context.catalog.load(
        "oa.data_matching.gtr.doi.intermediate"
    )
    return oa_input_data.sample(10, random_state=42)


@pytest.fixture(scope="function")
def cr_rlu_candidates(project_context):
    cr_rlu_candidates = project_context.catalog.load(
        "cr.data_matching.gtr.doi.intermediate"
    )
    return cr_rlu_candidates.sample(10, random_state=42)


@pytest.fixture(scope="function")
def oa_rlu_candidates(project_context):
    oa_rlu_candidates = project_context.catalog.load(
        "oa_search.data_matching.gtr.doi.best_match.intermediate"
    )
    return oa_rlu_candidates.sample(10, random_state=42)


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    gtr_data,
    oa_first_data,
    cr_rlu_candidates,
    oa_rlu_candidates,
    params,
):
    catalog.add_feed_dict(
        {
            "gtr.data_collection.publications.intermediate": gtr_data,
            "oa.data_matching.gtr.doi.intermediate": oa_first_data,
            "cr.data_matching.gtr.doi.intermediate": cr_rlu_candidates,
            "oa_search.data_matching.gtr.doi.best_match.intermediate": oa_rlu_candidates,
            "params:oa.data_matching.gtr.api.mails": params["api"]["mails"],
            "params:oa.data_matching.gtr.api.perpage": params["api"]["perpage"],
            "params:oa.data_matching.gtr.n_jobs": 2,
            "params:oa.data_matching.gtr.filter_doi": params["filter_doi"],
            "params:oa.data_matching.gtr.filter_oa": params["filter_oa"],
            "params:crossref.doi_matching.gtr.api.mailto": params["api"]["mailto"],
            "params:oa.data_matching.gtr.api": params["api"],
        }
    )
    return catalog


@pytest.mark.integration
def test_gtr_collection_pipeline(caplog, seq_runner, catalog_data):

    assert "gtr.data_collection.publications.intermediate" in catalog_data.list(), (
        "The input data for the pipeline is not loaded into the catalog. "
        "Please make sure to load the data before running the pipeline."
    )

    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes("preprocess_publication_doi")
        .to_nodes("concatenate_openalex")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    input_data = catalog_data.load("gtr.data_collection.publications.intermediate")
    output_data = results["oa.data_matching.gtr.doi.intermediate"]

    # check some of the doi in input data are in the output data
    input_data["doi"] = input_data["doi"].str.extract(r"(10\..+)")
    output_data["doi"] = output_data["doi"].str.extract(r"(10\..+)")
    assert input_data["doi"].isin(output_data["doi"]).any()

    # assert columns in output include id, doi, title, topics, authorships
    assert all(
        column in output_data.columns
        for column in ["id", "doi", "title", "topics", "authorships"]
    )

    # assert code ran successfully
    assert successful_run_msg in caplog.text


@pytest.mark.integration
def test_gtr_cr_rlu_collection_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes("crossref_doi_match")
        .to_nodes("crossref_doi_match")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text


@pytest.mark.integration
def test_gtr_oa_rlu_collection_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes("oa_search_with_query")
        .to_nodes("oa_search_with_query")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text


@pytest.mark.integration
def test_final_selection_pipeline(caplog, catalog_data, seq_runner):
    pipeline = (
        create_gtr_matching_oa_pipeline()
        .from_nodes("select_better_match")
        .to_nodes("map_outcome_ids_to_oa_papers")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    assert successful_run_msg in caplog.text
