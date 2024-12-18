# pylint: skip-file
import logging
import pandas as pd
import pytest
from kedro.io import MemoryDataset
from unittest.mock import MagicMock
from dsit_impact.pipelines.data_collection_s2.pipeline import (
    create_pipeline as create_s2_collection_pipeline,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["s2"]["data_collection"]


@pytest.fixture(scope="function")
def oa_input_data(project_context):
    oa_input_data = project_context.catalog.load("oa.publications.gtr.primary")
    return oa_input_data.sample(10, random_state=42)


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    oa_input_data,
    params,
):
    catalog.add_feed_dict(
        {
            "oa.publications.gtr.primary": oa_input_data,
            "params:s2.data_collection.strength.api.base_url": params["strength"][
                "api"
            ]["base_url"],
            "params:s2.data_collection.strength.api.fields": params["strength"]["api"][
                "fields"
            ],
            "params:s2.data_collection.strength.api.key": params["strength"]["api"][
                "key"
            ],
            "params:s2.data_collection.strength.api.perpage": params["strength"]["api"][
                "perpage"
            ],
            "params:s2.data_collection.paper_details.api.base_url": params[
                "paper_details"
            ]["api"]["base_url"],
            "params:s2.data_collection.paper_details.api.fields": params[
                "paper_details"
            ]["api"]["fields"],
            "params:s2.data_collection.paper_details.api.key": params["paper_details"][
                "api"
            ]["key"],
        }
    )
    return catalog


@pytest.mark.integration
def test_citation_pipeline(caplog, seq_runner, catalog_data):
    assert "oa.publications.gtr.primary" in catalog_data.list(), (
        "The input data for the pipeline is not loaded into the catalog. "
        "Please make sure to load the data before running the pipeline."
    )

    pipeline = (
        create_s2_collection_pipeline()
        .from_nodes("get_s2_citation_data")
        .to_nodes("get_s2_citation_data")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text

    citation_results = results["s2.citation_details.raw"]

    for key in citation_results:
        citation_results[key] = MagicMock(return_value=citation_results[key])

    catalog_data.add_feed_dict(
        {"s2.citation_details.raw": MemoryDataset(citation_results)}
    )

    pipeline = (
        create_s2_collection_pipeline()
        .from_nodes("concatenate_citation_partitions")
        .to_nodes("concatenate_citation_partitions")
    )

    seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text


@pytest.mark.integration
def test_paper_details_pipeline(caplog, seq_runner, catalog_data):
    pipeline = (
        create_s2_collection_pipeline()
        .from_nodes("get_s2_paper_data")
        .to_nodes("get_s2_paper_data")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text

    paper_results = results["s2.paper_details.raw"]

    for key in paper_results:
        paper_results[key] = MagicMock(return_value=paper_results[key])

    catalog_data.add_feed_dict({"s2.paper_details.raw": MemoryDataset(paper_results)})

    pipeline = (
        create_s2_collection_pipeline()
        .from_nodes("concatenate_paper_partitions")
        .to_nodes("concatenate_paper_partitions")
    )

    seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text
