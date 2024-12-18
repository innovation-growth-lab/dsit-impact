# pylint: skip-file
import logging
import pandas as pd
import pytest
from kedro.io import MemoryDataset
from dsit_impact.pipelines.data_processing_authors.pipeline import (
    create_pipeline as create_author_collection_pipeline,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the OpenAlex API."""
    return project_context.config_loader["parameters"]["authors"]


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
            "params:authors.api.mailto": params["api"]["mailto"],
            "params:authors.api.perpage": params["api"]["perpage"],
            "params:authors.filter_criteria": params["filter_criteria"],
        }
    )
    return catalog


@pytest.mark.integration
def test_author_collection_pipeline(caplog, seq_runner, catalog_data):
    assert "oa.publications.gtr.primary" in catalog_data.list(), (
        "The input data for the pipeline is not loaded into the catalog. "
        "Please make sure to load the data before running the pipeline."
    )

    pipeline = (
        create_author_collection_pipeline()
        .from_nodes("create_author_list")
        .to_nodes("fetch_author_papers")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text

    authors_oa_dataset = results["authors.oa_dataset.raw"]["s0"]
    assert isinstance(authors_oa_dataset, pd.DataFrame)
    assert not authors_oa_dataset.empty
