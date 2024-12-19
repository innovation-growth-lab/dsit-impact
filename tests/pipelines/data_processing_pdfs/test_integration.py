# pylint: skip-file
import logging
from datetime import datetime
import pandas as pd
import pytest
from kedro.io import MemoryDataset
from unittest.mock import MagicMock
from dsit_impact.pipelines.data_processing_pdfs.pipeline import (
    create_pipeline as create_pdf_collection_pipeline,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the OpenAlex API."""
    return project_context.config_loader["parameters"]["pdfs"]


@pytest.fixture(scope="function")
def s2_input_data(project_context):
    s2_input_data = project_context.catalog.load("s2.citation_details.intermediate")
    s2_input_data = s2_input_data.dropna(subset=["pdf_url"])
    return s2_input_data.sample(10, random_state=42)


@pytest.fixture(scope="function")
def oa_input_data(project_context, s2_input_data):
    oa_input_data = project_context.catalog.load("oa.publications.gtr.primary")
    return oa_input_data.loc[oa_input_data["id"].isin(s2_input_data["id"])]

@pytest.fixture(scope="function")
def oracle_data(project_context):
    return project_context.catalog.load("pdfs.section_details.oracle")


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    s2_input_data,
    oa_input_data,
    oracle_data,
    params,
):
    catalog.add_feed_dict(
        {
            "s2.citation_details.intermediate": s2_input_data,
            "oa.publications.gtr.primary": oa_input_data,
            "pdfs.section_details.oracle": oracle_data,
            "params:pdfs.data_collection.main_sections": params["data_collection"][
                "main_sections"
            ],
        }
    )
    return catalog


@pytest.mark.integration
def test_pdf_collection_pipeline(caplog, seq_runner, catalog_data):
    assert "s2.citation_details.intermediate" in catalog_data.list(), (
        "The input data for the pipeline is not loaded into the catalog. "
        "Please make sure to load the data before running the pipeline."
    )

    assert "oa.publications.gtr.primary" in catalog_data.list(), (
        "The input data for the pipeline is not loaded into the catalog. "
        "Please make sure to load the data before running the pipeline."
    )

    pipeline = (
        create_pdf_collection_pipeline()
        .from_nodes("preprocess_for_section_collection")
        .to_nodes("get_citation_sections")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    results = seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text

    day_datetime = datetime.now().strftime("%y%m%d")
    pdf_sections = results["pdfs.section_details.raw"]
    assert isinstance(pdf_sections[f"{day_datetime}/s0"], pd.DataFrame)
    assert all(
        col in pdf_sections[f"{day_datetime}/s0"].columns
        for col in [
            "parent_id",
            "doi",
            "mag_id",
            "pmid",
            "section_index",
            "section_heading",
            "main_section_heading",
        ]
    )
    for key in pdf_sections:
        pdf_sections[key] = MagicMock(return_value=pdf_sections[key])
    catalog_data.add_feed_dict(
        {"pdfs.section_details.raw": MemoryDataset(pdf_sections)}
    )

    pipeline = (
        create_pdf_collection_pipeline()
        .from_nodes("compute_section_shares")
        .to_nodes("compute_section_shares")
    )

    seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text
