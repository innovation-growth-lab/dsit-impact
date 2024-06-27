
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
    gtr_sample_data = gtr_input_data.sample(100, random_state=42)
    return gtr_sample_data

@pytest.mark.integration
def test_gtr_collection_pipeline(caplog, params, catalog, seq_runner, gtr_data):
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

    catalog.add_feed_dict(
        {
            'oa.data_matching.gtr.input': gtr_data,
            'params:oa.data_matching.gtr.api.mailto': params["api"]["mailto"],
            'params:oa.data_matching.gtr.api.perpage': params["api"]["perpage"],
            'params:oa.data_matching.gtr.filter_doi': params["filter_doi"],
            'params:oa.data_matching.gtr.n_jobs': 2,
        }
    )
    seq_runner.run(pipeline, catalog)

    assert successful_run_msg in caplog.text


