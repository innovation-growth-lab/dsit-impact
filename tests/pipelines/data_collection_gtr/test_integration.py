"""
Test file for pipeline 'data_collection_gtr'. We use pytest, see more at:
https://docs.pytest.org/en/latest/getting-started.html

To run this test file just type in the terminal:
$ pytest tests/pipelines/data_collection_gtr/test_unit.py
"""

# pylint: skip-file
import logging
import pandas as pd
import pytest

from dsit_impact.settings import GTR_ENDPOINTS
from dsit_impact.pipelines.data_collection_gtr.pipeline import (
    create_pipeline as create_gtr_collection_pipeline,
)

from kedro.runner import SequentialRunner
from kedro.io import DataCatalog

@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["gtr"]["data_collection"]

@pytest.mark.integration
def test_gtr_collection_pipeline(caplog, params):
    pipeline = (
        create_gtr_collection_pipeline(test_mode=True)
        .from_nodes(
            "gtr.data_collection.publications.fetch_gtr_data"
        ).to_nodes(
            "gtr.data_collection.publications.concatenate_endpoint"
        )
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    catalog = DataCatalog()
    catalog.add_feed_dict(
        {
            'params:gtr.data_collection.publications.label': params["publications"]["label"],
            'params:gtr.data_collection.publications.param_requests': params["publications"]["param_requests"],
            'params:gtr.data_collection.publications.test_mode': True
        }
    )
    SequentialRunner().run(pipeline, catalog)

    assert successful_run_msg in caplog.text



