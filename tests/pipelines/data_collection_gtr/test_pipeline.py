"""
Test file for pipeline 'data_collection_gtr'. We use pytest, see more at:
https://docs.pytest.org/en/latest/getting-started.html

To run this test file just type in the terminal:
"""

# pylint: skip-file
import pandas as pd
import pytest

from dsit_impact.settings import GTR_ENDPOINTS
from dsit_impact.pipelines.data_collection_gtr.nodes import fetch_gtr_data


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["gtr"]["data_collection"]


@pytest.mark.parametrize("endpoint", GTR_ENDPOINTS)
def test_nodes(params, endpoint):
    """
    Test that data is fetched from the GtR API and processed correctly.

    Args:
        params (dict): The parameters for the GtR API.
        endpoint (str): The API endpoint to fetch data from.
        label (str): The label specifying the data preprocessing method.

    Raises:
        AssertionError: If any of the assertions fail.
    """

    # fetch data from the GtR API
    data_generator = fetch_gtr_data(
        parameters=params[endpoint]["param_requests"], endpoint=params[endpoint]["label"]
    )

    data = next(data_generator)

    # assert the returned object is a yield dictionary
    _response_object_is_dict(data)

    # fetch the first item from the response list
    data = list(data.values())[0]

    # assert that the processed data is a pandas DataFrame
    _dataframe_is_returned(data)
    # assert that the processed data does not have a "links" column
    _dataframe_has_no_links_column(data)


def _response_object_is_dict(data):
    """Assert that each item in the response list is a dictionary."""
    assert isinstance(data, dict)


def _dataframe_is_returned(data):
    """Assert that the processed data is a pandas DataFrame."""
    assert isinstance(data, pd.DataFrame)


def _dataframe_has_no_links_column(data):
    """Assert that the processed data does not have a "links" column."""
    assert "links" not in data.columns
