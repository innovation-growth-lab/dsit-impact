# pylint: skip-file
import pandas as pd
import pytest
from unittest.mock import patch

from dsit_impact.pipelines.data_collection_s2.nodes import (
    get_citation_data,
    get_paper_data,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["s2"]["data_collection"]


@pytest.fixture(scope="function")
def oa_dataset():
    data = {
        "id": [
            "W4307439606",
            "W1911552272",
            "W4313644649",
            "W3164447669",
            "W3172375810",
            "W2765767940",
            "W2556204855",
            "W3005393191",
            "W2138810805",
            "W4295204307",
        ],
        "mag_id": [
            None,
            "1911552272",
            None,
            "3164447669",
            "3172375810",
            "2765767940",
            "2556204855",
            "3005393191",
            "2138810805",
            None,
        ],
        "pmid": [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "32062688",
            "25770819",
            "36088954",
        ],
        "doi": [
            "https://doi.org/10.1063/5.0101424",
            "https://doi.org/10.1029/2010gl043603",
            "https://doi.org/10.1016/j.enconman.2022.116545",
            "https://doi.org/10.1007/978-3-030-55874-1_121",
            "https://doi.org/10.1016/j.gca.2021.05.047",
            "https://doi.org/10.1177/0278364917734298",
            "https://doi.org/10.1039/9781782626657-00274",
            "https://doi.org/10.1007/s00198-020-05296-1",
            "https://doi.org/10.15252/emmm.201404487",
            "https://doi.org/10.1016/s2468-1253(22)00274-6",
        ],
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    params,
):
    catalog.add_feed_dict(
        {
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


@pytest.mark.unit
def test_get_citation_data(oa_dataset, catalog_data):
    result = list(
        get_citation_data(
            oa_dataset=oa_dataset,
            base_url=catalog_data.load(
                "params:s2.data_collection.strength.api.base_url"
            ),
            fields=catalog_data.load("params:s2.data_collection.strength.api.fields"),
            api_key=catalog_data.load("params:s2.data_collection.strength.api.key"),
            perpage=catalog_data.load("params:s2.data_collection.strength.api.perpage"),
        )
    )

    assert len(result) == 1
    assert "s0" in result[0]
    assert isinstance(result[0]["s0"], pd.DataFrame)
    assert all(
        col in result[0]["s0"].columns
        for col in [
            "id",
            "pmid",
            "doi",
            "mag_id",
            "is_open_access",
            "pdf_url",
            "influential",
            "intent",
            "context",
        ]
    )


@pytest.mark.unit
def test_get_paper_data(oa_dataset, catalog_data):
    result = list(
        get_paper_data(
            oa_dataset=oa_dataset,
            base_url=catalog_data.load(
                "params:s2.data_collection.paper_details.api.base_url"
            ),
            fields=catalog_data.load(
                "params:s2.data_collection.paper_details.api.fields"
            ),
            api_key=catalog_data.load(
                "params:s2.data_collection.paper_details.api.key"
            ),
        )
    )

    assert len(result) == 1
    assert "s0" in result[0]
    assert isinstance(result[0]["s0"], pd.DataFrame)
    assert all(
        col in result[0]["s0"].columns
        for col in ["id", "influential", "is_open_access", "pdf_url"]
    )
