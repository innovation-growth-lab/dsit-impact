"""
Test file for pipeline 'data_matching_oa'. We use pytest, see more at:
https://docs.pytest.org/en/latest/getting-started.html
"""

# pylint: skip-file
import pandas as pd
import pytest
import requests
from requests.adapters import HTTPAdapter, Retry

from dsit_impact.pipelines.data_matching_oa.nodes import (
    preprocess_publication_doi,
    create_list_doi_inputs,
    fetch_papers,
    concatenate_openalex
)

from dsit_impact.pipelines.data_matching_oa.utils.cr import get_doi


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["oa"]["data_matching"]["gtr"]


@pytest.fixture(scope="function")
def gtr_data(project_context):
    gtr_input_data = project_context.catalog.load("oa.data_matching.gtr.input")
    gtr_sample_data = gtr_input_data.sample(100, random_state=42)
    return gtr_sample_data

@pytest.fixture(scope="function")
def session():
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

@pytest.fixture(scope="function")
def cr_input(session):
    return {
        "outcome_id": "TEST-ID",
        "title": (
            "Knowledge of nativelike selections in an L2 The influence of exposure, "
            "memory, age of onset, and motivation in foreign language and immersion settings"
        ),
        "author": "Foster,P",
        "journal": "Studies in Second Language Acquisition",
        "publication_date": "2013-01-01",
        "mailto": "analytics@nesta.org.uk",
        "session": session,
    }


def test_preprocess_publication_doi(gtr_data):
    processed_data = preprocess_publication_doi(gtr_data)

    assert processed_data.shape[0] == gtr_data.shape[0]
    assert "doi" in processed_data.columns


def test_create_list_doi_inputs(gtr_data):
    processed_data = preprocess_publication_doi(gtr_data)

    grouped_data = create_list_doi_inputs(processed_data, grouped=True)
    assert all("|" in doi for doi in grouped_data)

    ungrouped_data = create_list_doi_inputs(processed_data, grouped=False)
    assert all("|" not in doi for doi in ungrouped_data)
    assert len(ungrouped_data) == processed_data["doi"].nunique()


def test_fetch_papers(gtr_data, params):
    processed_data = preprocess_publication_doi(gtr_data)
    doi_list = create_list_doi_inputs(processed_data, grouped=True)

    papers = fetch_papers(
        ids=doi_list,
        mailto=params["api"]["mailto"],
        perpage=params["api"]["perpage"],
        filter_criteria=params["filter_doi"],
        parallel_jobs=params["n_jobs"],
    )

    # assert papers is a list of lists of jsons
    assert isinstance(papers, dict)
    data_slice = papers["s0"]()
    assert all(isinstance(chunk, list) for chunk in data_slice)
    assert all(isinstance(paper, dict) for chunk in data_slice for paper in chunk)


def test_concatenate_openalex(gtr_data, params):
    processed_data = preprocess_publication_doi(gtr_data)
    doi_list = create_list_doi_inputs(processed_data, grouped=True)

    papers = fetch_papers(
        ids=doi_list,
        mailto=params["api"]["mailto"],
        perpage=params["api"]["perpage"],
        filter_criteria=params["filter_doi"],
        parallel_jobs=params["n_jobs"],
    )

    concatenated_data = concatenate_openalex(papers)

    assert isinstance(concatenated_data, pd.DataFrame)
    assert "doi" in concatenated_data.columns
    assert concatenated_data.shape[0] <= sum(
        [len(dois.split("|")) for dois in doi_list]
    )
    assert concatenated_data["doi"].nunique() == concatenated_data.shape[0]

def test_cr_get_doi(cr_input):
    returned_item = get_doi(**cr_input)
    assert isinstance(returned_item["doi"], str)
    assert returned_item["doi"].startswith("10.")
    assert returned_item["title"] == "KNOWLEDGE OF NATIVELIKE SELECTIONS IN A L2"
    assert returned_item["author"] == "Foster, Pauline"
    assert returned_item["journal"] == "Studies in Second Language Acquisition"
    assert returned_item["year"] == 2013