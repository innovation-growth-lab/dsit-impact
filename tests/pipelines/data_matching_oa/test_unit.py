"""
Test file for pipeline 'data_matching_oa'. We use pytest, see more at:
https://docs.pytest.org/en/latest/getting-started.html
"""

# pylint: skip-file
import pandas as pd
import pytest

from dsit_impact.pipelines.data_matching_oa.nodes import (
    preprocess_publication_doi,
    create_list_doi_inputs,
    fetch_papers,
    concatenate_openalex,
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
