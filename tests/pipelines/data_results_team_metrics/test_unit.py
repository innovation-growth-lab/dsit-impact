# pylint: skip-file
import itertools
import pandas as pd
import pytest
from dsit_impact.pipelines.data_results_team_metrics.nodes import (
    compute_topic_embeddings,
    create_author_aggregates,
    cumulative_author_aggregates,
    calculate_paper_diversity,
    calculate_coauthor_diversity,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the pipeline."""
    return project_context.config_loader["parameters"]["tm"]


@pytest.fixture(scope="function")
def cwts_data(project_context):
    return project_context.catalog.load("cwts.topics.input")


@pytest.fixture(scope="function")
def authors_data(project_context):
    authors_data = project_context.catalog.load("authors.oa_dataset.raw")
    return dict(itertools.islice(authors_data.items(), 2))


@pytest.fixture(scope="function")
def oa_input_data(project_context):
    oa_input_data = project_context.catalog.load("oa.publications.gtr.primary")
    author_data = project_context.catalog.load("authors.oa_dataset.raw")
    author_data = author_data["s0"]()
    # randomly select 10 with id also in authors_data
    oa_input_data = oa_input_data.loc[
        oa_input_data["id"].isin(author_data["id"].unique())
    ].sample(10)

    return oa_input_data


@pytest.fixture(scope="function")
def disparity_matrix(project_context):
    return project_context.catalog.load("cwts.topics.subfield.distance_matrix")


@pytest.mark.unit
def test_compute_topic_embeddings(cwts_data):
    subfield_distance_matrix, field_distance_matrix, domain_distance_matrix = (
        compute_topic_embeddings(cwts_data)
    )

    assert isinstance(subfield_distance_matrix, pd.DataFrame)
    assert not subfield_distance_matrix.empty

    assert isinstance(field_distance_matrix, pd.DataFrame)
    assert not field_distance_matrix.empty

    assert isinstance(domain_distance_matrix, pd.DataFrame)
    assert not domain_distance_matrix.empty


@pytest.mark.unit
def test_create_author_aggregates(authors_data, cwts_data):
    subfield_distance_matrix, field_distance_matrix, domain_distance_matrix = (
        compute_topic_embeddings(cwts_data)
    )

    for distance_matrix, level in zip(
        [subfield_distance_matrix, field_distance_matrix, domain_distance_matrix],
        [2, 4, 6],
    ):
        result = create_author_aggregates(authors_data, level, distance_matrix)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert all(
            col in result.columns
            for col in [
                "author",
                "year",
                "publications",
                "total_publications",
                "frequency",
            ]
        )


@pytest.mark.unit
def test_cumulative_author_aggregates(authors_data, cwts_data):
    subfield_distance_matrix, _, _ = compute_topic_embeddings(cwts_data)
    author_aggregates = create_author_aggregates(
        authors_data, 2, subfield_distance_matrix
    )

    result = cumulative_author_aggregates(author_aggregates)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert all(
        col in result.columns
        for col in ["author", "year", "publications", "total_publications", "frequency"]
    )


@pytest.mark.unit
def test_calculate_paper_diversity(oa_input_data, disparity_matrix, cwts_data):
    subfield_distance_matrix, _, _ = compute_topic_embeddings(cwts_data)
    result = calculate_paper_diversity(
        oa_input_data, disparity_matrix, 2, subfield_distance_matrix
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert all(
        col in result.columns for col in ["id", "variety", "evenness", "disparity"]
    )


@pytest.mark.unit
def test_calculate_coauthor_diversity(
    oa_input_data, authors_data, disparity_matrix, cwts_data
):
    subfield_distance_matrix, _, _ = compute_topic_embeddings(cwts_data)
    author_aggregates = create_author_aggregates(
        authors_data, 2, subfield_distance_matrix
    )
    cumulative_aggregates = cumulative_author_aggregates(author_aggregates)
    result = calculate_coauthor_diversity(
        oa_input_data, cumulative_aggregates, disparity_matrix
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert all(
        col in result.columns for col in ["id", "variety", "evenness", "disparity"]
    )
