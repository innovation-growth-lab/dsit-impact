# pylint: skip-file
import logging
import itertools
import pandas as pd
import pytest
from dsit_impact.pipelines.data_results_team_metrics.pipeline import (
    create_pipeline as create_team_metrics_pipeline,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the pipeline."""
    return project_context.config_loader["parameters"]["tm"]


@pytest.fixture(scope="function")
def cwts_data(project_context):
    cwts_data = project_context.catalog.load("cwts.topics.input")
    return cwts_data


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
def catalog_data(
    catalog,
    cwts_data,
    oa_input_data,
    authors_data,
    params,
):
    catalog.add_feed_dict(
        {
            "cwts.topics.input": cwts_data,
            "oa.publications.gtr.primary": oa_input_data,
            "authors.oa_dataset.raw": authors_data,
            "params:tm.levels.subfield": params["levels"]["subfield"],
            "params:tm.levels.field": params["levels"]["field"],
            "params:tm.levels.domain": params["levels"]["domain"],
        }
    )
    return catalog


@pytest.mark.integration
def test_team_metrics_pipeline(caplog, seq_runner, catalog_data):
    pipeline = (
        create_team_metrics_pipeline()
        .from_nodes("compute_topic_embeddings")
        .to_nodes("compute_topic_embeddings")
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."
    results = seq_runner.run(pipeline, catalog_data)

    # assert code ran successfully
    assert successful_run_msg in caplog.text

    catalog_data.add_feed_dict(results)

    # Check the outputs
    for level in ["subfield", "field", "domain"]:

        pipeline = (
            create_team_metrics_pipeline()
            .from_nodes(f"create_author_aggregates_{level}")
            .to_nodes(f"calculate_coauthor_diversity_{level}")
        )

        results = seq_runner.run(pipeline, catalog_data)

        # assert code ran successfully
        assert successful_run_msg in caplog.text

        coauthor_diversity_scores = results[
            f"publications.{level}.coauthor_diversity_scores.intermediate"
        ]
        assert isinstance(coauthor_diversity_scores, pd.DataFrame)
        assert not coauthor_diversity_scores.empty
        assert all(
            col in coauthor_diversity_scores.columns
            for col in [
                "id",
                "variety",
                "evenness",
                "disparity"
            ]
        )
        
        # assert that all values are between 0 and 1
        assert coauthor_diversity_scores["variety"].between(0, 1).all()
        assert coauthor_diversity_scores["evenness"].between(0, 1).all()
        assert coauthor_diversity_scores["disparity"].between(0, 1).all()