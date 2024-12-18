# pylint: skip-file
import pandas as pd
import pytest
from kedro.io import MemoryDataset

from dsit_impact.pipelines.data_processing_authors.nodes import (
    create_author_list,
    fetch_author_papers,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the GtR API."""
    return project_context.config_loader["parameters"]["authors"]


@pytest.fixture(scope="function")
def author_dataset():
    author_dataset = pd.DataFrame(
        {
            "id": [
                "W3033366669",
                "W2562278500",
                "W4319233449",
            ],
            "authorships": [
                [["A5012533217", "", "", "first"], ["A5077763490", "", "", "last"]],
                [
                    ["A5004525521", "I4210092773", "GB", "first"],
                    ["A5113918474", "I4210092773", "GB", "middle"],
                    ["A5008991798", "I4210092773", "GB", "middle"],
                    ["A5028860536", "I4210092773", "GB", "middle"],
                    ["A5051059666", "I4210092773", "GB", "middle"],
                    ["A5107930252", "I1310212576", "IT", "middle"],
                    ["A5078234115", "I1310212576", "IT", "middle"],
                    ["A5109404061", "I4210092773", "GB", "middle"],
                    ["A5039479556", "I4210092773", "GB", "middle"],
                    ["A5010840659", "I4210092773", "GB", "last"],
                ],
                [["A5039827310", "I2801081054", "GB", "first"]],
            ],
        }
    )
    return author_dataset


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    params,
):
    catalog.add_feed_dict(
        {
            "params:authors.api.mailto": params["api"]["mailto"],
            "params:authors.api.perpage": params["api"]["perpage"],
            "params:authors.filter_criteria": params["filter_criteria"],
        }
    )
    return catalog


@pytest.mark.unit
def test_authors_collection(author_dataset, catalog_data):
    result = create_author_list(input_df=author_dataset)

    assert len(result[0]) >= 10
    assert all(authorship[0].startswith("A") for authorship in result)

    catalog_data.add_feed_dict({"author_list": MemoryDataset(result)})

    result = fetch_author_papers(
        authors=catalog_data.load("author_list"),
        mailto=catalog_data.load("params:authors.api.mailto"),
        perpage=catalog_data.load("params:authors.api.perpage"),
        filter_criteria=catalog_data.load("params:authors.filter_criteria"),
    )

    generator_output = next(result)

    results = generator_output["s0"]

    assert len(results) > 0
    assert all(
        col in results.columns for col in ["id", "author", "publication_date", "topics"]
    )
    assert all(results["author"].str.startswith("A"))
    assert all(results["id"].str.startswith("W"))
    assert all(results["publication_date"].notnull())
    assert any(results["topics"].notnull())
    assert any(results["topics"].str.len() > 0)
