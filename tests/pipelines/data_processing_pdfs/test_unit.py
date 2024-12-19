# pylint: skip-file
from datetime import datetime
import pandas as pd
import pytest
from kedro.io import MemoryDataset
from unittest.mock import MagicMock
from dsit_impact.pipelines.data_processing_pdfs.nodes import (
    preprocess_for_section_collection,
    get_citation_sections,
    compute_section_shares,
)


@pytest.fixture
def params(project_context):
    """Get the parameters for the OpenAlex API."""
    return project_context.config_loader["parameters"]["pdfs"]


@pytest.fixture(scope="function")
def s2_input_data():
    data = {
        "id": [
            "W2963793841",
            "W2770178180",
            "W2139990416",
            "W2112861482",
            "W2112193291",
            "W2746972957",
            "W2809879025",
            "W1994439127",
            "W3101145195",
            "W2420109333",
        ],
        "pdf_url": [
            "https://strathprints.strath.ac.uk/65439/1/Picard_etal_GAMM_Reports_2018_On_the_well_posedness_of_a_class_of_non_autonomous_SPDEs.pdf",
            "https://www.mdpi.com/1422-0067/20/23/5988/pdf?version=1574929493",
            "https://academic.oup.com/gbe/article-pdf/13/11/evab238/41135741/evab238.pdf",
            "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1111/gcbb.12315",
            "https://arxiv.org/pdf/1108.2506",
            "https://link.springer.com/content/pdf/10.1007/s00221-020-05780-4.pdf",
            "https://www.biorxiv.org/content/biorxiv/early/2019/09/14/769919.full.pdf",
            "https://www.frontiersin.org/articles/10.3389/fphy.2023.1141972/pdf",
            "https://link.springer.com/content/pdf/10.1140/epjc/s10052-022-10217-z.pdf",
            "https://pharmrev.aspetjournals.org/content/pharmrev/72/1/80.full.pdf",
        ],
        "doi": [
            "10.1002/gamm.201800014",
            "10.3390/ijms20235988",
            "10.1093/gbe/evab238",
            "10.1111/gcbb.12315",
            "10.1111/j.1365-2966.2011.19620.x",
            "10.1007/s00221-020-05780-4",
            "10.1101/769919",
            "10.3389/fphy.2023.1141972",
            "10.1140/epjc/s10052-022-10217-z",
            "10.1124/pr.119.017772",
        ],
        "mag_id": [
            "2964244465",
            "2991253489",
            "",
            "1841044245",
            "2949081915",
            "3012894934",
            "2972926359",
            "",
            "",
            "2995584735",
        ],
        "pmid": [
            "",
            "31795097",
            "34718556",
            "",
            "",
            "32206850",
            "",
            "",
            "",
            "31826934",
        ],
        "intent": [
            "",
            "methodology",
            "background",
            "background",
            "",
            "background",
            "methodology",
            "methodology",
            "",
            "",
        ],
        "context": [
            "",
            "research",
            "research",
            "research",
            "",
            "research",
            "research",
            "research",
            "",
            "",
        ],
        "is_open_access": [True, True, True, True, True, True, True, True, True, True],
    }

    return pd.DataFrame(data)


@pytest.fixture(scope="function")
def oa_input_data():
    data = {
        "id": [
            "W2963793841",
            "W2770178180",
            "W2139990416",
            "W2112861482",
            "W2112193291",
            "W2746972957",
            "W2809879025",
            "W1994439127",
            "W3101145195",
            "W2420109333",
        ],
        "title": [
            "A solution theory for a general class of SPDEs",
            "JASPAR 2018: update of the open-access database of transcription factor binding profiles and its web framework",
            "Transposable element islands facilitate adaptation to novel environments in an invasive species",
            "How do soil emissions of N2O, CH4 and CO2 from perennial bioenergy crops differ from arable annual crops?",
            "Galaxy Zoo: morphologies derived from visual inspection of galaxies from the Sloan Digital Sky Survey★",
            "The relationship between intelligence and reaction time varies with age: Results from three representative narrow-age age cohorts at 30, 50 and 69 years",
            "Design of metalloproteins and novel protein folds using variational autoencoders",
            "New multi-GPU implementation for smoothed particle hydrodynamics on heterogeneous clusters",
            "A posteriori inclusion of parton density functions in NLO QCD final-state calculations at hadron colliders: the APPLGRID project",
            "A systematic review of the clinical effectiveness and cost-effectiveness of pharmacological and psychological interventions for the management of obsessive–compulsive disorder in children/adolescents and adults",
        ],
    }

    return pd.DataFrame(data)


@pytest.fixture(scope="function")
def catalog_data(
    catalog,
    s2_input_data,
    oa_input_data,
    params,
):
    catalog.add_feed_dict(
        {
            "s2.citation_details.intermediate": s2_input_data,
            "oa.publications.gtr.primary": oa_input_data,
            "params:pdfs.data_collection.main_sections": params["data_collection"][
                "main_sections"
            ],
        }
    )
    return catalog


@pytest.mark.unit
def test_preprocess_for_section_collection(oa_input_data, s2_input_data):
    result = preprocess_for_section_collection(oa_input_data, s2_input_data)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert all(
        col in result.columns
        for col in ["doi", "mag_id", "pmid", "pdf_url", "id", "title"]
    )

    assert result.shape[0] == 10


@pytest.mark.unit
def test_get_citation_sections(oa_input_data, s2_input_data, catalog_data):
    data = preprocess_for_section_collection(oa_input_data, s2_input_data)

    result = get_citation_sections(
        dataset=data,
        main_sections=catalog_data.load("params:pdfs.data_collection.main_sections"),
    )

    parse_result = next(result)

    day_datetime = datetime.now().strftime("%y%m%d")

    assert isinstance(parse_result[f"{day_datetime}/s0"], pd.DataFrame)
    assert not parse_result[f"{day_datetime}/s0"].empty
    assert all(
        col in parse_result[f"{day_datetime}/s0"].columns
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
    assert parse_result[f"{day_datetime}/s0"].parent_id.nunique() <= 10
    assert parse_result[f"{day_datetime}/s0"].parent_id.nunique() >= 3


def test_compute_section_shares():
    section_details = {
        "s0": MagicMock(
            return_value=pd.DataFrame(
                {
                    "parent_id": [
                        "W2963793841",
                        "W2963793841",
                        "W2963793841",
                        "W2963793841",
                    ],
                    "doi": [
                        "10.1002/gamm.201800014",
                        "10.1002/gamm.201800014",
                        "10.1002/gamm.25123014",
                        "10.1002/gamm.251420014",
                    ],
                    "main_section_heading": [
                        "Introduction",
                        "Methods",
                        "Results",
                        "Discussion",
                    ],
                }
            )
        )
    }

    result = compute_section_shares(section_details)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "parent_id" in result.columns
    assert "total_sections" in result.columns
    assert "unique_dois" in result.columns
    assert result.shape[0] == 1
    assert result["total_sections"].iloc[0] == 4
    assert result["unique_dois"].iloc[0] == 3
