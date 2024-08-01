"""
This is a boilerplate pipeline 'data_generation'
generated using Kedro 0.19.6
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def create_base_data(
    gtr_to_oa_map: pd.DataFrame,
    oa: pd.DataFrame,
    s2_papers: pd.DataFrame,
    s2_citations: pd.DataFrame,
    section_details: pd.DataFrame,
    coauthor_diversity: pd.DataFrame,
    paper_diversity: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates the base data by merging various dataframes.

    Args:
        oa (pd.DataFrame): The dataframe containing OA data.
        s2_papers (pd.DataFrame): The dataframe containing S2 papers data.
        s2_citations (pd.DataFrame): The dataframe containing S2 citations data.
        section_details (pd.DataFrame): The dataframe containing PDF sectional data.
        coauthor_diversity (pd.DataFrame): The dataframe containing coauthor diversity scores.
        paper_diversity (pd.DataFrame): The dataframe containing paper diversity scores.

    Returns:
        pd.DataFrame: The merged dataframe containing the base data.
    """

    logger.info("Merging S2 data.")
    base_data = oa.merge(s2_papers, on="id", how="left")
    aggregated_s2_citations = _aggregate_citations(s2_citations)
    base_data = base_data.merge(aggregated_s2_citations, on="id", how="left")
    base_data = base_data.copy()
    for column in [
        "s2_citation_count",
        "methodology_count",
        "result_count",
        "background_count",
        "open_access_count",
    ]:
        base_data.fillna({column: 0}, inplace=True)
        base_data[column] = base_data[column].astype(int)

    logger.info("Merging PDF sectional data.")
    section_details = _aggregate_citation_sections(section_details)
    base_data = base_data.merge(section_details, on="id", how="left")
    base_data.fillna({"total_sections": 0}, inplace=True)
    base_data["total_sections"] = base_data["total_sections"].astype(int)
    base_data["section_counts"] = base_data["section_counts"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    logger.info("Merging diversity scores.")
    base_data = base_data.merge(coauthor_diversity, on="id", how="left")
    # relabel evenness to coauthor_evenness, same with variety, disparity
    base_data.rename(
        columns={
            "evenness": "coauthor_evenness",
            "variety": "coauthor_variety",
            "disparity": "coauthor_disparity",
        },
        inplace=True,
    )
    base_data = base_data.merge(paper_diversity, on="id", how="left")
    base_data.rename(
        columns={
            "evenness": "paper_evenness",
            "variety": "paper_variety",
            "disparity": "paper_disparity",
        },
        inplace=True,
    )

    logger.info("Merging GtR data.")
    gtr_oa_list = gtr_to_oa_map.groupby("id")["outcome_id"].apply(list).reset_index()
    base_data = base_data.merge(gtr_oa_list, on="id", how="left")
    base_data = base_data[base_data["outcome_id"].notnull()]

    return base_data


def _aggregate_citations(citations: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate citation data based on specified criteria.

    Args:
        citations (pd.DataFrame): The input DataFrame containing citation data.

    Returns:
        pd.DataFrame: The aggregated citation data DataFrame.

    """
    citations["citation_id"] = (
        citations["pmid"] + citations["doi"] + citations["mag_id"]
    )

    # sort citations' intent column, by "methodology", "result", "background", None
    citations["intent"] = pd.Categorical(
        citations["intent"],
        categories=["methodology", "result", "background"],
        ordered=True,
    )

    # sort values by intent
    citations = citations.sort_values("intent")

    # drop duplicates based on id and citation_id, keep first
    citations = citations.drop_duplicates(subset=["id", "citation_id"], keep="first")

    logger.info("Creating aggregated s2 citation data.")
    aggregated = (
        citations.groupby("id")
        .agg(
            s2_citation_count=("citation_id", "size"),
            methodology_count=("intent", lambda x: (x == "methodology").sum()),
            result_count=("intent", lambda x: (x == "result").sum()),
            background_count=("intent", lambda x: (x == "background").sum()),
            open_access_count=("is_open_access", "sum"),
        )
        .reset_index()
    )

    return aggregated


def _aggregate_citation_sections(section_details: pd.DataFrame) -> pd.DataFrame:
    section_details.fillna(0, inplace=True)
    section_details = section_details.astype(
        {col: "int" for col in section_details.columns if col != "parent_id"}
    )

    # create a list of tuples
    section_details["section_counts"] = section_details.apply(
        lambda x: [
            [col, str(x[col])]
            for col in section_details.columns
            if col not in ["parent_id", "total_sections"] and x[col] != 0
        ],
        axis=1,
    )

    # change parent_id to id
    section_details = section_details.rename(columns={"parent_id": "id"})

    return section_details[["id", "total_sections", "section_counts"]]
