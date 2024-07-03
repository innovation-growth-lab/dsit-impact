import pandas as pd
from thefuzz import fuzz  # pylint: disable=import-error

def break_ties(group: pd.DataFrame) -> pd.DataFrame:
    """
    Breaks ties between matching records based on similarity scores, DOI presence,
    and source preference.

    Args:
        group (pandas.DataFrame): A group of matching records.

    Returns:
        pandas.DataFrame: The best matching record based on tie-breaking rules.
    """

    group = group.copy()

    # Compute similarity scores
    group["similarity"] = group.apply(
        lambda x: fuzz.token_set_ratio(x["title_match"], x["title_gtr"]), axis=1
    )

    # Sort by similarity, then by presence of DOI, then prefer 'oa' source
    group_sorted = group.sort_values(
        by=["similarity", "doi_match", "source"], ascending=[False, False, True]
    )

    # Apply tie-breaking rules
    if (
        len(group_sorted) > 1
        and abs(group_sorted.iloc[0]["similarity"] - group_sorted.iloc[1]["similarity"])
        <= 5
    ):
        # If the top two are within 5 points, check DOI and source
        if pd.notnull(group_sorted.iloc[0]["doi_match"]) and pd.notnull(
            group_sorted.iloc[1]["doi_match"]
        ):
            # If both have DOI, prefer 'oa' source
            best_match = group_sorted[group_sorted["source"] == "oa"].head(1)
        else:
            # Else, select the one with a DOI
            best_match = group_sorted[pd.notnull(
                group_sorted["doi_match"])].head(1)
    else:
        # Else, select the top one
        best_match = group_sorted.head(1)

    return best_match
