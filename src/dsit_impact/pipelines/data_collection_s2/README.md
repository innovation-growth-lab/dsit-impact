# S2 Pipeline: Citation and Paper Data Processing

The **S2 pipeline** focuses on processing data from the Semantic Scholar API and OpenAlex datasets. It retrieves citation and paper details, processes partitions, and prepares intermediate datasets for further analysis.

<img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS5ZdhMkgtBgDNXCA18EmFXunmWWfG3HN_bLg&s" alt="GtR" style="width:100%;"/>

## Features
- Fetch citation and paper data using Semantic Scholar APIs.
- Dynamically identify unparsed papers using oracle checks, avoiding redundant processing.
- Apply date filters to ensure only recent or relevant data is processed.
- Concatenate partitioned datasets for citation and paper details.

## Nodes Overview
1. **`get_unmatched_papers`**  
   Dynamically identifies papers that have not yet been processed by comparing the incoming data with previously parsed results using an oracle.

2. **`get_citation_data`**  
   Retrieves citation details, including intent, context, and influential metrics from the Semantic Scholar dataset. Supports filtering by date.

3. **`get_paper_data`**  
   Collects paper metadata, such as open access status and influential citation counts. Also supports filtering by date.

4. **`concatenate_citation_partitions`**  
   Merges citation data partitions, removing duplicates and sorting by influence.

5. **`concatenate_paper_partitions`**  
   Combines paper detail partitions, ensuring data consistency and uniqueness.

## Key Datasets
- **Raw Inputs:**
  - `oa.publications.gtr.primary`  
    Contains OpenAlex publication data mapped to GTR publications.

- **Intermediate Outputs:**
  - `s2.citation_details.unmatched`: Papers identified as unmatched and unparsed.
  - `s2.citation_details.raw`: Raw citation details fetched from Semantic Scholar.
  - `s2.citation_details.intermediate`: Processed citation details.
  - `s2.paper_details.unmatched`: Papers identified as unmatched and unparsed.
  - `s2.paper_details.raw`: Raw paper details fetched from Semantic Scholar.
  - `s2.paper_details.intermediate`: Processed paper details.

## Configuration
The pipeline is parameterised via the Kedro configuration system:
- **API Parameters**:
  - `base_url`, `fields`, `api_key`, and `perpage` for fetching data from the Semantic Scholar API.
- **Oracle Checks**:
  - The `only_unparsed` parameter determines whether to process only unparsed papers.
- **Date Filtering**:
  - The `filter_date` parameter allows left-censored updates by filtering data based on publication date.

## Running the Pipeline
Run the entire pipeline:
```bash
kedro run --pipeline s2_pipeline
```

Run only the citation data processing:
```bash
kedro run --nodes get_citation_data
```

Run only the paper data processing:
```bash
kedro run --nodes get_paper_data
```

Identify unparsed papers using the oracle:
```bash
kedro run --nodes get_unmatched_papers
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `requests`, `joblib`.
- External API: Semantic Scholar.