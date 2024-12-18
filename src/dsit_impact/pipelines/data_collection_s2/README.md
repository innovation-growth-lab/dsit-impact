# S2 Pipeline: Citation and paper data processing

The **S2 pipeline** focuses on processing data from the Semantic Scholar API and OpenAlex datasets. It retrieves citation and paper details, processes partitions, and prepares intermediate datasets for further analysis.

<img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS5ZdhMkgtBgDNXCA18EmFXunmWWfG3HN_bLg&s" alt="GtR" style="width:100%;"/>

## Features
- Fetch citation and paper data using Semantic Scholar APIs.
- Concatenate partitioned datasets for citation and paper details.
- Filter and process data based on configurable parameters (e.g., date filters).

## Nodes Overview
1. **`get_citation_data`**  
   Retrieves citation details, including intent, context, and influential metrics from OpenAlex datasets.
   
2. **`get_paper_data`**  
   Collects paper metadata, such as open access status and influential citation counts.
   
3. **`concatenate_citation_partitions`**  
   Merges citation data partitions, removing duplicates and sorting by influence.
   
4. **`concatenate_paper_partitions`**  
   Combines paper detail partitions, ensuring data consistency and uniqueness.

## Key Datasets
- **Raw Inputs:**
  - `oa.publications.gtr.primary`  
    Contains OpenAlex publication data mapped to GTR publications.
- **Outputs:**
  - `s2.citation_details.intermediate`  
    Processed citation details.
  - `s2.paper_details.intermediate`  
    Processed paper details.

## Configuration
The pipeline is parametrised via the Kedro configuration system:
- API parameters (`base_url`, `fields`, `api_key`) are set in `parameters.yml`.
- Filtering by publication date is supported for incremental updates.

## Running the Pipeline
Run the pipeline using the following Kedro command:
```bash
kedro run --pipeline s2_pipeline -e base
```

## Nota Bene
- For incremental updates (e.g., Dec 24 update), specific catalog entries (`s2.citation_details.dec24`, `s2.paper_details.dec24`) can be used as outputs.

## Dependencies
- Python libraries: `Kedro`, `pandas`, `requests`
- External API: Semantic Scholar