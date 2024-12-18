# Author Collection Pipeline

The **Author Collection Pipeline** retrieves and processes publications from OpenAlex for authors associated with Gateway to Research (GtR) projects. This pipeline enables diversity estimates using the broader publication history of researchers funded by UKRI.

## Features
- Extract a list of unique authors from GtR publications.
- Fetch publications for these authors from OpenAlex using efficient batching.
- Enrich the data with author-specific and topic-related metadata.

## Nodes Overview
1. **`create_author_list`**  
   Extracts a unique list of authors from the GtR dataset and prepares it for OpenAlex API queries.  

2. **`fetch_author_papers`**  
   Retrieves publications for each author using the OpenAlex API, filters the results, and processes them into a structured format.

## Key Datasets
- **Raw Inputs:**
  - `oa.publications.gtr.primary`: GtR publications enriched with OpenAlex metadata.
- **Outputs:**
  - `authors.oa_dataset.raw`: Publications by authors, including metadata on topics, authorship, and publication dates.

## Configuration
The pipeline leverages Kedro's configuration system:
- API parameters (`mailto`, `perpage`, `filter_criteria`) are defined in `parameters.yml`.
- The batching process ensures efficient querying of the OpenAlex API.

## Running the Pipeline
To execute the pipeline:
```bash
kedro run --pipeline author_collection_pipeline
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `joblib`, `logging`.
- OpenAlex API.