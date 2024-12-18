# Data Collection GtR Pipeline

The **Data Collection GtR Pipeline** fetches and preprocesses data from the Gateway to Research (GtR) API. The pipeline retrieves data for publications, projects, organisations, and funds.

<img src="https://eosc.eu/wp-content/uploads/2024/11/UKRI-logo.png" alt="GtR" style="width:100%;"/>

## Features
- Fetch GtR data from multiple endpoints.
- Preprocess data to handle nested structures and extract relevant fields.
- Concatenate data for a unified format.

## Nodes Overview
1. **`fetch_gtr_data`**  
   Fetches data from a specified GtR API endpoint, handles pagination, and preprocesses it using the appropriate method.  

2. **`concatenate_endpoint`**  
   Combines multiple DataFrames from the same endpoint into a single DataFrame.

## Key Datasets
- **Raw Inputs:**
  - Data fetched from GtR API endpoints (e.g., `gtr.data_collection.publications.raw`).
- **Outputs:**
  - Preprocessed datasets such as `gtr.data_collection.publications.intermediate`.

## Configuration
The pipeline is parametrized via Kedro's configuration system:
- API parameters (`base_url`, `headers`, `page_size`) are set in `parameters.yml`.
- Endpoint-specific configurations allow fetching and preprocessing data for specific resource types (e.g., publications, projects).

## Running the Pipeline
To execute the pipeline:
```bash
kedro run --pipeline data_collection_gtr
```
For a single endpoint, use tags:
```bash
kedro run --pipeline data_collection_gtr --tags publications
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `requests`, `logging`.
- Gateway to Research API.