# Data Collection OA Pipeline

The **Data Collection OA Pipeline** is used to process publication data by matching Gateway to Research (GtR) records with metadata from open-source OpenAlex and CrossRef. It facilitates reverse lookups, data enrichment, and merging processes.

<img src="https://openalex.org/img/logo-full-small.a21c0984.png" alt="GtR" style="width:100%;"/>

## Features
- Match GtR publications with OpenAlex and CrossRef records.
- Collect metadata for matched records using DOIs and OpenAlex IDs.
- Deduplicate and merge datasets to create enriched outputs.

## Nodes Overview
1. **`preprocess_publication_doi`**  
   Prepares DOIs for compatibility with OpenAlex and CrossRef APIs.  

2. **`create_list_doi_inputs`**  
   Generates a list of DOI inputs from GtR data for use in API requests.  

3. **`fetch_papers`**  
   Retrieves metadata from OpenAlex or CrossRef for a list of DOIs or IDs.  

4. **`concatenate_openalex`**  
   Merges partitioned OpenAlex data into a single dataset.  

5. **`crossref_doi_match`**  
   Matches DOIs with CrossRef records for metadata enrichment.  

6. **`oa_search_match`**  
   Matches GtR records with OpenAlex data using search queries.  

7. **`oa_filter`**  
   Filters OpenAlex matches to retain the best candidate based on metadata quality.  

8. **`select_better_match`**  
   Selects the optimal match between CrossRef and OpenAlex for each GtR record.  

9. **`concatenate_oa_datasets`**  
   Combines data from all sources into a single unified dataset.  

10. **`map_outcome_id`**  
    Maps GtR outcome IDs to the corresponding OpenAlex records.  

## Key Datasets
- **Raw Inputs:**
  - `gtr.data_collection.publications.intermediate`: GtR publications.
- **Outputs:**
  - `oa.publications.gtr.primary`: Final enriched publication data.

## Configuration
The pipeline is parametrised through Kedro's configuration system:
- API details (`mailto`, `perpage`, `filter_criteria`) are configured in `parameters.yml`.
- Intermediate and output datasets are defined in the catalog.

## Running the pipeline
To execute the pipeline:
```bash
kedro run --pipeline data_collection_oa
```

Specific sub-pipelines can be executed using tags:
```bash
kedro run --pipeline data_collection_oa --tags=first_search
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `requests`, `thefuzz`.
- APIs: OpenAlex, CrossRef.