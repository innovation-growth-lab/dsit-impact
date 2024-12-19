# Citation Sections Pipeline

The **Citation Sections Pipeline** identifies and processes citation contexts from PDFs where UKRI-supported publications have been cited.

<img src="https://cdn-thumbnails.huggingface.co/social-thumbnails/spaces/kermitt2/grobid.png" alt="GtR" style="width:100%;"/>

## Features
- Preprocess datasets from OpenAlex and Semantic Scholar for citation context extraction.
- Retrieve and parse PDF content to extract citation sections.
- Check parsed PDFs using an oracle to avoid redundant processing.
- Compute citation shares across key document sections.

## Nodes Overview
1. **`preprocess_for_section_collection`**  
   Merges datasets from OpenAlex and Semantic Scholar, preparing the data for citation section extraction.  

2. **`get_unparsed_pdfs`**  
   Identifies which PDFs have not yet been parsed by comparing the incoming data against previously processed results using an oracle.  

3. **`get_citation_sections`**  
   Retrieves citation sections from unparsed PDFs using the preprocessed data and a list of typical section headings.  

4. **`compute_section_shares`**  
   Computes the distribution of citations across document sections for UKRI-supported publications.

## Key Datasets
- **Raw Inputs:**
  - `oa.publications.gtr.primary`: OpenAlex publications dataset.
  - `s2.citation_details.intermediate`: Semantic Scholar citation details.
- **Intermediate Outputs:**
  - `pdfs.section_details.preprocessed`: Preprocessed data ready for section collection.
  - `pdfs.section_details.unparsed`: PDFs identified as unparsed and ready for processing.
  - `pdfs.section.details.raw`: Extracted citation sections from PDFs.
- **Final Outputs:**
  - `pdfs.section_shares.intermediate`: Citation shares across sections.

## Configuration
The pipeline uses Kedro's configuration system to manage:
- Section headings for parsing PDFs (`main_sections` parameter).
- API credentials and other necessary parameters.

## Running the Pipeline
To execute the pipeline:
```bash
kedro run --pipeline data_processing_pdfs
```

To preprocess only for section collection:
```bash
kedro run --nodes preprocess_for_section_collection
```

To identify unparsed PDFs:
```bash
kedro run --nodes get_unparsed_pdfs
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `scipdf`, `joblib`, `logging`.
- Data sources: OpenAlex, Semantic Scholar.