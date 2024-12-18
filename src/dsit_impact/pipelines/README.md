# Pipelines Overview

This document provides an overview of the data processing and analysis pipelines used in the project. Each pipeline addresses a specific aspect of data collection, enrichment, or analysis.

---

## Data Collection Pipelines

### 1. Data Collection GtR Pipeline
Fetches and preprocesses data from the Gateway to Research (GtR) API. 

- **Purpose:** Retrieve GtR data on publications, projects, organisations, and funds, transforming it into a structured format.
- **Key Outputs:**
  - `gtr.data_collection.publications.intermediate`

### 2. Data Collection OA Pipeline
Retrieves, matches, and processes metadata from OpenAlex and CrossRef for Gateway to Research outputs.

- **Purpose:** Enrich GtR data with additional metadata using DOIs and other identifiers.
- **Key Outputs:**
  - `oa.publications.gtr.primary`

### 3. Semantic Scholar Pipeline
Fetches citation and paper details using Semantic Scholar API and OpenAlex datasets.

- **Purpose:** Retrieve citation contexts, influential metrics, and metadata for UKRI-supported publications.
- **Key Outputs:**
  - `s2.citation_details.intermediate`
  - `s2.paper_details.intermediate`

### 4. Author Collection Pipeline
Fetches publications authored by researchers associated with GtR projects from OpenAlex.

- **Purpose:** Analyse the broader publication history of researchers funded by UKRI.
- **Key Outputs:**
  - `authors.oa_dataset.raw`

### 5. Citation Sections Pipeline
Extracts citation contexts from PDFs where UKRI-supported publications are cited.

- **Purpose:** Assess where and how UKRI-supported outputs are cited.
- **Key Outputs:**
  - `pdfs.section_shares.intermediate`

---

## Analysis Pipelines

### 6. Data Results and Team Metrics Pipeline
Analyses topics, author aggregates, and diversity metrics for UKRI-supported outputs.

- **Purpose:** Compute diversity and collaboration metrics based on taxonomy-level topic embeddings.
- **Key Outputs:**
  - `cwts.topics.subfield.distance_matrix`
  - `publications.subfield.paper_diversity_scores.intermediate`

---

## Usage

All pipelines can be executed using Kedro commands. Example:
```bash
kedro run --pipeline <pipeline_name>
```
Use tags or nodes for more granular control of the execution process.

---

<img src="https://www.innovationgrowthlab.org/sites/default/files/igl_logo.png" alt="GtR" style="width:100%;"/>
