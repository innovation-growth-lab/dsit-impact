# Impact and Team Science Metrics for UKRI-funded Research Publications

## Summary

This project aims to enhance the impact and team science metrics of UKRI-funded research publications by linking publication data from the Gateway to Research (GtR) database to OpenAlex. The methodology addresses the challenge of matching publications on self-reported, often incomplete data, through a dual approach for reverse-lookups when Digital Object Identifiers (DOI) are absent. The process includes steps to enhance citation data with contextual information and measure the interdisciplinary nature of research teams.

## Approach and Methodology

### DOI Labelling and Dataset Matching

The core of the project involves linking GtR publication data with OpenAlex entries, even in the absence of DOIs. Our approach includes:

1. **CrossRef API Utilisation:** Generating potential DOI matches using publication metadata.
2. **OpenAlex Query Searches:** Systematic API searches on OpenAlex using metadata combinations and cosine similarity measures.

These methods ensure robust additional labelling, improving coverage for subsequent analysis.

### Citation Intent and Section Identification

Using Semantic Scholar’s API, we collect contextual citation information and categorise citations based on intent. We complement this with data from open-access full-text publications tagged by OpenAlex or available through CORE. A classification model will be trained to identify citation intent, enhancing our understanding of the influence of UKRI-funded research.

### Development and Implementation of Interdisciplinary Metrics

We evaluate the interdisciplinary nature of research teams using the methodology from Leydesdorff, Wagner, and Bornmann (2019, 2022b). This includes:

- **Variety of Disciplines:** The diversity of disciplines the authors publish in.
- **Balance:** The publishing behavior for each discipline.
- **Disparity:** How different the disciplines are from each other.

We use the Leiden CWTS topics taxonomy for discipline classification.

## Expected Outcomes

The project delivers:

1. **Categorised Dataset:** A detailed dataset linking GtR publications to OpenAlex, including predicted DOIs and enhanced with context and intent impact metrics.
2. **Scalable and Reusable Code:** Python-written, user-friendly code following Open Source principles and Nesta’s guidelines.
3. **Explanatory Documentation:** An accessible notebook detailing methodologies, code functionalities, and troubleshooting tips.
4. **Continuous Collaboration:** Ongoing work with DSIT to ensure proper code transfer.

## How to install

In order to work with this package, you will need to clone it using git, and install the package as editable:

```
pip install -e .
```

`requirements.txt` should contain all necessary libraries, but **note that** `scipdf` **requires some changes in the source code**, namely in the `parser.py` file to enable URL requests with no ".pdf" suffix. Additionally, `scipdf` requires both a spaCy language library as well as an instance of `grobid` to be running. See scipdf's [repository](https://github.com/titipata/scipdf_parser/tree/master) for instructions on how to do this.

In addition, environment variables are required to use S3 file repositories. See kedro [documentation](https://docs.kedro.org/en/stable/configuration/credentials.html) for how to make a `credentials.yml` file.

### Getting started 

Execute the desired pipeline using the command `kedro run --pipeline <pipeline_name>`. Replace `<pipeline_name>` with the name of the pipeline you want to run (e.g., `data_collection_gtr`).

## Project Structure

The project is organised into several key directories and files, each serving a specific purpose within the Kedro framework:

### Root Directory

- **requirements.txt**: Lists the Python dependencies required to run the project. These can be installed using `pip install -r requirements.txt`.
- **.envrc**: Environment variables configuration file. It is used to set up environment-specific variables needed by the project.
- **README.md**: This README file, providing an overview and details about the project structure and usage.

### Configuration Directory (`conf/`)

The `conf/` directory contains configuration files that define the parameters and settings used throughout the project. The structure is as follows:

- **conf/logging.yml**: Configuration for logging within the project.
- **conf/local/**: Contains local environment-specific configurations.
  - **.gitkeep**: An empty file to keep the directory structure in version control.
  - **credentials.yml**: Stores credentials for accessing external services (not stored in version control).
- **conf/base/**: Contains the base configuration files for different data processing tasks.
  - **parameters_global.yml**: Global parameters shared across different pipelines.
  - **parameters_data_processing_authors.yml**: Configuration for processing author data.
  - **parameters_data_analysis_team_metrics.yml**: Configuration for analysing team metrics.
  - **parameters_data_collection_gtr.yml**: Configuration for collecting data from GtR.
  - **parameters_data_matching_oa.yml**: Configuration for matching datasets with OpenAlex.
  - **parameters_data_collection_s2.yml**: Configuration for collecting data from Semantic Scholar.
  - **catalog.yml**: Catalog file defining the data sources, datasets, and their respective locations.
  - **parameters_data_processing_pdfs.yml**: Configuration for processing PDF documents.

### Source Code Directory (`src/`)

The `src/` directory contains the core codebase of the project, organised into submodules that correspond to different stages of the data processing pipeline:

- **dsit_impact/**: Main package containing the implementation of the data pipelines and utilities.
  - **pipeline_registry.py**: Registers and manages the data pipelines within the project.
  - **settings.py**: Configurations and settings for the Kedro project.
  - **\_\_init\_\_.py** and **\_\_main\_\_.py**: Initialisation files for the package.

  #### Submodules:

  - **datasets/**: Contains modules for handling different types of datasets.
    - **pdf_dataset.py**: Module for processing PDF datasets.
    - **\_\_init\_\_.py**: Initialisation file for the datasets package.

  - **pipelines/**: Contains the different data processing pipelines.
    - **data_analysis_team_metrics/**: Pipeline for analysing interdisciplinary team metrics.
      - **utils.py**, **pipeline.py**, **nodes.py**: Modules containing utilities, pipeline definitions, and data processing nodes.
      - **\_\_pycache\_\_/**: Compiled Python files for performance optimisation.
    
    - **data_generation/**: Pipeline for generating final datasets for analysis.
      - **pipeline.py**, **nodes.py**, **\_\_init\_\_.py**: Modules defining the data generation process.

    - **data_collection_gtr/**: Pipeline for collecting data from the Gateway to Research (GtR) API.
      - **utils.py**, **pipeline.py**, **nodes.py**: Modules defining the GtR data collection process.

    - **data_matching_oa/**: Pipeline for matching datasets with OpenAlex.
      - **utils/**: Utilities for matching OpenAlex datasets.
      - **\_\_pycache\_\_/**: Compiled Python files.

    - **data_processing_authors/**: Pipeline for processing author-related data.
      - **pipeline.py**, **nodes.py**, **\_\_init\_\_.py**: Modules defining the data processing steps for author data.

    - **data_collection_s2/**: Pipeline for collecting data from Semantic Scholar (S2).
      - **utils.py**, **pipeline.py**, **nodes.py**: Modules defining the data collection process from S2.

    - **data_processing_pdfs/**: Pipeline for processing PDF documents.
      - **utils.py**, **pipeline.py**, **nodes.py**: Modules defining the PDF data processing steps.

### Kedro Framework Context

The project leverages the Kedro framework to create modular and reusable data pipelines. Each pipeline is responsible for a specific aspect of the project, such as data collection, processing, or analysis. Kedro's structure helps in organising the project, making it easy to extend and maintain.

#### Key Concepts in Kedro:
- **Nodes**: Individual processing units that perform a specific task, such as data transformation or analysis.
- **Pipelines**: Collections of nodes that are executed in sequence to perform complex data processing tasks.
- **Catalog**: A configuration file that defines data sources, datasets, and how they are loaded and saved.
- **Parameters**: Configuration settings that define how nodes and pipelines should operate.

# How the Project Ties to the Code Pipelines

This section details how the conceptual approach of the **dsit-impact** project is implemented within the codebase using specific pipelines. Each project narrative is closely linked to one or more code pipelines, which collectively process, analyse, and generate the required data and insights.

## DOI Labeling and Dataset Matching

The section revolves around linking Gateway to Research (GtR) publication data with OpenAlex entries, especially in cases where Direct Object Identifier (DOI) matching is not feasible. This involves using advanced techniques to generate potential DOI matches and systematically merge them to improve data coverage.

**Relevant Pipelines**:
- **`data_collection_gtr`**: This pipeline is responsible for collecting publication data from the Gateway to Research (GtR) API. It handles the extraction of metadata that will later be used for matching with OpenAlex datasets.
  - **Nodes**: These include utilities for connecting to the GtR API, parsing responses, and storing the collected data.
- **`data_matching_oa`**: This pipeline deals with matching the GtR data with OpenAlex entries. It involves generating potential matches using metadata and refining these matches using techniques like cosine similarity.
  - **`utils`**: Includes modules like `oa_cr_merge.py`, `oa_match.py`, `cr.py`, and `oa.py` that perform the matching operations and merge results.
  - **Nodes**: Implement the matching logic and decision-making processes to handle ties and finalise DOI assignments.

## Citation Intent and Section Identification

The section project aims to provide insights into how UKRI-funded research influences subsequent studies by analysing citation intent and section context from publications that cite UKRI-linked papers. This involves collecting and processing citation context data from Semantic Scholar and open-access full-text publications.

**Relevant Pipelines**:
- **`data_collection_s2`**: This pipeline is designed to collect citation context data from Semantic Scholar (S2). It gathers information on onward and backward citations, categorising each based on its intent (e.g., background, methodology, results).
  - **Nodes**: Handle the API calls to Semantic Scholar, parsing and storing the citation context data.
- **`data_processing_pdfs`**: This pipeline processes open-access full-text publications to identify where in the text UKRI-linked papers are cited. It also extracts adjacent text to the citation, which can be used to train a model for identifying citation intent.
  - **Nodes**: Implement text extraction and processing techniques to identify citation sections and contexts within PDFs.

## Team Science Metrics

The section evaluates the interdisciplinary nature of research teams involved in UKRI-funded projects by implementing a methodology that studies diversity in disciplines through variety, balance, and disparity metrics.

**Relevant Pipelines**:
- **`data_processing_authors`**: This pipeline processes author-related data, linking authors to their respective disciplines and identifying patterns in their publication history. This data serves as the basis for computing the interdisciplinary metrics.
  - **Nodes**: Include functions for parsing author data, mapping them to disciplines using the Leiden CWTS topics taxonomy, and preparing the data for analysis.
- **`data_analysis_team_metrics`**: This pipeline implements the methodology proposed by Leydesdorff, Wagner, and Bornmann (2019) to calculate interdisciplinary metrics for research teams. It brings together the variety of disciplines, balance of publishing behavior, and disparity between disciplines.
  - **Nodes**: Perform the calculations for the interdisciplinary metrics, analyse the data, and generate insights into the collaborative nature of UKRI-funded projects.

## Data Generation

After all data has been collected, processed, and analysed, the final datasets are generated for further analysis or reporting. This stage consolidates the outputs of the various pipelines.

**Relevant Pipeline**:
- **`data_generation`**: This pipeline is responsible for generating the final datasets by integrating the outputs from the other pipelines. It prepares the data for subsequent analysis or reporting.
  - **Nodes**: Combine the processed data, apply any final transformations or filters, and output the datasets in a ready-to-use format.