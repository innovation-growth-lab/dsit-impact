# Data Results and Team Metrics Pipeline

The **Data Results and Team Metrics Pipeline** computes topic embeddings, author aggregates, and diversity metrics. It provides insights into the diversity of topics and team collaboration in UKRI-supported research outputs.

## Features
- Generate topic embeddings and compute distance matrices across taxonomy levels (topics, subfields, fields, and domains).
- Aggregate author data to analyse publication frequencies and topic distributions.
- Calculate diversity metrics for papers and co-authors, including variety, evenness, and disparity.


## Key Processes Explained:

This pipeline takes input datasets, including UKRI-supported publications, CWTS taxonomy data (organised into topics, subfields, fields, and domains), and OpenAlex author data. It then processes these to generate insights about diversity and collaboration in research.

1. **Topic Embeddings and Distance Matrices:**  
   Topics and their hierarchical taxonomy are encoded into numerical embeddings using a sentence transformer model. These embeddings are used to calculate pairwise distances at different levels of granularity (topics, subfields, fields, domains). These distance matrices allow the pipeline to assess how "distant" or diverse topics are in terms of content.

2. **Author Aggregates:**  
   Author data is grouped by year and taxonomy level. The pipeline calculates how frequently authors publish in various topics or fields. This aggregation helps identify an author’s thematic focus over time.

3. **Weighted Cumulative Aggregates:**  
   For each author, the pipeline computes a weighted cumulative sum of topic frequencies. This approach accounts for the temporal aspect, giving more weight to recent publications. It’s particularly useful for understanding an author’s changing research interests and collaborations at time of UKRI-funded publication.

4. **Diversity Metrics:**  
   Diversity metrics are calculated for both individual papers and teams of co-authors:
   - **Variety:** Measures the number of unique topics represented.
   - **Evenness:** Evaluates how balanced the distribution of topics is.
   - **Disparity:** Assesses the thematic distance between topics, leveraging the distance matrices.  

   These metrics provide insights into the breadth of research (variety), balance of topic distribution (evenness), and conceptual distinctiveness (disparity) in publications and collaborations.

## Nodes Overview
1. **`compute_topic_embeddings`**  
   Encodes topics and computes distance matrices for taxonomy levels (subfields, fields, domains).  

2. **`create_author_aggregates`**  
   Aggregates author data by taxonomy levels and calculates topic frequencies.  

3. **`cumulative_author_aggregates`**  
   Computes weighted cumulative sums of topic frequencies over time for each author.  

4. **`calculate_paper_diversity`**  
   Computes paper-level diversity metrics, including variety, evenness, and disparity.  

5. **`calculate_coauthor_diversity`**  
   Computes co-author diversity metrics by combining cumulative author aggregates and disparity matrices.

## Key Datasets
- **Raw Inputs:**
  - `cwts.topics.input`: Taxonomy data for topics, subfields, fields, and domains.
  - `authors.oa_dataset.raw`: Author-level OpenAlex data.
  - `oa.publications.gtr.primary`: UKRI-supported publication data.
- **Outputs:**
  - Topic embeddings and distance matrices:
    - `cwts.topics.subfield.distance_matrix`
    - `cwts.topics.field.distance_matrix`
    - `cwts.topics.domain.distance_matrix`
  - Author aggregates:
    - `authors.{level}.aggregates.intermediate`
    - `authors.{level}.cumulative_aggregates.intermediate`
  - Diversity scores:
    - `publications.{level}.paper_diversity_scores.intermediate`
    - `publications.{level}.coauthor_diversity_scores.intermediate`

## Configuration
The pipeline leverages Kedro’s configuration system for parameters:
- Taxonomy levels (`params:tm.levels`): Configured for subfields, fields, and domains.
- Disparity matrices: Used for diversity calculations.

## Running the Pipeline
To execute the entire pipeline:
```bash
kedro run --pipeline data_results_team_metrics
```
For specific nodes:
```bash
kedro run --nodes compute_topic_embeddings,create_author_aggregates
```

## Dependencies
- Python libraries: `Kedro`, `pandas`, `numpy`, `scipy`, `sentence-transformers`, `logging`.
- Data sources: CWTS taxonomy, OpenAlex.