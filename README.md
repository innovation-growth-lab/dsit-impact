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

We evaluate the interdisciplinary nature of research teams using the methodology from Leydesdorff, Wagner, and Bornmann (2019). This includes:

- **Variety of Disciplines:** The diversity of disciplines the authors publish in.
- **Balance:** The publishing behavior for each discipline.
- **Disparity:** How different the disciplines are from each other.

We use the Leiden CWTS topics taxonomy for discipline classification.

## Expected Outcomes

The project will deliver:

1. **Categorised Dataset:** A detailed dataset linking GtR publications to OpenAlex, including predicted DOIs and enhanced with context and intent impact metrics.
2. **Scalable and Reusable Code:** Python-written, user-friendly code following Open Source principles and Nesta’s guidelines.
3. **Explanatory Documentation:** An accessible notebook detailing methodologies, code functionalities, and troubleshooting tips.
4. **Continuous Collaboration:** Ongoing work with DSIT to ensure proper code transfer.

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `context`, 'session', `catalog`, and `pipelines`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)


### Note:
- Requires spacy's basic English dictionary `python -m spacy download en_core_web_sm`
- I had to change the `serve_grobid.sh` file to take the smaller docker image.