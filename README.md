# Netflix-data-engineering-project.
```{=html}
<p align="center">
```
`<img src="assets/project-banner.svg" alt="Netflix Content Data Engineering Project banner" width="100%">
</p>
```
```{=html}
<h1 align="center">
```
Netflix Content Data Engineering Pipeline
```{=html}
</h1>
```
```{=html}
<p align="center">
```
`<b>`{=html}Incremental ingestion • Data cleaning • PySpark
transformations • Delta Lake`</b>`{=html}
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<img alt="Azure" src="https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0078D4?logo=microsoftazure&logoColor=white">`{=html}
`<img alt="Databricks" src="https://img.shields.io/badge/Platform-Azure%20Databricks-E50914?logo=databricks&logoColor=white">`{=html}
`<img alt="PySpark" src="https://img.shields.io/badge/Processing-PySpark-E25A1C?logo=apachespark&logoColor=white">`{=html}
`<img alt="Delta Lake" src="https://img.shields.io/badge/Storage-Delta%20Lake-00A1E0">`{=html}
`<img alt="Status" src="https://img.shields.io/badge/Project-Learning%20%26%20Portfolio-2ea44f">`{=html}
```{=html}
</p>
```
## 📌 Project Overview

This project builds a cloud-based data pipeline for Netflix catalogue
data using **Azure Data Lake Storage Gen2, Azure Databricks, PySpark,
Auto Loader, and Delta Lake**. It demonstrates how raw CSV files can be
ingested incrementally, converted into Delta tables, and cleaned into a
more consistent structure for downstream analysis.

The project works with datasets such as:

-   `netflix_titles.csv` --- title-level catalogue information.
-   `netflix_directors.csv` --- director information associated with
    Netflix titles.

The shared key between these datasets is `show_id`.

## 🏗️ Architecture

```{=html}
<p align="center">
```
`<img src="assets/pipeline-architecture.svg" alt="Netflix data pipeline architecture diagram" width="100%">`{=html}
```{=html}
</p>
```
### Medallion-style layers

  -----------------------------------------------------------------------
  Layer                   Purpose                 Examples of work
  ----------------------- ----------------------- -----------------------
  **Raw**                 Store source files in   CSV files such as
                          ADLS Gen2               `netflix_titles.csv`

  **Bronze**              Ingest source data into Databricks Auto Loader
                          Delta format            (`cloudFiles`), schema
                                                  inference, streaming
                                                  append

  **Silver**              Clean and transform the Data type casting,
                          ingested data           handling missing
                                                  duration values, string
                                                  cleanup, derived
                                                  columns, ranking and
                                                  aggregations

  **Gold**                Analytics-ready curated Optional extension:
                          outputs                 join titles with
                                                  director data and
                                                  derive reporting
                                                  fields/metrics
  -----------------------------------------------------------------------

> **Implementation note:** The Raw → Bronze → Silver flow reflects the
> notebook details documented for this project. Treat Gold as
> implemented only after adding and verifying the corresponding Gold
> notebook/table in your repository.

## 🧰 Technology Stack

-   **Azure Data Lake Storage Gen2 (ADLS Gen2)** --- cloud storage for
    raw and layered data.
-   **Azure Databricks** --- managed workspace for notebooks and Spark
    processing.
-   **Apache Spark / PySpark** --- distributed data transformation.
-   **Databricks Auto Loader** --- incremental file ingestion using the
    `cloudFiles` source.
-   **Delta Lake** --- table storage format for reliable reads and
    writes.
-   **Python** --- pipeline logic and reusable transformation code.
-   **Git and GitHub** --- version control and project documentation.

## 🔄 Pipeline Walkthrough

### 1. Raw → Bronze: incremental ingestion

The Bronze notebook uses Databricks Auto Loader to read CSV files from
the ADLS Gen2 `raw` container and write them as Delta data under the
`bronze` layer.

Key concepts demonstrated:

-   `cloudFiles` source for file discovery and incremental ingestion.
-   CSV format and schema inference.
-   Delta output in append mode.
-   A short processing trigger interval (10 seconds in the documented
    notebook configuration).
-   Checkpoint and schema tracking to support streaming progress and
    schema management.

**Storage account used in the project:** `netflixprojectdlansh`

Example paths (adjust if your final folder layout differs):

``` text
abfss://raw@netflixprojectdlansh.dfs.core.windows.net/
abfss://bronze@netflixprojectdlansh.dfs.core.windows.net/netflix_titles/
abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_titles/
```

Do not commit access keys, SAS tokens, client secrets, or other
credentials to GitHub.

### 2. Bronze → Silver: cleaning and transformation

The Silver notebook transforms the Bronze data using PySpark. The
documented transformation logic includes:

-   Filling missing `duration_minutes` values with `0`.
-   Filling missing `duration_seasons` values with `1`.
-   Casting duration-related fields to integer types.
-   Deriving `Shorttitle` by splitting a title string at `:`.
-   Cleaning the `rating` field using string splitting.
-   Creating a `type_flag` value (`Movie = 1`, `TV Show = 2`, other
    values = `0`).
-   Applying dense ranking to duration values.
-   Creating temporary/global views for SQL-style analysis.
-   Aggregating counts by content type.
-   Writing the transformed data to the Silver Delta layer.

These rules are project-specific choices for practice and should be
validated against the source data before being used in a production
pipeline.

### 3. Lookup datasets

The reusable lookup notebook is parameterized for datasets including:

-   `netflix_directors`
-   `netflix_cast`
-   `netflix_countries`
-   `netflix_category`

The notebook uses widgets/task parameters to select a dataset and writes
the lookup output in Delta format. Confirm that each source dataset is
present in your Raw container before running the corresponding task.

### 4. Silver → Gold: analytics-ready extension

A useful next layer is to join title data with director data using
`show_id` and create curated reporting fields such as `content_category`
and `release_decade`. Add this section as a completed implementation
only if the matching Gold notebook and output table are included in the
repository.

## 🗂️ Suggested GitHub Repository Structure

``` text
netflix-data-engineering/
├── README.md
├── assets/
│   ├── project-banner.svg
│   └── pipeline-architecture.svg
├── notebooks/
│   ├── 01_bronze_autoloader.py
│   ├── 02_silver_transformations.py
│   ├── 03_lookup_datasets.py
│   └── 04_gold_transformations.py   # include only if implemented
├── docs/
│   └── data-dictionary.md           # optional
└── screenshots/
    └── README.md                    # add genuine workspace screenshots here
```

Rename the notebook files in this suggested layout to match the actual
filenames you export from Databricks. Remove any placeholder files that
you do not create.

## 📊 Data Quality and Validation Ideas

Before treating the output as analysis-ready, validate:

-   `show_id` is present for records that require a title-level key.
-   Duplicate records are identified and handled consistently.
-   `release_year` and duration fields have appropriate data types.
-   Missing values are handled according to documented business rules.
-   Joins use the expected key (`show_id`) and their row counts are
    checked.
-   Bronze and Silver counts are compared to help identify unexpected
    record loss.
-   Streaming checkpoints are stored in the intended location and are
    not committed to source control.

## ▶️ How to Run

1.  Upload the source CSV files to the configured ADLS Gen2 `raw`
    container.
2.  Open the Azure Databricks workspace and import the exported
    notebooks.
3.  Confirm the storage account and container paths in the notebook
    configuration.
4.  Configure the required Azure permissions/credentials securely in the
    workspace.
5.  Run the Bronze Auto Loader notebook and confirm that Delta output is
    created.
6.  Run the Silver transformation notebook and inspect the resulting
    schema and sample rows.
7.  Run lookup and Gold notebooks only when their source files and
    implementations are available.
8.  Validate counts, nulls, data types, and join results.

Exact setup steps can vary depending on whether your workspace uses
Unity Catalog external locations, service principals, managed
identities, or another approved access method.

## 🖼️ Screenshots

For a stronger project presentation, add **real screenshots from your
own Databricks workspace** to a `screenshots/` folder, for example:

1.  Bronze Auto Loader notebook and successful run.
2.  Bronze Delta table or folder in ADLS Gen2.
3.  Silver transformation code and output schema.
4.  A sample of cleaned Silver data.
5.  Workflow/task dependencies, if configured.
6.  Gold output and a dashboard, if implemented.

Use relative image links in this README, for example:

``` markdown
![Silver transformation output](screenshots/silver-output.png)
```

The included banner and architecture diagram are original project
visuals; they are not screenshots of a live Azure environment.

## 🎯 Key Learnings

-   Understanding the Raw/Bronze/Silver/Gold data-layer pattern.
-   Incremental file ingestion with Databricks Auto Loader.
-   Working with ADLS Gen2 paths and cloud storage organization.
-   Applying PySpark transformations, type casting, string operations,
    ranking, and aggregations.
-   Writing Delta tables and managing streaming checkpoints/schema
    information.
-   Parameterizing reusable notebook logic for lookup datasets.
-   Documenting a data engineering workflow for version control and
    review.

## 🚀 Future Improvements

-   Complete and validate the Gold transformation notebook.
-   Add automated data-quality checks and pipeline logging.
-   Add workflow orchestration and failure notifications.
-   Add a data dictionary and sample before/after records.
-   Build a Power BI dashboard from validated Gold outputs.
-   Add genuine Databricks and Azure screenshots to the repository.

## 👨‍💻 Author

**Divyansh Sharma**\
Aspiring Data Engineer \| SQL \| Python \| PySpark \| Azure

-   GitHub:
    [brahamanadarsh77-ship-it](https://github.com/brahamanadarsh77-ship-it)

------------------------------------------------------------------------

```{=html}
<p align="center">
```
`<i>`{=html}Built as a hands-on portfolio project to practice cloud data
ingestion and transformation with Azure Databricks.`</i>`{=html}
```{=html}
</p>
```
