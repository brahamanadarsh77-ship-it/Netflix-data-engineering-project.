# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Notebook: Lookup Tables
# MAGIC Parameterized ingestion of dimension/lookup datasets from Bronze to Silver in Delta Lake format.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Parameters (Widgets)

# COMMAND ----------
# Setup Databricks runtime parameter widgets with defaults
dbutils.widgets.text("sourcefolder", "netflix_directors")
dbutils.widgets.text("targetfolder", "netflix_directors")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Variables & Path Configuration

# COMMAND ----------
# Retrieve dynamic parameters passed from Databricks Workflows / ADF
var_src_folder = dbutils.widgets.get("sourcefolder")
var_trg_folder = dbutils.widgets.get("targetfolder")

STORAGE_ACCOUNT = "netflixprojectdlansh"

# Storage paths
BRONZE_SOURCE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/{var_src_folder}"
SILVER_TARGET_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/{var_trg_folder}"

print(f"Reading from: {BRONZE_SOURCE_PATH}")
print(f"Writing to:   {SILVER_TARGET_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Read Lookup Data from Bronze Container

# COMMAND ----------
# Read Bronze CSV file
df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(BRONZE_SOURCE_PATH)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Preview Ingested Data

# COMMAND ----------
display(df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Write to Silver Container in Delta Format

# COMMAND ----------
# Write to Silver in Delta format (use overwrite for idempotent full updates, or append for incremental updates)
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("path", SILVER_TARGET_PATH)
    .save()
)

print(f"Successfully loaded {var_src_folder} into Silver Delta Lake at: {SILVER_TARGET_PATH}")