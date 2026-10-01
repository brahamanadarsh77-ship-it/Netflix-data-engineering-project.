# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # DLT Notebook - GOLD LAYER
# MAGIC Declarative Delta Live Tables (DLT) streaming pipeline with data quality expectations.

# COMMAND ----------
import dlt
from pyspark.sql.functions import *

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Data Quality Expectation Rule Sets

# COMMAND ----------
# Expectation dictionary for lookup dimension tables
looktables_rules = {
    "rule1": "show_id is NOT NULL"
}

# Expectation dictionary for master titles table
masterdata_rules = {
    "rule1": "newflag is NOT NULL",
    "rule2": "show_id is NOT NULL"
}

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Gold Streaming Tables: Lookup Dimensions

# COMMAND ----------
# Curated Directors Dimension Table
@dlt.table(
    name="gold_netflixdirectors",
    comment="Curated Netflix Directors streaming table"
)
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixdirectors():
    df = (
        spark.readStream
        .format("delta")
        .load("abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_directors")
    )
    return df

# COMMAND ----------
# Curated Cast Dimension Table
@dlt.table(
    name="gold_netflixcast",
    comment="Curated Netflix Cast streaming table"
)
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixcast():
    df = (
        spark.readStream
        .format("delta")
        .load("abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_cast")
    )
    return df

# COMMAND ----------
# Curated Countries Dimension Table
@dlt.table(
    name="gold_netflixcountries",
    comment="Curated Netflix Countries streaming table"
)
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixcountries():
    df = (
        spark.readStream
        .format("delta")
        .load("abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_countries")
    )
    return df

# COMMAND ----------
# Curated Category Dimension Table
@dlt.table(
    name="gold_netflixcategory",
    comment="Curated Netflix Category streaming table"
)
@dlt.expect_or_drop("rule1", "show_id is NOT NULL")
def gold_netflixcategory():
    df = (
        spark.readStream
        .format("delta")
        .load("abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_category")
    )
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Staging Streaming Table: Master Titles

# COMMAND ----------
# Reads Silver Delta master data into a staging DLT streaming table
@dlt.table(
    name="gold_stg_netflixtitles",
    comment="Staging layer for Netflix master titles"
)
def gold_stg_netflixtitles():
    df = (
        spark.readStream
        .format("delta")
        .load("abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_titles")
    )
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Transformation Streaming View

# COMMAND ----------
# Intermediate streaming view reading from staging using the LIVE keyword
@dlt.view(
    name="gold_trns_netflixtitles",
    comment="Transformation streaming view adding newflag"
)
def gold_trns_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("newflag", lit(1))
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Final Gold Master Table with Expectations

# COMMAND ----------
# Final materialized gold table enforcing master data quality constraints
@dlt.table(
    name="gold_netflixtitles",
    comment="Production Gold Netflix Titles Fact/Dimension Table"
)
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df