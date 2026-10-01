# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Incremental Data Loading using Auto Loader (Raw -> Bronze)

# COMMAND ----------
# Storage Account Configuration
STORAGE_ACCOUNT = "netflixprojectdlansh"

# Storage paths
RAW_SOURCE_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_TARGET_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/netflix_titles"

# Dedicated locations for schema evolution and state tracking
CHECKPOINT_LOCATION = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoint/netflix_titles"
SCHEMA_LOCATION = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoint/netflix_titles_schema"

# COMMAND ----------
# 1. Incremental Read Stream with Auto Loader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(RAW_SOURCE_PATH)
)

# COMMAND ----------
# 2. Write Stream to Bronze Container in Delta Format
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="10 seconds")
    .start(BRONZE_TARGET_PATH)
)

# Wait for micro-batch execution (or remove timeout to run indefinitely)
query.awaitTermination(timeout=60)