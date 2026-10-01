# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------
# 1. Import necessary PySpark functions and types
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------
# 2. Read master data from Bronze in Delta format
df = (
    spark.read.format("delta")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://bronze@netflixprojectdlansh.dfs.core.windows.net/netflix_titles")
)

# COMMAND ----------
# 3. Handle null values using dictionary mapping
df = df.fillna({
    "duration_minutes": 0, 
    "duration_seasons": 1
})

# COMMAND ----------
# 4. Cast columns to IntegerType
df = (
    df
    .withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))
    .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))
)

df.printSchema()
display(df)

# COMMAND ----------
# 5. Extract short title (split by colon ':')
df = df.withColumn("Shorttitle", split(col("title"), ":")[0])
display(df)

# COMMAND ----------
# 6. Clean rating column (split by hyphen '-')
df = df.withColumn("rating", split(col("rating"), "-")[0])
display(df)

# COMMAND ----------
# 7. Add conditional flag for content type (when / otherwise)
df = df.withColumn(
    "type_flag",
    when(col("type") == "Movie", 1)
    .when(col("type") == "TV Show", 2)
    .otherwise(0)
)
display(df)

# COMMAND ----------
# 8. Apply Window function: Dense Rank over duration_minutes
window_spec = Window.orderBy(col("duration_minutes").desc())
df = df.withColumn("duration_ranking", dense_rank().over(window_spec))
display(df)

# COMMAND ----------
# 9. Create Temp View (notebook-scoped) and Global Temp View (cluster session-scoped)
df.createOrReplaceTempView("temp_view")
df.createOrReplaceGlobalTempView("global_view")

# Query from Global Temp View using Spark SQL
df_global = spark.sql("""
    SELECT * FROM global_temp.global_view
""")
display(df_global)

# COMMAND ----------
# 10. Aggregation for visualization (Type distribution)
df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
display(df_vis)

# COMMAND ----------
# 11. Write final cleaned master table to Silver in Delta format
(
    df.write.format("delta")
    .mode("overwrite")
    .option("path", "abfss://silver@netflixprojectdlansh.dfs.core.windows.net/netflix_titles")
    .save()
)