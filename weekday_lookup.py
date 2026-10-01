# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Weekday Lookup Task
# MAGIC Captures the runtime weekday parameter and sets task values for conditional branching in Databricks Workflows.

# COMMAND ----------
# 1. Define widget parameter with default value ("7" for Sunday)
dbutils.widgets.text("weekday", "7")

# COMMAND ----------
# 2. Retrieve widget value and cast to integer
var = int(dbutils.widgets.get("weekday"))

# COMMAND ----------
# 3. Publish weekday integer to Databricks Workflows task values
dbutils.jobs.taskValues.set(key="weekoutput", value=var)
print(f"Registered weekday task value: {var}")